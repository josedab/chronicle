package chronicle

import (
	"testing"
)

func TestGraphStoreAddGetNode(t *testing.T) {
	gs := NewGraphStore()

	err := gs.AddNode(GraphNode{ID: "svc-a", Type: GraphNodeService, Labels: map[string]string{"app": "frontend"}})
	if err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	node, ok := gs.GetNode("svc-a")
	if !ok {
		t.Fatal("expected node to exist")
	}
	if node.Type != GraphNodeService {
		t.Errorf("expected service type, got %s", node.Type)
	}
	if gs.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", gs.NodeCount())
	}
}

func TestGraphStoreAddEdge(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "svc-a", Type: GraphNodeService})
	gs.AddNode(GraphNode{ID: "svc-b", Type: GraphNodeService})

	err := gs.AddEdge(GraphEdge{Source: "svc-a", Target: "svc-b", Type: GraphEdgeCallsTo})
	if err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	if gs.EdgeCount() != 1 {
		t.Errorf("expected 1 edge, got %d", gs.EdgeCount())
	}
}

func TestGraphStoreEdgeRequiresNodes(t *testing.T) {
	gs := NewGraphStore()

	err := gs.AddEdge(GraphEdge{Source: "missing", Target: "also-missing", Type: GraphEdgeCallsTo})
	if err == nil {
		t.Fatal("expected error for missing nodes")
	}
}

func TestGraphStoreNeighbors(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "a"})
	gs.AddNode(GraphNode{ID: "b"})
	gs.AddNode(GraphNode{ID: "c"})
	gs.AddEdge(GraphEdge{Source: "a", Target: "b", Type: GraphEdgeCallsTo})
	gs.AddEdge(GraphEdge{Source: "a", Target: "c", Type: GraphEdgeDependsOn})

	neighbors := gs.Neighbors("a", "out")
	if len(neighbors) != 2 {
		t.Errorf("expected 2 out-neighbors, got %d", len(neighbors))
	}

	inNeighbors := gs.Neighbors("b", "in")
	if len(inNeighbors) != 1 {
		t.Errorf("expected 1 in-neighbor, got %d", len(inNeighbors))
	}
}

func TestGraphStoreTraversal(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "a"})
	gs.AddNode(GraphNode{ID: "b"})
	gs.AddNode(GraphNode{ID: "c"})
	gs.AddNode(GraphNode{ID: "d"})
	gs.AddEdge(GraphEdge{Source: "a", Target: "b", Type: GraphEdgeCallsTo})
	gs.AddEdge(GraphEdge{Source: "b", Target: "c", Type: GraphEdgeCallsTo})
	gs.AddEdge(GraphEdge{Source: "c", Target: "d", Type: GraphEdgeCallsTo})

	result := gs.Traverse("a", 3, nil)
	if len(result.Nodes) != 4 {
		t.Errorf("expected 4 nodes in traversal, got %d", len(result.Nodes))
	}

	// Depth-limited
	result2 := gs.Traverse("a", 1, nil)
	if len(result2.Nodes) != 2 {
		t.Errorf("expected 2 nodes at depth 1, got %d", len(result2.Nodes))
	}
}

func TestGraphStoreTraversalWithEdgeFilter(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "a"})
	gs.AddNode(GraphNode{ID: "b"})
	gs.AddNode(GraphNode{ID: "c"})
	gs.AddEdge(GraphEdge{Source: "a", Target: "b", Type: GraphEdgeCallsTo})
	gs.AddEdge(GraphEdge{Source: "a", Target: "c", Type: GraphEdgeDependsOn})

	result := gs.Traverse("a", 2, []GraphEdgeType{GraphEdgeCallsTo})
	if len(result.Nodes) != 2 { // a + b (not c)
		t.Errorf("expected 2 nodes with edge filter, got %d", len(result.Nodes))
	}
}

func TestGraphStoreRemoveNode(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "a"})
	gs.AddNode(GraphNode{ID: "b"})
	gs.AddEdge(GraphEdge{Source: "a", Target: "b", Type: GraphEdgeCallsTo})

	gs.RemoveNode("a")
	if gs.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", gs.NodeCount())
	}
	if gs.EdgeCount() != 0 {
		t.Errorf("expected 0 edges after node removal, got %d", gs.EdgeCount())
	}
}

func TestGraphStoreFindNodes(t *testing.T) {
	gs := NewGraphStore()

	gs.AddNode(GraphNode{ID: "s1", Type: GraphNodeService, Labels: map[string]string{"env": "prod"}})
	gs.AddNode(GraphNode{ID: "s2", Type: GraphNodeService, Labels: map[string]string{"env": "staging"}})
	gs.AddNode(GraphNode{ID: "h1", Type: GraphNodeHost, Labels: map[string]string{"env": "prod"}})

	prodServices := gs.FindNodes(GraphNodeService, map[string]string{"env": "prod"})
	if len(prodServices) != 1 {
		t.Errorf("expected 1 prod service, got %d", len(prodServices))
	}
}

func TestDocumentIndex(t *testing.T) {
	di := NewDocumentIndex()

	di.Index("doc1", map[string]any{
		"title": "CPU spike analysis",
		"type":  "incident",
	})
	di.Index("doc2", map[string]any{
		"title": "Memory leak report",
		"type":  "incident",
	})

	results := di.Search("cpu")
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'cpu', got %d", len(results))
	}

	results = di.Search("incident")
	// "incident" appears in type:incident for both docs
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'incident', got %d", len(results))
	}

	di.Remove("doc1")
	results = di.Search("cpu")
	if len(results) != 0 {
		t.Errorf("expected 0 results after removal, got %d", len(results))
	}
}

func TestMultiModelGraphStoreDocument(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mm := NewMultiModelGraphStore(db)

	err := mm.StoreDocument(&Document{
		ID:         "inc-1",
		Collection: "incidents",
		Data: map[string]any{
			"title":    "High CPU on web-1",
			"severity": "high",
		},
	})
	if err != nil {
		t.Fatalf("store document failed: %v", err)
	}

	doc, ok := mm.GetDocument("inc-1")
	if !ok {
		t.Fatal("expected document to exist")
	}
	if doc.Collection != "incidents" {
		t.Errorf("expected incidents collection, got %s", doc.Collection)
	}

	docs := mm.SearchDocuments("cpu")
	if len(docs) != 1 {
		t.Errorf("expected 1 search result, got %d", len(docs))
	}
}

func TestMultiModelGraphStoreCrossQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mm := NewMultiModelGraphStore(db)

	mm.Graph().AddNode(GraphNode{ID: "web-1", Type: GraphNodeService})
	mm.Graph().AddNode(GraphNode{ID: "db-1", Type: GraphNodeService})
	mm.Graph().AddEdge(GraphEdge{Source: "web-1", Target: "db-1", Type: GraphEdgeCallsTo})

	mm.StoreDocument(&Document{
		ID: "event-1", Collection: "events",
		Data: map[string]any{"title": "web-1 latency spike"},
	})

	result, err := mm.CrossModelQueryExecute(CrossModelQuery{
		GraphStartNode: "web-1",
		GraphMaxDepth:  2,
		DocSearch:      "web-1",
	})
	if err != nil {
		t.Fatalf("cross-model query failed: %v", err)
	}

	if result.GraphResult == nil || len(result.GraphResult.Nodes) == 0 {
		t.Error("expected graph results")
	}
	if len(result.Documents) != 1 {
		t.Errorf("expected 1 document, got %d", len(result.Documents))
	}
}

func TestExportTopology(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mm := NewMultiModelGraphStore(db)
	mm.Graph().AddNode(GraphNode{ID: "a"})
	mm.Graph().AddNode(GraphNode{ID: "b"})
	mm.Graph().AddEdge(GraphEdge{Source: "a", Target: "b", Type: GraphEdgeCallsTo})

	data, err := mm.ExportTopology()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if len(data) < 10 {
		t.Fatal("expected non-trivial topology export")
	}
}
