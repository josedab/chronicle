package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// --- Graph Model ---

// GraphNodeType classifies a graph node.
type GraphNodeType string

const (
	GraphNodeService    GraphNodeType = "service"
	GraphNodeHost       GraphNodeType = "host"
	GraphNodeContainer  GraphNodeType = "container"
	GraphNodeMetric     GraphNodeType = "metric"
	GraphNodeCustom     GraphNodeType = "custom"
)

// GraphEdgeType classifies a graph relationship.
type GraphEdgeType string

const (
	GraphEdgeDependsOn  GraphEdgeType = "depends_on"
	GraphEdgeCallsTo    GraphEdgeType = "calls_to"
	GraphEdgeRunsOn     GraphEdgeType = "runs_on"
	GraphEdgeEmits      GraphEdgeType = "emits"
	GraphEdgeCustom     GraphEdgeType = "custom"
)

// GraphNode represents an entity in the topology graph.
type GraphNode struct {
	ID         string            `json:"id"`
	Type       GraphNodeType     `json:"type"`
	Labels     map[string]string `json:"labels,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Created    time.Time         `json:"created"`
	Updated    time.Time         `json:"updated"`
}

// GraphEdge represents a relationship between two nodes.
type GraphEdge struct {
	ID         string            `json:"id"`
	Source     string            `json:"source"`
	Target     string            `json:"target"`
	Type       GraphEdgeType     `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	ValidFrom  int64             `json:"valid_from,omitempty"`
	ValidTo    int64             `json:"valid_to,omitempty"`
	Weight     float64           `json:"weight"`
	Created    time.Time         `json:"created"`
}

// GraphTraversalResult is returned by graph traversal queries.
type GraphTraversalResult struct {
	Paths    []GraphPath  `json:"paths"`
	Nodes    []GraphNode  `json:"nodes"`
	Edges    []GraphEdge  `json:"edges"`
	Duration int64        `json:"duration_ms"`
}

// GraphPath is a sequence of nodes/edges in a traversal.
type GraphPath struct {
	Nodes []string `json:"nodes"`
	Edges []string `json:"edges"`
	Depth int      `json:"depth"`
}

// GraphStore manages the graph model.
type GraphStore struct {
	nodes    map[string]*GraphNode
	edges    map[string]*GraphEdge
	outEdges map[string][]string // nodeID -> edgeIDs
	inEdges  map[string][]string // nodeID -> edgeIDs
	mu       sync.RWMutex
}

// NewGraphStore creates a new graph store.
func NewGraphStore() *GraphStore {
	return &GraphStore{
		nodes:    make(map[string]*GraphNode),
		edges:    make(map[string]*GraphEdge),
		outEdges: make(map[string][]string),
		inEdges:  make(map[string][]string),
	}
}

// AddNode adds or updates a graph node.
func (gs *GraphStore) AddNode(node GraphNode) error {
	if node.ID == "" {
		return errors.New("graph: node ID required")
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	if existing, ok := gs.nodes[node.ID]; ok {
		existing.Labels = node.Labels
		existing.Properties = node.Properties
		existing.Updated = time.Now()
		return nil
	}

	node.Created = time.Now()
	node.Updated = time.Now()
	gs.nodes[node.ID] = &node
	return nil
}

// GetNode returns a node by ID.
func (gs *GraphStore) GetNode(id string) (*GraphNode, bool) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	n, ok := gs.nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// RemoveNode removes a node and all connected edges.
func (gs *GraphStore) RemoveNode(id string) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, ok := gs.nodes[id]; !ok {
		return fmt.Errorf("graph: node %q not found", id)
	}

	// Remove connected edges
	for _, edgeID := range gs.outEdges[id] {
		e := gs.edges[edgeID]
		if e != nil {
			gs.removeEdgeFromIndex(e)
		}
		delete(gs.edges, edgeID)
	}
	for _, edgeID := range gs.inEdges[id] {
		e := gs.edges[edgeID]
		if e != nil {
			gs.removeEdgeFromIndex(e)
		}
		delete(gs.edges, edgeID)
	}

	delete(gs.nodes, id)
	delete(gs.outEdges, id)
	delete(gs.inEdges, id)
	return nil
}

// AddEdge creates a relationship between two nodes.
func (gs *GraphStore) AddEdge(edge GraphEdge) error {
	if edge.Source == "" || edge.Target == "" {
		return errors.New("graph: source and target required")
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, ok := gs.nodes[edge.Source]; !ok {
		return fmt.Errorf("graph: source node %q not found", edge.Source)
	}
	if _, ok := gs.nodes[edge.Target]; !ok {
		return fmt.Errorf("graph: target node %q not found", edge.Target)
	}

	if edge.ID == "" {
		edge.ID = fmt.Sprintf("%s->%s:%s", edge.Source, edge.Target, edge.Type)
	}
	edge.Created = time.Now()

	gs.edges[edge.ID] = &edge
	gs.outEdges[edge.Source] = append(gs.outEdges[edge.Source], edge.ID)
	gs.inEdges[edge.Target] = append(gs.inEdges[edge.Target], edge.ID)
	return nil
}

// RemoveEdge removes an edge by ID.
func (gs *GraphStore) RemoveEdge(id string) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	e, ok := gs.edges[id]
	if !ok {
		return fmt.Errorf("graph: edge %q not found", id)
	}

	gs.removeEdgeFromIndex(e)
	delete(gs.edges, id)
	return nil
}

func (gs *GraphStore) removeEdgeFromIndex(e *GraphEdge) {
	gs.outEdges[e.Source] = removeString(gs.outEdges[e.Source], e.ID)
	gs.inEdges[e.Target] = removeString(gs.inEdges[e.Target], e.ID)
}

func removeString(s []string, val string) []string {
	result := s[:0]
	for _, v := range s {
		if v != val {
			result = append(result, v)
		}
	}
	return result
}

// Neighbors returns adjacent nodes for a given node.
func (gs *GraphStore) Neighbors(nodeID string, direction string) []GraphNode {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var edgeIDs []string
	switch direction {
	case "out":
		edgeIDs = gs.outEdges[nodeID]
	case "in":
		edgeIDs = gs.inEdges[nodeID]
	default: // "both"
		edgeIDs = append(gs.outEdges[nodeID], gs.inEdges[nodeID]...)
	}

	seen := make(map[string]bool)
	var neighbors []GraphNode
	for _, eid := range edgeIDs {
		e := gs.edges[eid]
		if e == nil {
			continue
		}
		targetID := e.Target
		if targetID == nodeID {
			targetID = e.Source
		}
		if !seen[targetID] {
			seen[targetID] = true
			if n, ok := gs.nodes[targetID]; ok {
				neighbors = append(neighbors, *n)
			}
		}
	}
	return neighbors
}

// Traverse performs a BFS traversal from a start node up to maxDepth.
func (gs *GraphStore) Traverse(startID string, maxDepth int, edgeTypes []GraphEdgeType) *GraphTraversalResult {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	start := time.Now()

	if _, ok := gs.nodes[startID]; !ok {
		return &GraphTraversalResult{}
	}

	edgeFilter := make(map[GraphEdgeType]bool)
	for _, et := range edgeTypes {
		edgeFilter[et] = true
	}

	visited := map[string]bool{startID: true}
	queue := []struct {
		nodeID string
		depth  int
		path   []string
	}{{startID, 0, []string{startID}}}

	var paths []GraphPath
	var resultNodes []GraphNode
	var resultEdges []GraphEdge

	if n, ok := gs.nodes[startID]; ok {
		resultNodes = append(resultNodes, *n)
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		for _, eid := range gs.outEdges[current.nodeID] {
			e := gs.edges[eid]
			if e == nil {
				continue
			}
			if len(edgeFilter) > 0 && !edgeFilter[e.Type] {
				continue
			}

			target := e.Target
			if !visited[target] {
				visited[target] = true

				if n, ok := gs.nodes[target]; ok {
					resultNodes = append(resultNodes, *n)
				}
				resultEdges = append(resultEdges, *e)

				newPath := make([]string, len(current.path)+1)
				copy(newPath, current.path)
				newPath[len(current.path)] = target

				paths = append(paths, GraphPath{
					Nodes: newPath,
					Edges: []string{eid},
					Depth: current.depth + 1,
				})

				queue = append(queue, struct {
					nodeID string
					depth  int
					path   []string
				}{target, current.depth + 1, newPath})
			}
		}
	}

	return &GraphTraversalResult{
		Paths:    paths,
		Nodes:    resultNodes,
		Edges:    resultEdges,
		Duration: time.Since(start).Milliseconds(),
	}
}

// NodeCount returns the number of nodes.
func (gs *GraphStore) NodeCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return len(gs.nodes)
}

// EdgeCount returns the number of edges.
func (gs *GraphStore) EdgeCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return len(gs.edges)
}

// FindNodes returns nodes matching label criteria.
func (gs *GraphStore) FindNodes(nodeType GraphNodeType, labels map[string]string) []GraphNode {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var result []GraphNode
	for _, n := range gs.nodes {
		if nodeType != "" && n.Type != nodeType {
			continue
		}
		match := true
		for k, v := range labels {
			if n.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			result = append(result, *n)
		}
	}
	return result
}

// --- Document Store Extensions ---

// DocumentIndex provides inverted index functionality for documents.
type DocumentIndex struct {
	index map[string]map[string]bool // term -> set of docIDs
	mu    sync.RWMutex
}

// NewDocumentIndex creates a new document index.
func NewDocumentIndex() *DocumentIndex {
	return &DocumentIndex{
		index: make(map[string]map[string]bool),
	}
}

// Index indexes a document by extracting terms.
func (di *DocumentIndex) Index(docID string, data map[string]interface{}) {
	di.mu.Lock()
	defer di.mu.Unlock()

	terms := extractTerms(data, "")
	for _, term := range terms {
		if di.index[term] == nil {
			di.index[term] = make(map[string]bool)
		}
		di.index[term][docID] = true
	}
}

// Search returns document IDs matching a term.
func (di *DocumentIndex) Search(term string) []string {
	di.mu.RLock()
	defer di.mu.RUnlock()

	term = strings.ToLower(term)
	docs := di.index[term]
	result := make([]string, 0, len(docs))
	for docID := range docs {
		result = append(result, docID)
	}
	sort.Strings(result)
	return result
}

// Remove removes a document from the index.
func (di *DocumentIndex) Remove(docID string) {
	di.mu.Lock()
	defer di.mu.Unlock()

	for term, docs := range di.index {
		delete(docs, docID)
		if len(docs) == 0 {
			delete(di.index, term)
		}
	}
}

// TermCount returns the number of indexed terms.
func (di *DocumentIndex) TermCount() int {
	di.mu.RLock()
	defer di.mu.RUnlock()
	return len(di.index)
}

func extractTerms(data map[string]interface{}, prefix string) []string {
	var terms []string
	for key, val := range data {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}

		switch v := val.(type) {
		case string:
			// Index both the path and value
			terms = append(terms, strings.ToLower(path+":"+v))
			for _, word := range strings.Fields(strings.ToLower(v)) {
				terms = append(terms, word)
			}
		case float64:
			terms = append(terms, fmt.Sprintf("%s:%g", strings.ToLower(path), v))
		case map[string]interface{}:
			terms = append(terms, extractTerms(v, path)...)
		}
	}
	return terms
}

// --- Cross-Model Query ---

// CrossModelQuery combines time-series, graph, and document queries.
type CrossModelQuery struct {
	// Time-series component
	MetricQuery *Query `json:"metric_query,omitempty"`

	// Graph component
	GraphStartNode string          `json:"graph_start_node,omitempty"`
	GraphMaxDepth  int             `json:"graph_max_depth,omitempty"`
	GraphEdgeTypes []GraphEdgeType `json:"graph_edge_types,omitempty"`

	// Document component
	DocSearch   string `json:"doc_search,omitempty"`
	DocCollection string `json:"doc_collection,omitempty"`
}

// CrossModelResult combines results from all models.
type CrossModelResult struct {
	Points     []Point              `json:"points,omitempty"`
	GraphResult *GraphTraversalResult `json:"graph,omitempty"`
	Documents  []*Document          `json:"documents,omitempty"`
	Duration   int64                `json:"duration_ms"`
}

// MultiModelGraphStore combines graph, document, and time-series capabilities.
type MultiModelGraphStore struct {
	graph    *GraphStore
	docIndex *DocumentIndex
	docs     map[string]*Document
	db       *DB
	mu       sync.RWMutex
}

// NewMultiModelGraphStore creates a new multi-model store with graph+document support.
func NewMultiModelGraphStore(db *DB) *MultiModelGraphStore {
	return &MultiModelGraphStore{
		graph:    NewGraphStore(),
		docIndex: NewDocumentIndex(),
		docs:     make(map[string]*Document),
		db:       db,
	}
}

// Graph returns the graph store.
func (mm *MultiModelGraphStore) Graph() *GraphStore {
	return mm.graph
}

// DocIndex returns the document index.
func (mm *MultiModelGraphStore) DocIndex() *DocumentIndex {
	return mm.docIndex
}

// StoreDocument stores and indexes a document.
func (mm *MultiModelGraphStore) StoreDocument(doc *Document) error {
	if doc == nil || doc.ID == "" {
		return errors.New("multi_model_graph: document ID required")
	}

	mm.mu.Lock()
	defer mm.mu.Unlock()

	doc.Created = time.Now()
	doc.Updated = time.Now()
	mm.docs[doc.ID] = doc
	mm.docIndex.Index(doc.ID, doc.Data)
	return nil
}

// GetDocument returns a document by ID.
func (mm *MultiModelGraphStore) GetDocument(id string) (*Document, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	doc, ok := mm.docs[id]
	return doc, ok
}

// SearchDocuments returns documents matching a search term.
func (mm *MultiModelGraphStore) SearchDocuments(term string) []*Document {
	docIDs := mm.docIndex.Search(term)

	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var results []*Document
	for _, id := range docIDs {
		if doc, ok := mm.docs[id]; ok {
			results = append(results, doc)
		}
	}
	return results
}

// CrossModelQueryExecute runs a cross-model query spanning metrics, graph, and documents.
func (mm *MultiModelGraphStore) CrossModelQueryExecute(q CrossModelQuery) (*CrossModelResult, error) {
	start := time.Now()
	result := &CrossModelResult{}

	// Time-series query
	if q.MetricQuery != nil && mm.db != nil {
		if r, err := mm.db.Execute(q.MetricQuery); err == nil {
			result.Points = r.Points
		}
	}

	// Graph traversal
	if q.GraphStartNode != "" {
		maxDepth := q.GraphMaxDepth
		if maxDepth <= 0 {
			maxDepth = 3
		}
		result.GraphResult = mm.graph.Traverse(q.GraphStartNode, maxDepth, q.GraphEdgeTypes)
	}

	// Document search
	if q.DocSearch != "" {
		result.Documents = mm.SearchDocuments(q.DocSearch)
	}

	result.Duration = time.Since(start).Milliseconds()
	return result, nil
}

// ExportTopology exports the graph as JSON for visualization.
func (mm *MultiModelGraphStore) ExportTopology() ([]byte, error) {
	mm.graph.mu.RLock()
	defer mm.graph.mu.RUnlock()

	nodes := make([]GraphNode, 0, len(mm.graph.nodes))
	for _, n := range mm.graph.nodes {
		nodes = append(nodes, *n)
	}
	edges := make([]GraphEdge, 0, len(mm.graph.edges))
	for _, e := range mm.graph.edges {
		edges = append(edges, *e)
	}

	topology := map[string]interface{}{
		"nodes": nodes,
		"edges": edges,
	}
	return json.Marshal(topology)
}
