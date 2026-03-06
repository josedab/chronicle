package chronicle

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNotebookStore_SaveLoad(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	store := NewNotebookStore(engine)

	nb, err := engine.CreateNotebook(Notebook{Title: "Test Notebook"})
	if err != nil {
		t.Fatalf("create notebook: %v", err)
	}

	data, err := store.Save(nb.ID)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Delete and reload
	engine.DeleteNotebook(nb.ID)
	loaded, err := store.Load(data)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Title != "Test Notebook" {
		t.Fatalf("expected 'Test Notebook', got %q", loaded.Title)
	}
}

func TestNotebookStore_DirtyTracking(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	store := NewNotebookStore(engine)

	store.MarkDirty("nb-1")
	store.MarkDirty("nb-2")

	dirty := store.DirtyNotebooks()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty notebooks, got %d", len(dirty))
	}
}

func TestTemplateRegistry(t *testing.T) {
	tr := NewTemplateRegistry()

	templates := tr.ListTemplates()
	if len(templates) < 2 {
		t.Fatalf("expected at least 2 built-in templates, got %d", len(templates))
	}

	tmpl, ok := tr.GetTemplate("service-overview")
	if !ok {
		t.Fatal("service-overview template not found")
	}
	if len(tmpl.Parameters) == 0 {
		t.Fatal("expected parameters in template")
	}
}

func TestTemplateInstantiate(t *testing.T) {
	tr := NewTemplateRegistry()

	params := map[string]string{
		"service":  "api-gateway",
		"duration": "5m",
	}

	nb, err := tr.Instantiate("service-overview", params)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}

	if nb.Title == "" {
		t.Fatal("expected non-empty title")
	}

	// Check parameter substitution happened
	found := false
	for _, cell := range nb.Cells {
		if cell.Type == CellQuery && len(cell.Source) > 0 {
			if !containsStr(cell.Source, "{{") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected parameter substitution in cells")
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestTemplateInstantiate_MissingRequired(t *testing.T) {
	tr := NewTemplateRegistry()

	_, err := tr.Instantiate("service-overview", map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing required parameter")
	}
}

func TestGrafanaExport(t *testing.T) {
	nb := &Notebook{
		Title: "Test Dashboard",
		Cells: []NotebookCell{
			{Type: CellMarkdown, Source: "# Overview"},
			{Type: CellQuery, Source: "rate(http_requests[5m])"},
			{Type: CellChart, Source: "cpu_usage"},
		},
	}

	exporter := &GrafanaDashboardExport{}
	dashboard := exporter.ExportToGrafana(nb)

	dash, ok := dashboard["dashboard"].(map[string]interface{})
	if !ok {
		t.Fatal("expected dashboard key")
	}
	if dash["title"] != "Test Dashboard" {
		t.Fatalf("expected title 'Test Dashboard', got %v", dash["title"])
	}
	panels, ok := dash["panels"].([]interface{})
	if !ok || len(panels) != 3 {
		t.Fatalf("expected 3 panels, got %d", len(panels))
	}
}

func TestNotebookSharing(t *testing.T) {
	mgr := NewNotebookSharingManager()

	// Create share
	share, err := mgr.CreateShare("nb-1", "user1", "view", 10*time.Minute)
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	if share.Token == "" {
		t.Fatal("expected non-empty token")
	}

	// Validate access
	access, err := mgr.ValidateAccess(share.Token)
	if err != nil {
		t.Fatalf("validate access: %v", err)
	}
	if access.Permission != "view" {
		t.Fatalf("expected 'view' permission, got %s", access.Permission)
	}
	if access.ViewCount != 1 {
		t.Fatalf("expected view count 1, got %d", access.ViewCount)
	}

	// Revoke
	if !mgr.RevokeShare(share.Token) {
		t.Fatal("expected successful revocation")
	}

	// Should fail after revoke
	_, err = mgr.ValidateAccess(share.Token)
	if err == nil {
		t.Fatal("expected error after revocation")
	}
}

func TestNotebookSharing_InvalidPermission(t *testing.T) {
	mgr := NewNotebookSharingManager()
	_, err := mgr.CreateShare("nb-1", "user1", "admin", 0)
	if err == nil {
		t.Fatal("expected error for invalid permission")
	}
}

func TestWorkbenchAPI_Creation(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	api := NewWorkbenchAPI(engine)
	if api == nil {
		t.Fatal("expected non-nil API")
	}
	if api.templates == nil || api.sharing == nil || api.store == nil {
		t.Fatal("expected non-nil components")
	}
}

func TestDefaultChartConfig(t *testing.T) {
	cfg := DefaultChartConfig("line")
	if cfg.Type != "line" {
		t.Fatalf("expected 'line', got %s", cfg.Type)
	}
	if cfg.Width != 800 || cfg.Height != 400 {
		t.Fatal("unexpected default dimensions")
	}
	if len(cfg.Colors) == 0 {
		t.Fatal("expected default colors")
	}
}

func TestOTDocument_Insert(t *testing.T) {
	doc := NewOTDocument("hello world")

	err := doc.Apply(OTOperation{Type: "insert", Position: 5, Text: " beautiful", UserID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if doc.GetContent() != "hello beautiful world" {
		t.Fatalf("expected 'hello beautiful world', got %q", doc.GetContent())
	}
	if doc.GetVersion() != 1 {
		t.Fatalf("expected version 1, got %d", doc.GetVersion())
	}
}

func TestOTDocument_Delete(t *testing.T) {
	doc := NewOTDocument("hello world")

	err := doc.Apply(OTOperation{Type: "delete", Position: 5, Count: 6, UserID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if doc.GetContent() != "hello" {
		t.Fatalf("expected 'hello', got %q", doc.GetContent())
	}
}

func TestOTDocument_InvalidOps(t *testing.T) {
	doc := NewOTDocument("hello")

	err := doc.Apply(OTOperation{Type: "insert", Position: 100, Text: "x", UserID: "u1"})
	if err == nil {
		t.Fatal("expected error for out-of-range insert")
	}

	err = doc.Apply(OTOperation{Type: "delete", Position: 0, Count: 100, UserID: "u1"})
	if err == nil {
		t.Fatal("expected error for out-of-range delete")
	}

	err = doc.Apply(OTOperation{Type: "unknown", UserID: "u1"})
	if err == nil {
		t.Fatal("expected error for unknown operation type")
	}
}

func TestOTTransform_InsertInsert(t *testing.T) {
	op1 := OTOperation{Type: "insert", Position: 3, Text: "abc", UserID: "u1"}
	op2 := OTOperation{Type: "insert", Position: 5, Text: "xyz", UserID: "u2"}

	transformed := Transform(op1, op2)
	// op2's position should shift by len(op1.Text)=3 since op2.Position >= op1.Position
	if transformed.Position != 8 {
		t.Fatalf("expected position 8, got %d", transformed.Position)
	}
}

func TestOTTransform_InsertDelete(t *testing.T) {
	op1 := OTOperation{Type: "insert", Position: 3, Text: "abc", UserID: "u1"}
	op2 := OTOperation{Type: "delete", Position: 5, Count: 2, UserID: "u2"}

	transformed := Transform(op1, op2)
	if transformed.Position != 8 {
		t.Fatalf("expected position 8, got %d", transformed.Position)
	}
}

func TestPresenceManager(t *testing.T) {
	pm := NewPresenceManager(time.Second)

	pm.UpdatePresence("nb-1", UserPresence{
		UserID: "u1", Name: "Alice", Color: "#ff0000", CursorPos: 10,
	})
	pm.UpdatePresence("nb-1", UserPresence{
		UserID: "u2", Name: "Bob", Color: "#0000ff", CursorPos: 20,
	})

	users := pm.GetPresence("nb-1")
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	pm.RemovePresence("nb-1", "u1")
	users = pm.GetPresence("nb-1")
	if len(users) != 1 {
		t.Fatalf("expected 1 user after removal, got %d", len(users))
	}
}

func TestPresenceManager_Cleanup(t *testing.T) {
	pm := NewPresenceManager(time.Millisecond)

	pm.UpdatePresence("nb-1", UserPresence{UserID: "u1", Name: "Alice"})
	time.Sleep(5 * time.Millisecond)

	removed := pm.CleanupStale()
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
}

func TestWSMessageTypes(t *testing.T) {
	msg := WSMessage{
		Type:       WSMsgTypeEdit,
		NotebookID: "nb-1",
		UserID:     "u1",
		Operation:  &OTOperation{Type: "insert", Position: 0, Text: "hi"},
		Timestamp:  time.Now(),
	}

	if msg.Type != WSMsgTypeEdit {
		t.Fatal("expected edit message type")
	}
	if msg.Operation == nil {
		t.Fatal("expected operation in edit message")
	}
}

func TestCollaborativeNotebookSession(t *testing.T) {
	nb := &Notebook{
		ID:    "nb-1",
		Title: "Test",
		Cells: []NotebookCell{
			{ID: "c1", Type: CellQuery, Source: "rate(http[5m])"},
			{ID: "c2", Type: CellMarkdown, Source: "# Hello"},
		},
	}

	session := NewCollaborativeNotebookSession("nb-1", nb)

	// Verify initial cell content
	content, ok := session.GetCellContent("c1")
	if !ok || content != "rate(http[5m])" {
		t.Fatalf("expected initial content, got %q", content)
	}

	// Apply an edit: insert " world" at end of c2
	msg := WSMessage{
		Type:       WSMsgTypeEdit,
		NotebookID: "nb-1",
		CellID:     "c2",
		UserID:     "alice",
		Operation:  &OTOperation{Type: "insert", Position: 7, Text: " World"},
	}
	broadcast, err := session.HandleMessage(msg)
	if err != nil {
		t.Fatalf("handle edit: %v", err)
	}
	if broadcast.Type != WSMsgTypeEdit {
		t.Errorf("expected edit broadcast, got %s", broadcast.Type)
	}

	content, _ = session.GetCellContent("c2")
	if content != "# Hello World" {
		t.Fatalf("expected '# Hello World', got %q", content)
	}

	// Handle presence
	presMsg := WSMessage{
		Type: WSMsgTypePresence,
		Presence: &UserPresence{
			UserID: "alice", Name: "Alice", Color: "#ff0000",
		},
	}
	session.HandleMessage(presMsg)
	users := session.ActiveUsers()
	if len(users) != 1 {
		t.Fatalf("expected 1 active user, got %d", len(users))
	}

	// Add a new cell
	addMsg := WSMessage{
		Type:   WSMsgTypeAddCell,
		CellID: "c3",
		UserID: "alice",
	}
	session.HandleMessage(addMsg)
	_, ok = session.GetCellContent("c3")
	if !ok {
		t.Fatal("expected cell c3 to exist after add")
	}
}

func TestCollaborativeSession_ConcurrentEdits(t *testing.T) {
	nb := &Notebook{
		Cells: []NotebookCell{
			{ID: "c1", Source: "hello"},
		},
	}
	session := NewCollaborativeNotebookSession("nb-1", nb)

	// User A inserts at position 5
	_, err := session.ApplyEdit(WSMessage{
		CellID:    "c1",
		UserID:    "alice",
		Operation: &OTOperation{Type: "insert", Position: 5, Text: " world"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// After A's edit, content is "hello world"
	content, _ := session.GetCellContent("c1")
	if content != "hello world" {
		t.Fatalf("expected 'hello world', got %q", content)
	}

	// User B inserts at position 0
	_, err = session.ApplyEdit(WSMessage{
		CellID:    "c1",
		UserID:    "bob",
		Operation: &OTOperation{Type: "insert", Position: 0, Text: "say: "},
	})
	if err != nil {
		t.Fatal(err)
	}

	content, _ = session.GetCellContent("c1")
	if content != "say: hello world" {
		t.Fatalf("expected 'say: hello world', got %q", content)
	}
}

func TestCollaborativeSession_HandleError(t *testing.T) {
	session := NewCollaborativeNotebookSession("nb-1", nil)

	// Edit with no operation should error
	_, err := session.HandleMessage(WSMessage{
		Type:   WSMsgTypeEdit,
		CellID: "c1",
	})
	if err == nil {
		t.Fatal("expected error for missing operation")
	}
}

// Suppress unused import warning
var _ = json.Marshal
