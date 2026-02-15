package chronicle

import (
	"testing"
	"time"
)

func TestChronicleStudioCreateProject(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, err := studio.CreateProject("TestProject", "A test project", "alice")
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}
	if p.ID == "" {
		t.Errorf("expected non-empty project ID")
	}
	if p.Name != "TestProject" {
		t.Errorf("expected Name 'TestProject', got %q", p.Name)
	}
	if p.Owner != "alice" {
		t.Errorf("expected Owner 'alice', got %q", p.Owner)
	}
}

func TestChronicleStudioListProjects(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	for i := 0; i < 3; i++ {
		_, err := studio.CreateProject("Project"+string(rune('A'+i)), "desc", "owner")
		if err != nil {
			t.Fatalf("CreateProject %d failed: %v", i, err)
		}
		time.Sleep(time.Nanosecond)
	}
	projects := studio.ListProjects()
	if len(projects) != 3 {
		t.Errorf("expected 3 projects, got %d", len(projects))
	}
}

func TestChronicleStudioDeleteProject(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, err := studio.CreateProject("ToDelete", "desc", "owner")
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}
	if err := studio.DeleteProject(p.ID); err != nil {
		t.Fatalf("DeleteProject failed: %v", err)
	}
	projects := studio.ListProjects()
	if len(projects) != 0 {
		t.Errorf("expected 0 projects after delete, got %d", len(projects))
	}
}

func TestChronicleStudioGetProjectNotFound(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	_, err := studio.GetProject("nonexistent")
	if err == nil {
		t.Errorf("expected error for non-existent project, got nil")
	}
}

func TestChronicleStudioCreateNotebook(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, err := studio.CreateProject("Proj", "desc", "owner")
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}
	nb, err := studio.CreateNotebook(p.ID, "Notebook1", "author1")
	if err != nil {
		t.Fatalf("CreateNotebook failed: %v", err)
	}
	if nb.ID == "" {
		t.Errorf("expected non-empty notebook ID")
	}
	if nb.ProjectID != p.ID {
		t.Errorf("expected ProjectID %q, got %q", p.ID, nb.ProjectID)
	}
}

func TestChronicleStudioAddCell(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")

	types := []StudioCellType{StudioCellQuery, StudioCellMarkdown, StudioCellCode}
	for i, ct := range types {
		_, err := studio.AddCell(nb.ID, ct, "content")
		if err != nil {
			t.Fatalf("AddCell %d failed: %v", i, err)
		}
		time.Sleep(time.Nanosecond)
	}

	got, _ := studio.GetNotebook(nb.ID)
	if len(got.Cells) != 3 {
		t.Errorf("expected 3 cells, got %d", len(got.Cells))
	}
}

func TestChronicleStudioUpdateCell(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")
	cell, _ := studio.AddCell(nb.ID, StudioCellQuery, "original")

	if err := studio.UpdateCell(nb.ID, cell.ID, "updated"); err != nil {
		t.Fatalf("UpdateCell failed: %v", err)
	}
	got, _ := studio.GetNotebook(nb.ID)
	if got.Cells[0].Content != "updated" {
		t.Errorf("expected content 'updated', got %q", got.Cells[0].Content)
	}
}

func TestChronicleStudioDeleteCell(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")
	cell, _ := studio.AddCell(nb.ID, StudioCellQuery, "to delete")

	if err := studio.DeleteCell(nb.ID, cell.ID); err != nil {
		t.Fatalf("DeleteCell failed: %v", err)
	}
	got, _ := studio.GetNotebook(nb.ID)
	if len(got.Cells) != 0 {
		t.Errorf("expected 0 cells after delete, got %d", len(got.Cells))
	}
}

func TestChronicleStudioExecuteCell(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")
	cell, _ := studio.AddCell(nb.ID, StudioCellQuery, "SELECT * FROM metrics")

	output, err := studio.ExecuteCell(nb.ID, cell.ID)
	if err != nil {
		t.Fatalf("ExecuteCell failed: %v", err)
	}
	if output.Data == nil {
		t.Errorf("expected non-nil output data")
	}
}

func TestChronicleStudioQuerySuggestions(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	suggestions := studio.GetQuerySuggestions("SELECT")
	if len(suggestions) < 1 {
		t.Errorf("expected at least 1 suggestion for 'SELECT', got %d", len(suggestions))
	}
}

func TestChronicleStudioExportNotebook(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")
	studio.AddCell(nb.ID, StudioCellQuery, "SELECT 1")
	studio.AddCell(nb.ID, StudioCellMarkdown, "# Hello")

	result, err := studio.ExportNotebook(nb.ID, "json")
	if err != nil {
		t.Fatalf("ExportNotebook failed: %v", err)
	}
	if len(result.Data) == 0 {
		t.Errorf("expected non-empty export data")
	}
	if result.Format != "json" {
		t.Errorf("expected format 'json', got %q", result.Format)
	}
}

func TestChronicleStudioCollaborators(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")

	if err := studio.AddCollaborator(p.ID, "bob", "editor"); err != nil {
		t.Fatalf("AddCollaborator failed: %v", err)
	}
	proj, _ := studio.GetProject(p.ID)
	found := false
	for _, c := range proj.Collaborators {
		if c.UserID == "bob" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected collaborator 'bob' in project")
	}

	if err := studio.RemoveCollaborator(p.ID, "bob"); err != nil {
		t.Fatalf("RemoveCollaborator failed: %v", err)
	}
	proj, _ = studio.GetProject(p.ID)
	for _, c := range proj.Collaborators {
		if c.UserID == "bob" {
			t.Errorf("collaborator 'bob' should have been removed")
		}
	}
}

func TestChronicleStudioVersioning(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")

	_, err := studio.SaveVersion(nb.ID, "author", "initial save")
	if err != nil {
		t.Fatalf("SaveVersion failed: %v", err)
	}
	history := studio.GetVersionHistory(nb.ID)
	if len(history) != 1 {
		t.Errorf("expected 1 version in history, got %d", len(history))
	}
}

func TestChronicleStudioStats(t *testing.T) {
	studio := NewChronicleStudio(nil, DefaultChronicleStudioConfig())
	p, _ := studio.CreateProject("Proj", "desc", "owner")
	nb, _ := studio.CreateNotebook(p.ID, "NB", "author")
	studio.AddCell(nb.ID, StudioCellQuery, "q1")
	studio.AddCell(nb.ID, StudioCellMarkdown, "md1")

	stats := studio.Stats()
	if stats.TotalProjects != 1 {
		t.Errorf("expected TotalProjects=1, got %d", stats.TotalProjects)
	}
	if stats.TotalNotebooks != 1 {
		t.Errorf("expected TotalNotebooks=1, got %d", stats.TotalNotebooks)
	}
	if stats.TotalCells != 2 {
		t.Errorf("expected TotalCells=2, got %d", stats.TotalCells)
	}
}

func TestChronicleStudioDefaultConfig(t *testing.T) {
	cfg := DefaultChronicleStudioConfig()
	if !cfg.Enabled {
		t.Errorf("expected Enabled=true")
	}
	if cfg.MaxProjects != 100 {
		t.Errorf("expected MaxProjects=100, got %d", cfg.MaxProjects)
	}
	if cfg.MaxNotebooksPerProject != 50 {
		t.Errorf("expected MaxNotebooksPerProject=50, got %d", cfg.MaxNotebooksPerProject)
	}
	if cfg.MaxCellsPerNotebook != 200 {
		t.Errorf("expected MaxCellsPerNotebook=200, got %d", cfg.MaxCellsPerNotebook)
	}
	if cfg.AutoSaveInterval != 30*time.Second {
		t.Errorf("expected AutoSaveInterval=30s, got %v", cfg.AutoSaveInterval)
	}
	if !cfg.EnableCollaboration {
		t.Errorf("expected EnableCollaboration=true")
	}
	if !cfg.EnableVersioning {
		t.Errorf("expected EnableVersioning=true")
	}
	if !cfg.EnableExport {
		t.Errorf("expected EnableExport=true")
	}
	if len(cfg.ExportFormats) != 4 {
		t.Errorf("expected 4 export formats, got %d", len(cfg.ExportFormats))
	}
}

func TestChronicleStudioMaxProjects(t *testing.T) {
	cfg := DefaultChronicleStudioConfig()
	cfg.MaxProjects = 2
	studio := NewChronicleStudio(nil, cfg)

	for i := 0; i < 2; i++ {
		_, err := studio.CreateProject("P"+string(rune('0'+i)), "desc", "owner")
		if err != nil {
			t.Fatalf("CreateProject %d failed: %v", i, err)
		}
		time.Sleep(time.Nanosecond)
	}

	_, err := studio.CreateProject("P3", "desc", "owner")
	if err == nil {
		t.Errorf("expected error when exceeding MaxProjects, got nil")
	}
}
