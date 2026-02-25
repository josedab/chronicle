package chronicle

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"
)

// Notebook export, collaboration, and project management for Chronicle Studio.

// ExportNotebook exports a notebook to the specified format.
func (s *ChronicleStudio) ExportNotebook(notebookID, format string) (*StudioExportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return nil, fmt.Errorf("notebook not found: %s", notebookID)
	}

	supported := false
	for _, f := range s.config.ExportFormats {
		if f == format {
			supported = true
			break
		}
	}
	if !supported {
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}

	var data []byte
	var err error

	switch format {
	case "json":
		data, err = json.MarshalIndent(nb, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to export as JSON: %w", err)
		}
	case "csv":
		var b strings.Builder
		b.WriteString("cell_id,type,content\n")
		for _, cell := range nb.Cells {
			b.WriteString(fmt.Sprintf("%s,%s,%q\n", cell.ID, cell.Type, cell.Content))
		}
		data = []byte(b.String())
	case "markdown":
		var b strings.Builder
		b.WriteString(fmt.Sprintf("# %s\n\n", nb.Name))
		if nb.Description != "" {
			b.WriteString(fmt.Sprintf("%s\n\n", nb.Description))
		}
		for _, cell := range nb.Cells {
			switch cell.Type {
			case StudioCellMarkdown:
				b.WriteString(cell.Content + "\n\n")
			case StudioCellQuery:
				b.WriteString(fmt.Sprintf("```sql\n%s\n```\n\n", cell.Content))
			case StudioCellCode:
				b.WriteString(fmt.Sprintf("```\n%s\n```\n\n", cell.Content))
			default:
				b.WriteString(fmt.Sprintf("_%s_\n\n", cell.Content))
			}
		}
		data = []byte(b.String())
	case "html":
		var b strings.Builder
		b.WriteString(fmt.Sprintf("<html><head><title>%s</title></head><body>\n", html.EscapeString(nb.Name)))
		b.WriteString(fmt.Sprintf("<h1>%s</h1>\n", html.EscapeString(nb.Name)))
		for _, cell := range nb.Cells {
			switch cell.Type {
			case StudioCellMarkdown:
				b.WriteString(fmt.Sprintf("<div class=\"markdown\">%s</div>\n", html.EscapeString(cell.Content)))
			case StudioCellQuery:
				b.WriteString(fmt.Sprintf("<pre class=\"query\">%s</pre>\n", html.EscapeString(cell.Content)))
			case StudioCellCode:
				b.WriteString(fmt.Sprintf("<pre class=\"code\">%s</pre>\n", html.EscapeString(cell.Content)))
			default:
				b.WriteString(fmt.Sprintf("<div>%s</div>\n", html.EscapeString(cell.Content)))
			}
		}
		b.WriteString("</body></html>")
		data = []byte(b.String())
	}

	s.exportsGenerated++
	return &StudioExportResult{
		Format:      format,
		Data:        data,
		Filename:    fmt.Sprintf("%s.%s", nb.Name, format),
		Size:        int64(len(data)),
		GeneratedAt: time.Now(),
	}, nil
}

// AddCollaborator adds a collaborator to a project.
func (s *ChronicleStudio) AddCollaborator(projectID, userID, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.projects[projectID]
	if !ok {
		return fmt.Errorf("project not found: %s", projectID)
	}
	if len(p.Collaborators) >= s.config.MaxCollaborators {
		return fmt.Errorf("maximum collaborators (%d) reached", s.config.MaxCollaborators)
	}
	for _, c := range p.Collaborators {
		if c.UserID == userID {
			return fmt.Errorf("user %s is already a collaborator", userID)
		}
	}
	p.Collaborators = append(p.Collaborators, Collaborator{
		UserID:    userID,
		Role:      role,
		InvitedAt: time.Now(),
		Active:    true,
	})
	p.UpdatedAt = time.Now()
	return nil
}

// RemoveCollaborator removes a collaborator from a project.
func (s *ChronicleStudio) RemoveCollaborator(projectID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.projects[projectID]
	if !ok {
		return fmt.Errorf("project not found: %s", projectID)
	}
	for i, c := range p.Collaborators {
		if c.UserID == userID {
			if c.Role == "owner" {
				return fmt.Errorf("cannot remove the project owner")
			}
			p.Collaborators = append(p.Collaborators[:i], p.Collaborators[i+1:]...)
			p.UpdatedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("collaborator not found: %s", userID)
}

// GetVersionHistory returns the version history for a notebook.
func (s *ChronicleStudio) GetVersionHistory(notebookID string) []NotebookVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if versions, ok := s.versions[notebookID]; ok {
		return versions
	}
	return []NotebookVersion{}
}

// SaveVersion saves a version snapshot of a notebook.
func (s *ChronicleStudio) SaveVersion(notebookID, author, message string) (*NotebookVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return nil, fmt.Errorf("notebook not found: %s", notebookID)
	}

	versions := s.versions[notebookID]
	if len(versions) >= s.config.MaxVersionHistory {
		versions = versions[1:]
	}

	ver := NotebookVersion{
		Version:   nb.Version,
		Timestamp: time.Now(),
		Author:    author,
		Message:   message,
		CellCount: len(nb.Cells),
	}
	nb.Version++
	nb.UpdatedAt = ver.Timestamp
	s.versions[notebookID] = append(versions, ver)
	return &ver, nil
}

// Stats returns aggregate statistics for Chronicle Studio.
func (s *ChronicleStudio) Stats() StudioStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StudioStats{
		TotalProjects:    len(s.projects),
		TotalNotebooks:   len(s.notebooks),
		CellsByType:      make(map[StudioCellType]int),
		QueriesExecuted:  s.queriesExecuted,
		ExportsGenerated: s.exportsGenerated,
	}

	for _, nb := range s.notebooks {
		stats.TotalCells += len(nb.Cells)
		for _, cell := range nb.Cells {
			stats.CellsByType[cell.Type]++
		}
	}

	activeCollabs := make(map[string]bool)
	for _, p := range s.projects {
		for _, c := range p.Collaborators {
			if c.Active {
				activeCollabs[c.UserID] = true
			}
		}
	}
	stats.ActiveCollaborators = len(activeCollabs)

	if s.queriesExecuted > 0 {
		stats.AvgQueryDurationMs = float64(s.totalQueryDurationMs) / float64(s.queriesExecuted)
	}

	return stats
}

// RegisterHTTPHandlers registers Chronicle Studio HTTP routes.
func (s *ChronicleStudio) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/studio/projects", s.handleProjects)
	mux.HandleFunc("/api/v1/studio/project/", s.handleProject)
	mux.HandleFunc("/api/v1/studio/notebooks", s.handleCreateNotebook)
	mux.HandleFunc("/api/v1/studio/notebook/cells", s.handleAddCell)
	mux.HandleFunc("/api/v1/studio/notebook/cell/execute", s.handleExecuteCell)
	mux.HandleFunc("/api/v1/studio/notebook/cell", s.handleCell)
	mux.HandleFunc("/api/v1/studio/notebook/", s.handleNotebook)
	mux.HandleFunc("/api/v1/studio/suggestions", s.handleSuggestions)
	mux.HandleFunc("/api/v1/studio/export", s.handleExport)
	mux.HandleFunc("/api/v1/studio/stats", s.handleStats)
}

func (s *ChronicleStudio) handleProjects(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name        string `json:"name"`
			Description string `json:"description"`
			Owner       string `json:"owner"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		p, err := s.CreateProject(req.Name, req.Description, req.Owner)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(p)
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.ListProjects())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *ChronicleStudio) handleProject(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/studio/project/")
	if id == "" {
		http.Error(w, "project ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		p, err := s.GetProject(id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	case http.MethodDelete:
		if err := s.DeleteProject(id); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *ChronicleStudio) handleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ProjectID string `json:"project_id"`
		Name      string `json:"name"`
		Author    string `json:"author"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	nb, err := s.CreateNotebook(req.ProjectID, req.Name, req.Author)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(nb)
}

func (s *ChronicleStudio) handleNotebook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/studio/notebook/")
	if id == "" {
		http.Error(w, "notebook ID required", http.StatusBadRequest)
		return
	}
	nb, err := s.GetNotebook(id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nb)
}

func (s *ChronicleStudio) handleAddCell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NotebookID string         `json:"notebook_id"`
		Type       StudioCellType `json:"type"`
		Content    string         `json:"content"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cell, err := s.AddCell(req.NotebookID, req.Type, req.Content)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(cell)
}

func (s *ChronicleStudio) handleCell(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		var req struct {
			NotebookID string `json:"notebook_id"`
			CellID     string `json:"cell_id"`
			Content    string `json:"content"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := s.UpdateCell(req.NotebookID, req.CellID, req.Content); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		var req struct {
			NotebookID string `json:"notebook_id"`
			CellID     string `json:"cell_id"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := s.DeleteCell(req.NotebookID, req.CellID); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *ChronicleStudio) handleExecuteCell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NotebookID string `json:"notebook_id"`
		CellID     string `json:"cell_id"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	output, err := s.ExecuteCell(req.NotebookID, req.CellID)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(output)
}

func (s *ChronicleStudio) handleSuggestions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query().Get("q")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.GetQuerySuggestions(q))
}

func (s *ChronicleStudio) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NotebookID string `json:"notebook_id"`
		Format     string `json:"format"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	result, err := s.ExportNotebook(req.NotebookID, req.Format)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *ChronicleStudio) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.Stats())
}
