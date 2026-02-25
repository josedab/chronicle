package chronicle

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var studioProjectCounter atomic.Int64

// ChronicleStudioConfig configures the Visual Time-Series Programming IDE.
type ChronicleStudioConfig struct {
	Enabled                bool          `json:"enabled"`
	MaxProjects            int           `json:"max_projects"`
	MaxNotebooksPerProject int           `json:"max_notebooks_per_project"`
	MaxCellsPerNotebook    int           `json:"max_cells_per_notebook"`
	AutoSaveInterval       time.Duration `json:"auto_save_interval"`
	MaxQueryTimeout        time.Duration `json:"max_query_timeout"`
	EnableCollaboration    bool          `json:"enable_collaboration"`
	MaxCollaborators       int           `json:"max_collaborators"`
	EnableVersioning       bool          `json:"enable_versioning"`
	MaxVersionHistory      int           `json:"max_version_history"`
	EnableExport           bool          `json:"enable_export"`
	ExportFormats          []string      `json:"export_formats"`
}

// DefaultChronicleStudioConfig returns sensible defaults for ChronicleStudio.
func DefaultChronicleStudioConfig() ChronicleStudioConfig {
	return ChronicleStudioConfig{
		Enabled:                true,
		MaxProjects:            100,
		MaxNotebooksPerProject: 50,
		MaxCellsPerNotebook:    200,
		AutoSaveInterval:       30 * time.Second,
		MaxQueryTimeout:        30 * time.Second,
		EnableCollaboration:    true,
		MaxCollaborators:       10,
		EnableVersioning:       true,
		MaxVersionHistory:      50,
		EnableExport:           true,
		ExportFormats:          []string{"json", "csv", "markdown", "html"},
	}
}

// StudioCellType represents the type of a notebook cell in Chronicle Studio.
type StudioCellType string

const (
	StudioCellQuery         StudioCellType = "query"
	StudioCellMarkdown      StudioCellType = "markdown"
	StudioCellVisualization StudioCellType = "visualization"
	StudioCellCode          StudioCellType = "code"
)

// VisualizationType represents a visualization chart type.
type VisualizationType string

const (
	VizLineChart VisualizationType = "line_chart"
	VizBarChart  VisualizationType = "bar_chart"
	VizAreaChart VisualizationType = "area_chart"
	VizScatter   VisualizationType = "scatter"
	VizHeatmap   VisualizationType = "heatmap"
	VizGauge     VisualizationType = "gauge"
	VizTable     VisualizationType = "table"
	VizStat      VisualizationType = "stat"
)

// StudioProject represents a project in Chronicle Studio.
type StudioProject struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	Owner         string          `json:"owner"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Notebooks     []string        `json:"notebooks"`
	Tags          []string        `json:"tags"`
	Settings      ProjectSettings `json:"settings"`
	Collaborators []Collaborator  `json:"collaborators"`
}

// ProjectSettings holds default settings for a project.
type ProjectSettings struct {
	DefaultTimeRange   string        `json:"default_time_range"`
	DefaultRefreshRate time.Duration `json:"default_refresh_rate"`
	Theme              string        `json:"theme"`
	Layout             string        `json:"layout"`
}

// Collaborator represents a user collaborating on a project.
type Collaborator struct {
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
	InvitedAt time.Time `json:"invited_at"`
	Active    bool      `json:"active"`
}

// StudioNotebook represents a notebook in Chronicle Studio.
type StudioNotebook struct {
	ID          string               `json:"id"`
	ProjectID   string               `json:"project_id"`
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Cells       []StudioNotebookCell `json:"cells"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
	Version     int                  `json:"version"`
	Author      string               `json:"author"`
	Tags        []string             `json:"tags"`
}

// StudioNotebookCell represents a single cell in a notebook.
type StudioNotebookCell struct {
	ID        string            `json:"id"`
	Type      StudioCellType    `json:"type"`
	Content   string            `json:"content"`
	Output    *StudioCellOutput `json:"output,omitempty"`
	Position  int               `json:"position"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Collapsed bool              `json:"collapsed"`
}

// StudioCellOutput holds the result of executing a cell.
type StudioCellOutput struct {
	Data       any       `json:"data"`
	Error      string    `json:"error,omitempty"`
	ExecutedAt time.Time `json:"executed_at"`
	DurationMs int64     `json:"duration_ms"`
	RowCount   int       `json:"row_count"`
}

// VisualizationSpec describes a visualization configuration.
type VisualizationSpec struct {
	Type    VisualizationType `json:"type"`
	Title   string            `json:"title"`
	XAxis   AxisConfig        `json:"x_axis"`
	YAxis   AxisConfig        `json:"y_axis"`
	Series  []SeriesConfig    `json:"series"`
	Options map[string]any    `json:"options,omitempty"`
}

// AxisConfig describes an axis in a visualization.
type AxisConfig struct {
	Label  string   `json:"label"`
	Field  string   `json:"field"`
	Format string   `json:"format"`
	Min    *float64 `json:"min,omitempty"`
	Max    *float64 `json:"max,omitempty"`
}

// SeriesConfig describes a data series in a visualization.
type SeriesConfig struct {
	Name  string            `json:"name"`
	Field string            `json:"field"`
	Color string            `json:"color"`
	Type  VisualizationType `json:"type"`
}

// StudioQuerySuggestion represents an auto-complete suggestion.
type StudioQuerySuggestion struct {
	Query       string  `json:"query"`
	Description string  `json:"description"`
	Category    string  `json:"category"`
	Confidence  float64 `json:"confidence"`
}

// NotebookVersion represents a saved version snapshot.
type NotebookVersion struct {
	Version   int       `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Author    string    `json:"author"`
	Message   string    `json:"message"`
	CellCount int       `json:"cell_count"`
}

// StudioExportResult holds the result of exporting a notebook.
type StudioExportResult struct {
	Format      string    `json:"format"`
	Data        []byte    `json:"data"`
	Filename    string    `json:"filename"`
	Size        int64     `json:"size"`
	GeneratedAt time.Time `json:"generated_at"`
}

// StudioStats holds aggregate statistics for Chronicle Studio.
type StudioStats struct {
	TotalProjects       int                    `json:"total_projects"`
	TotalNotebooks      int                    `json:"total_notebooks"`
	TotalCells          int                    `json:"total_cells"`
	CellsByType         map[StudioCellType]int `json:"cells_by_type"`
	QueriesExecuted     int64                  `json:"queries_executed"`
	AvgQueryDurationMs  float64                `json:"avg_query_duration_ms"`
	ActiveCollaborators int                    `json:"active_collaborators"`
	ExportsGenerated    int64                  `json:"exports_generated"`
}

// ChronicleStudio is the Visual Time-Series Programming IDE engine.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type ChronicleStudio struct {
	db                   *DB
	config               ChronicleStudioConfig
	projects             map[string]*StudioProject
	notebooks            map[string]*StudioNotebook
	versions             map[string][]NotebookVersion
	queriesExecuted      int64
	exportsGenerated     int64
	totalQueryDurationMs int64
	mu                   sync.RWMutex
}

// NewChronicleStudio creates a new ChronicleStudio instance.
func NewChronicleStudio(db *DB, cfg ChronicleStudioConfig) *ChronicleStudio {
	return &ChronicleStudio{
		db:        db,
		config:    cfg,
		projects:  make(map[string]*StudioProject),
		notebooks: make(map[string]*StudioNotebook),
		versions:  make(map[string][]NotebookVersion),
	}
}

// CreateProject creates a new project.
func (s *ChronicleStudio) CreateProject(name, description, owner string) (*StudioProject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.projects) >= s.config.MaxProjects {
		return nil, fmt.Errorf("maximum number of projects (%d) reached", s.config.MaxProjects)
	}
	if name == "" {
		return nil, fmt.Errorf("project name is required")
	}

	now := time.Now()
	p := &StudioProject{
		ID:          fmt.Sprintf("proj_%d_%d", now.UnixNano(), studioProjectCounter.Add(1)),
		Name:        name,
		Description: description,
		Owner:       owner,
		CreatedAt:   now,
		UpdatedAt:   now,
		Notebooks:   []string{},
		Tags:        []string{},
		Settings: ProjectSettings{
			DefaultTimeRange:   "1h",
			DefaultRefreshRate: 30 * time.Second,
			Theme:              "dark",
			Layout:             "grid",
		},
		Collaborators: []Collaborator{
			{UserID: owner, Role: "owner", InvitedAt: now, Active: true},
		},
	}
	s.projects[p.ID] = p
	return p, nil
}

// GetProject returns a project by ID.
func (s *ChronicleStudio) GetProject(id string) (*StudioProject, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found: %s", id)
	}
	return p, nil
}

// ListProjects returns all projects.
func (s *ChronicleStudio) ListProjects() []StudioProject {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]StudioProject, 0, len(s.projects))
	for _, p := range s.projects {
		result = append(result, *p)
	}
	return result
}

// DeleteProject removes a project and its notebooks.
func (s *ChronicleStudio) DeleteProject(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.projects[id]
	if !ok {
		return fmt.Errorf("project not found: %s", id)
	}
	for _, nbID := range p.Notebooks {
		delete(s.notebooks, nbID)
		delete(s.versions, nbID)
	}
	delete(s.projects, id)
	return nil
}

// CreateNotebook creates a new notebook within a project.
func (s *ChronicleStudio) CreateNotebook(projectID, name, author string) (*StudioNotebook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.projects[projectID]
	if !ok {
		return nil, fmt.Errorf("project not found: %s", projectID)
	}
	if len(p.Notebooks) >= s.config.MaxNotebooksPerProject {
		return nil, fmt.Errorf("maximum notebooks per project (%d) reached", s.config.MaxNotebooksPerProject)
	}
	if name == "" {
		return nil, fmt.Errorf("notebook name is required")
	}

	now := time.Now()
	nb := &StudioNotebook{
		ID:        fmt.Sprintf("nb_%d", now.UnixNano()),
		ProjectID: projectID,
		Name:      name,
		Author:    author,
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
		Cells:     []StudioNotebookCell{},
		Tags:      []string{},
	}
	s.notebooks[nb.ID] = nb
	p.Notebooks = append(p.Notebooks, nb.ID)
	p.UpdatedAt = now
	return nb, nil
}

// GetNotebook returns a notebook by ID.
func (s *ChronicleStudio) GetNotebook(id string) (*StudioNotebook, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nb, ok := s.notebooks[id]
	if !ok {
		return nil, fmt.Errorf("notebook not found: %s", id)
	}
	return nb, nil
}

// AddCell adds a new cell to a notebook.
func (s *ChronicleStudio) AddCell(notebookID string, cellType StudioCellType, content string) (*StudioNotebookCell, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return nil, fmt.Errorf("notebook not found: %s", notebookID)
	}
	if len(nb.Cells) >= s.config.MaxCellsPerNotebook {
		return nil, fmt.Errorf("maximum cells per notebook (%d) reached", s.config.MaxCellsPerNotebook)
	}

	now := time.Now()
	cell := StudioNotebookCell{
		ID:        fmt.Sprintf("cell_%d", now.UnixNano()),
		Type:      cellType,
		Content:   content,
		Position:  len(nb.Cells),
		CreatedAt: now,
		UpdatedAt: now,
	}
	nb.Cells = append(nb.Cells, cell)
	nb.UpdatedAt = now
	return &cell, nil
}

// UpdateCell updates the content of a cell.
func (s *ChronicleStudio) UpdateCell(notebookID, cellID, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return fmt.Errorf("notebook not found: %s", notebookID)
	}
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			nb.Cells[i].Content = content
			nb.Cells[i].UpdatedAt = time.Now()
			nb.UpdatedAt = nb.Cells[i].UpdatedAt
			return nil
		}
	}
	return fmt.Errorf("cell not found: %s", cellID)
}

// ExecuteCell executes a cell and returns its output.
func (s *ChronicleStudio) ExecuteCell(notebookID, cellID string) (*StudioCellOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return nil, fmt.Errorf("notebook not found: %s", notebookID)
	}

	var cell *StudioNotebookCell
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			cell = &nb.Cells[i]
			break
		}
	}
	if cell == nil {
		return nil, fmt.Errorf("cell not found: %s", cellID)
	}

	start := time.Now()
	var output StudioCellOutput

	switch cell.Type {
	case StudioCellQuery:
		output = StudioCellOutput{
			Data:       cell.Content,
			ExecutedAt: start,
			DurationMs: time.Since(start).Milliseconds(),
			RowCount:   0,
		}
		s.queriesExecuted++
		s.totalQueryDurationMs += output.DurationMs
	case StudioCellMarkdown, StudioCellVisualization, StudioCellCode:
		output = StudioCellOutput{
			Data:       cell.Content,
			ExecutedAt: start,
			DurationMs: time.Since(start).Milliseconds(),
		}
	default:
		return nil, fmt.Errorf("unsupported cell type: %s", cell.Type)
	}

	cell.Output = &output
	nb.UpdatedAt = time.Now()
	return &output, nil
}

// DeleteCell removes a cell from a notebook.
func (s *ChronicleStudio) DeleteCell(notebookID, cellID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb, ok := s.notebooks[notebookID]
	if !ok {
		return fmt.Errorf("notebook not found: %s", notebookID)
	}
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			nb.Cells = append(nb.Cells[:i], nb.Cells[i+1:]...)
			for j := i; j < len(nb.Cells); j++ {
				nb.Cells[j].Position = j
			}
			nb.UpdatedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("cell not found: %s", cellID)
}

// GetQuerySuggestions returns suggestions for a partial query.
func (s *ChronicleStudio) GetQuerySuggestions(partial string) []StudioQuerySuggestion {
	suggestions := []StudioQuerySuggestion{
		{Query: "SELECT * FROM metrics WHERE time > now() - 1h", Description: "Recent metrics from the last hour", Category: "time_range", Confidence: 0.9},
		{Query: "SELECT mean(value) FROM metrics GROUP BY time(5m)", Description: "Average values in 5-minute buckets", Category: "aggregation", Confidence: 0.85},
		{Query: "SELECT max(value), min(value) FROM metrics", Description: "Min and max values", Category: "aggregation", Confidence: 0.8},
		{Query: "SELECT count(*) FROM metrics GROUP BY tag", Description: "Count by tag", Category: "grouping", Confidence: 0.75},
		{Query: "SELECT derivative(value) FROM metrics", Description: "Rate of change", Category: "analysis", Confidence: 0.7},
		{Query: "SELECT percentile(value, 95) FROM metrics", Description: "95th percentile", Category: "analysis", Confidence: 0.7},
		{Query: "SELECT * FROM metrics WHERE value > threshold", Description: "Filter by threshold", Category: "filtering", Confidence: 0.65},
		{Query: "SELECT moving_average(value, 10) FROM metrics", Description: "Moving average over 10 points", Category: "analysis", Confidence: 0.6},
	}

	if partial == "" {
		return suggestions
	}

	lower := strings.ToLower(partial)
	var filtered []StudioQuerySuggestion
	for _, sg := range suggestions {
		if strings.Contains(strings.ToLower(sg.Query), lower) || strings.Contains(strings.ToLower(sg.Description), lower) {
			filtered = append(filtered, sg)
		}
	}
	return filtered
}

// CreateVisualization validates and returns a visualization spec.
func (s *ChronicleStudio) CreateVisualization(spec VisualizationSpec) (*VisualizationSpec, error) {
	if spec.Title == "" {
		return nil, fmt.Errorf("visualization title is required")
	}
	if spec.Type == "" {
		return nil, fmt.Errorf("visualization type is required")
	}
	return &spec, nil
}
