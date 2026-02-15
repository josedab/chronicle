package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NLDashboardConfig configures the natural language dashboard generator.
type NLDashboardConfig struct {
	// Enabled turns on NL dashboard generation
	Enabled bool

	// DefaultTimeRange for dashboards
	DefaultTimeRange string

	// DefaultRefreshInterval for panels
	DefaultRefreshInterval string

	// MaxPanelsPerRow in dashboards
	MaxPanelsPerRow int

	// EnableAutoComplete for metric suggestions
	EnableAutoComplete bool

	// EnableFeedback for learning from corrections
	EnableFeedback bool
}

// DefaultNLDashboardConfig returns default configuration.
func DefaultNLDashboardConfig() NLDashboardConfig {
	return NLDashboardConfig{
		Enabled:                true,
		DefaultTimeRange:       "now-1h",
		DefaultRefreshInterval: "30s",
		MaxPanelsPerRow:        3,
		EnableAutoComplete:     true,
		EnableFeedback:         true,
	}
}

// DashboardSpec represents the intermediate dashboard specification.
type DashboardSpec struct {
	ID          string       `json:"id"`
	Title       string       `json:"title"`
	Description string       `json:"description,omitempty"`
	Tags        []string     `json:"tags,omitempty"`
	TimeRange   *TimeRangeSpec `json:"time_range,omitempty"`
	Refresh     string       `json:"refresh,omitempty"`
	Panels      []*PanelSpec `json:"panels"`
	Variables   []*VariableSpec `json:"variables,omitempty"`
	Annotations []*AnnotationSpec `json:"annotations,omitempty"`
	Links       []*LinkSpec  `json:"links,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
	SourceNL    string       `json:"source_nl,omitempty"`
}

// TimeRangeSpec represents a time range.
type TimeRangeSpec struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// PanelSpec represents a dashboard panel.
type PanelSpec struct {
	ID          int          `json:"id"`
	Title       string       `json:"title"`
	Type        PanelType    `json:"type"`
	GridPos     *GridPos     `json:"gridPos"`
	Targets     []*TargetSpec `json:"targets"`
	Options     map[string]interface{} `json:"options,omitempty"`
	FieldConfig *FieldConfig `json:"fieldConfig,omitempty"`
	Thresholds  []*Threshold `json:"thresholds,omitempty"`
}

// PanelType defines the type of panel.
type PanelType string

const (
	PanelGraph      PanelType = "graph"
	PanelTimeseries PanelType = "timeseries"
	PanelStat       PanelType = "stat"
	PanelGauge      PanelType = "gauge"
	PanelTable      PanelType = "table"
	PanelHeatmap    PanelType = "heatmap"
	PanelPieChart   PanelType = "piechart"
	PanelText       PanelType = "text"
	PanelAlert      PanelType = "alertlist"
	PanelLogs       PanelType = "logs"
)

// GridPos represents panel position.
type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// TargetSpec represents a query target.
type TargetSpec struct {
	RefID      string            `json:"refId"`
	Metric     string            `json:"metric"`
	Tags       map[string]string `json:"tags,omitempty"`
	Aggregation string           `json:"aggregation,omitempty"`
	GroupBy    []string          `json:"groupBy,omitempty"`
	Alias      string            `json:"alias,omitempty"`
	RawQuery   string            `json:"rawQuery,omitempty"`
}

// FieldConfig for panel visualization.
type FieldConfig struct {
	Defaults  *FieldDefaults  `json:"defaults,omitempty"`
	Overrides []interface{}   `json:"overrides,omitempty"`
}

// FieldDefaults for panel fields.
type FieldDefaults struct {
	Unit       string     `json:"unit,omitempty"`
	Decimals   int        `json:"decimals,omitempty"`
	Min        *float64   `json:"min,omitempty"`
	Max        *float64   `json:"max,omitempty"`
	Color      *ColorConfig `json:"color,omitempty"`
}

// ColorConfig for visualization colors.
type ColorConfig struct {
	Mode       string `json:"mode"`
	FixedColor string `json:"fixedColor,omitempty"`
}

// Threshold for panel thresholds.
type Threshold struct {
	Value float64 `json:"value"`
	Color string  `json:"color"`
}

// VariableSpec represents a dashboard variable.
type VariableSpec struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Label   string   `json:"label,omitempty"`
	Query   string   `json:"query,omitempty"`
	Options []string `json:"options,omitempty"`
	Multi   bool     `json:"multi,omitempty"`
	Current string   `json:"current,omitempty"`
}

// AnnotationSpec represents a dashboard annotation.
type AnnotationSpec struct {
	Name    string `json:"name"`
	Enable  bool   `json:"enable"`
	Query   string `json:"query,omitempty"`
	IconColor string `json:"iconColor,omitempty"`
}

// LinkSpec represents a dashboard link.
type LinkSpec struct {
	Title string `json:"title"`
	URL   string `json:"url"`
	Type  string `json:"type"`
}

// NLDashboardEngine generates dashboards from natural language.
type NLDashboardEngine struct {
	db     *DB
	config NLDashboardConfig

	// Pattern matchers
	patterns []*NLPattern

	// Dashboard cache
	dashboards   map[string]*DashboardSpec
	dashboardsMu sync.RWMutex

	// Feedback for learning
	feedback   []DashboardFeedback
	feedbackMu sync.Mutex

	// Stats
	dashboardsGenerated int64
	panelsGenerated     int64
}

// NLPattern represents a natural language pattern.
type NLPattern struct {
	Pattern     *regexp.Regexp
	PanelType   PanelType
	Aggregation string
	Description string
	Priority    int
}

// DashboardFeedback for learning from corrections.
type DashboardFeedback struct {
	OriginalNL  string    `json:"original_nl"`
	CorrectedNL string    `json:"corrected_nl,omitempty"`
	Accepted    bool      `json:"accepted"`
	Timestamp   time.Time `json:"timestamp"`
}

// NewNLDashboardEngine creates a new NL dashboard engine.
func NewNLDashboardEngine(db *DB, config NLDashboardConfig) *NLDashboardEngine {
	engine := &NLDashboardEngine{
		db:         db,
		config:     config,
		patterns:   initPatterns(),
		dashboards: make(map[string]*DashboardSpec),
		feedback:   make([]DashboardFeedback, 0),
	}

	return engine
}

func initPatterns() []*NLPattern {
	return []*NLPattern{
		// Time series patterns
		{regexp.MustCompile(`(?i)show\s+(?:me\s+)?(.+?)\s+over\s+time`), PanelTimeseries, "", "Time series visualization", 10},
		{regexp.MustCompile(`(?i)graph\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Graph visualization", 9},
		{regexp.MustCompile(`(?i)trend\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Trend visualization", 9},
		{regexp.MustCompile(`(?i)chart\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Chart visualization", 8},
		
		// Stat patterns
		{regexp.MustCompile(`(?i)(?:current|latest|last)\s+(.+)`), PanelStat, "last", "Current value stat", 10},
		{regexp.MustCompile(`(?i)average\s+(?:of\s+)?(.+)`), PanelStat, "avg", "Average stat", 10},
		{regexp.MustCompile(`(?i)total\s+(?:of\s+)?(.+)`), PanelStat, "sum", "Total stat", 10},
		{regexp.MustCompile(`(?i)maximum\s+(?:of\s+)?(.+)`), PanelStat, "max", "Maximum stat", 10},
		{regexp.MustCompile(`(?i)minimum\s+(?:of\s+)?(.+)`), PanelStat, "min", "Minimum stat", 10},
		{regexp.MustCompile(`(?i)count\s+(?:of\s+)?(.+)`), PanelStat, "count", "Count stat", 10},
		
		// Gauge patterns
		{regexp.MustCompile(`(?i)gauge\s+(?:for\s+)?(.+)`), PanelGauge, "last", "Gauge visualization", 9},
		{regexp.MustCompile(`(?i)(.+)\s+gauge`), PanelGauge, "last", "Gauge visualization", 8},
		
		// Table patterns
		{regexp.MustCompile(`(?i)table\s+(?:of\s+)?(.+)`), PanelTable, "", "Table visualization", 9},
		{regexp.MustCompile(`(?i)list\s+(?:of\s+)?(.+)`), PanelTable, "", "Table visualization", 8},
		
		// Heatmap patterns
		{regexp.MustCompile(`(?i)heatmap\s+(?:of\s+)?(.+)`), PanelHeatmap, "", "Heatmap visualization", 9},
		{regexp.MustCompile(`(?i)distribution\s+(?:of\s+)?(.+)`), PanelHeatmap, "", "Distribution heatmap", 8},
		
		// Pie chart patterns
		{regexp.MustCompile(`(?i)(?:pie|breakdown)\s+(?:chart\s+)?(?:of\s+)?(.+)`), PanelPieChart, "", "Pie chart", 9},
	}
}

// GenerateDashboard generates a dashboard from natural language.
func (e *NLDashboardEngine) GenerateDashboard(ctx context.Context, description string) (*DashboardSpec, error) {
	// Parse the description
	parsed := e.parseDescription(description)

	// Create dashboard spec
	dashboard := &DashboardSpec{
		ID:          generateID(),
		Title:       parsed.Title,
		Description: description,
		Tags:        parsed.Tags,
		TimeRange: &TimeRangeSpec{
			From: e.config.DefaultTimeRange,
			To:   "now",
		},
		Refresh:   e.config.DefaultRefreshInterval,
		Panels:    make([]*PanelSpec, 0),
		Variables: parsed.Variables,
		CreatedAt: time.Now(),
		SourceNL:  description,
	}

	// Generate panels
	panelID := 1
	row := 0
	col := 0

	for _, panelDesc := range parsed.Panels {
		panel := e.createPanel(panelID, panelDesc, col, row)
		dashboard.Panels = append(dashboard.Panels, panel)

		panelID++
		col++
		if col >= e.config.MaxPanelsPerRow {
			col = 0
			row++
		}

		atomic.AddInt64(&e.panelsGenerated, 1)
	}

	// Cache dashboard
	e.dashboardsMu.Lock()
	e.dashboards[dashboard.ID] = dashboard
	e.dashboardsMu.Unlock()

	atomic.AddInt64(&e.dashboardsGenerated, 1)

	return dashboard, nil
}

// ParsedDescription contains parsed NL description.
type ParsedDescription struct {
	Title     string
	Tags      []string
	Panels    []*PanelDescription
	Variables []*VariableSpec
	TimeRange string
}

// PanelDescription describes a panel to create.
type PanelDescription struct {
	Title       string
	Type        PanelType
	Metrics     []string
	Tags        map[string]string
	Aggregation string
	GroupBy     []string
}

func (e *NLDashboardEngine) parseDescription(description string) *ParsedDescription {
	parsed := &ParsedDescription{
		Panels:    make([]*PanelDescription, 0),
		Variables: make([]*VariableSpec, 0),
	}

	// Extract title
	parsed.Title = e.extractTitle(description)

	// Extract tags
	parsed.Tags = e.extractTags(description)

	// Extract time range
	parsed.TimeRange = e.extractTimeRange(description)

	// Extract variables
	parsed.Variables = e.extractVariables(description)

	// Split into panel descriptions
	panelDescriptions := e.splitIntoPanels(description)

	for _, desc := range panelDescriptions {
		panelDesc := e.parsePanelDescription(desc)
		if panelDesc != nil {
			parsed.Panels = append(parsed.Panels, panelDesc)
		}
	}

	// If no panels extracted, create a default one
	if len(parsed.Panels) == 0 {
		metrics := e.extractMetrics(description)
		for _, metric := range metrics {
			parsed.Panels = append(parsed.Panels, &PanelDescription{
				Title:   metric,
				Type:    PanelTimeseries,
				Metrics: []string{metric},
			})
		}
	}

	return parsed
}

func (e *NLDashboardEngine) extractTitle(description string) string {
	// Look for explicit title
	titlePatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?dashboard\s+(?:called|named|titled)\s+"([^"]+)"`),
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?dashboard\s+(?:called|named|titled)\s+(\S+)`),
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?(.+?)\s+dashboard`),
		regexp.MustCompile(`(?i)dashboard\s+(?:for|showing|displaying)\s+(.+)`),
	}

	for _, pattern := range titlePatterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			return strings.TrimSpace(matches[1])
		}
	}

	// Generate title from metrics
	metrics := e.extractMetrics(description)
	if len(metrics) > 0 {
		return fmt.Sprintf("%s Dashboard", strings.Join(metrics[:min(len(metrics), 3)], " & "))
	}

	return "Generated Dashboard"
}

func (e *NLDashboardEngine) extractTags(description string) []string {
	tags := make([]string, 0)

	// Look for explicit tags
	tagPattern := regexp.MustCompile(`(?i)tags?:\s*\[?([^\]]+)\]?`)
	if matches := tagPattern.FindStringSubmatch(description); len(matches) > 1 {
		parts := strings.Split(matches[1], ",")
		for _, p := range parts {
			tag := strings.TrimSpace(p)
			if tag != "" {
				tags = append(tags, tag)
			}
		}
	}

	// Infer tags from content
	keywords := map[string]string{
		"cpu":       "system",
		"memory":    "system",
		"disk":      "system",
		"network":   "network",
		"http":      "web",
		"request":   "web",
		"database":  "database",
		"postgres":  "database",
		"mysql":     "database",
		"kubernetes": "kubernetes",
		"k8s":       "kubernetes",
		"docker":    "containers",
	}

	lowerDesc := strings.ToLower(description)
	for keyword, tag := range keywords {
		if strings.Contains(lowerDesc, keyword) {
			tags = append(tags, tag)
		}
	}

	return uniqueStrings(tags)
}

func (e *NLDashboardEngine) extractTimeRange(description string) string {
	timePatterns := map[*regexp.Regexp]string{
		regexp.MustCompile(`(?i)last\s+(\d+)\s*h(?:ours?)?`):   "now-%dh",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*m(?:inutes?)?`): "now-%dm",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*d(?:ays?)?`):    "now-%dd",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*w(?:eeks?)?`):   "now-%dw",
		regexp.MustCompile(`(?i)past\s+(\d+)\s*h(?:ours?)?`):   "now-%dh",
	}

	for pattern, format := range timePatterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			if num, err := strconv.Atoi(matches[1]); err == nil {
				return fmt.Sprintf(format, num)
			}
		}
	}

	return e.config.DefaultTimeRange
}

func (e *NLDashboardEngine) extractVariables(description string) []*VariableSpec {
	variables := make([]*VariableSpec, 0)

	// Look for variable patterns
	varPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)filter\s+by\s+(\w+)`),
		regexp.MustCompile(`(?i)variable\s+(?:for\s+)?(\w+)`),
		regexp.MustCompile(`(?i)dropdown\s+(?:for\s+)?(\w+)`),
	}

	for _, pattern := range varPatterns {
		matches := pattern.FindAllStringSubmatch(description, -1)
		for _, match := range matches {
			if len(match) > 1 {
				variables = append(variables, &VariableSpec{
					Name:  match[1],
					Type:  "query",
					Label: strings.Title(match[1]),
					Query: fmt.Sprintf("label_values(%s)", match[1]),
					Multi: true,
				})
			}
		}
	}

	return variables
}

func (e *NLDashboardEngine) splitIntoPanels(description string) []string {
	// First, split by bullet-point lines (e.g. "- item" or "* item")
	bulletPattern := regexp.MustCompile(`(?m)^\s*[-*]\s+`)
	if bulletPattern.MatchString(description) {
		lines := strings.Split(description, "\n")
		var bullets []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if len(trimmed) > 2 && (trimmed[0] == '-' || trimmed[0] == '*') {
				bullets = append(bullets, strings.TrimSpace(trimmed[1:]))
			}
		}
		if len(bullets) > 1 {
			return bullets
		}
	}

	// Split by common separators
	separators := []string{
		" and also ",
		" as well as ",
		" together with ",
		" alongside ",
		", and ",
		"; ",
	}

	parts := []string{description}
	for _, sep := range separators {
		newParts := make([]string, 0)
		for _, part := range parts {
			newParts = append(newParts, strings.Split(part, sep)...)
		}
		parts = newParts
	}

	return parts
}

func (e *NLDashboardEngine) parsePanelDescription(description string) *PanelDescription {
	// Match against patterns
	var bestMatch *NLPattern
	var bestSubmatch []string
	bestPriority := -1

	for _, pattern := range e.patterns {
		if matches := pattern.Pattern.FindStringSubmatch(description); len(matches) > 0 {
			if pattern.Priority > bestPriority {
				bestMatch = pattern
				bestSubmatch = matches
				bestPriority = pattern.Priority
			}
		}
	}

	if bestMatch == nil {
		// Try to extract metrics anyway
		metrics := e.extractMetrics(description)
		if len(metrics) == 0 {
			return nil
		}
		return &PanelDescription{
			Title:   metrics[0],
			Type:    PanelTimeseries,
			Metrics: metrics,
		}
	}

	// Extract metric from match
	metricPart := ""
	if len(bestSubmatch) > 1 {
		metricPart = bestSubmatch[1]
	} else {
		metricPart = description
	}

	metrics := e.extractMetrics(metricPart)
	tags := e.extractMetricTags(metricPart)
	groupBy := e.extractGroupBy(description)

	title := metricPart
	if len(metrics) > 0 {
		title = strings.Join(metrics, " & ")
	}

	return &PanelDescription{
		Title:       strings.Title(title),
		Type:        bestMatch.PanelType,
		Metrics:     metrics,
		Tags:        tags,
		Aggregation: bestMatch.Aggregation,
		GroupBy:     groupBy,
	}
}

func (e *NLDashboardEngine) extractMetrics(description string) []string {
	metrics := make([]string, 0)

	// Common metric patterns
	metricPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(cpu|memory|disk|network|http|request|error|latency|throughput|connections?|bytes|packets|load|temperature|humidity|pressure)`),
		regexp.MustCompile(`(?i)(\w+_\w+(?:_\w+)*)`),  // snake_case metrics
		regexp.MustCompile(`(?i)(\w+\.\w+(?:\.\w+)*)`), // dot.separated.metrics
	}

	for _, pattern := range metricPatterns {
		matches := pattern.FindAllString(description, -1)
		for _, match := range matches {
			normalized := strings.ToLower(match)
			if !nlContains(metrics, normalized) {
				metrics = append(metrics, normalized)
			}
		}
	}

	return metrics
}

func (e *NLDashboardEngine) extractMetricTags(description string) map[string]string {
	tags := make(map[string]string)

	// Look for tag specifications
	tagPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:where|for|with)\s+(\w+)\s*[=:]\s*"?(\w+)"?`),
		regexp.MustCompile(`(?i)(\w+)\s*[=:]\s*"([^"]+)"`),
	}

	for _, pattern := range tagPatterns {
		matches := pattern.FindAllStringSubmatch(description, -1)
		for _, match := range matches {
			if len(match) > 2 {
				tags[match[1]] = match[2]
			}
		}
	}

	return tags
}

func (e *NLDashboardEngine) extractGroupBy(description string) []string {
	groupBy := make([]string, 0)

	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:group|grouped|split|broken\s+down)\s+by\s+(\w+(?:\s*,\s*\w+)*)`),
		regexp.MustCompile(`(?i)per\s+(\w+)`),
		regexp.MustCompile(`(?i)by\s+(\w+)`),
	}

	for _, pattern := range patterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			parts := strings.Split(matches[1], ",")
			for _, p := range parts {
				groupBy = append(groupBy, strings.TrimSpace(p))
			}
		}
	}

	return groupBy
}

func (e *NLDashboardEngine) createPanel(id int, desc *PanelDescription, col, row int) *PanelSpec {
	// Calculate grid position
	panelWidth := 24 / e.config.MaxPanelsPerRow
	panelHeight := 8

	panel := &PanelSpec{
		ID:    id,
		Title: desc.Title,
		Type:  desc.Type,
		GridPos: &GridPos{
			H: panelHeight,
			W: panelWidth,
			X: col * panelWidth,
			Y: row * panelHeight,
		},
		Targets: make([]*TargetSpec, 0),
		Options: make(map[string]interface{}),
	}

	// Add targets for each metric
	refID := 'A'
	for _, metric := range desc.Metrics {
		target := &TargetSpec{
			RefID:       string(refID),
			Metric:      metric,
			Tags:        desc.Tags,
			Aggregation: desc.Aggregation,
			GroupBy:     desc.GroupBy,
		}
		panel.Targets = append(panel.Targets, target)
		refID++
	}

	// Set type-specific options
	switch desc.Type {
	case PanelStat:
		panel.Options["colorMode"] = "value"
		panel.Options["graphMode"] = "area"
		panel.Options["justifyMode"] = "auto"
		panel.Options["textMode"] = "auto"

	case PanelGauge:
		panel.Options["showThresholdLabels"] = false
		panel.Options["showThresholdMarkers"] = true
		panel.FieldConfig = &FieldConfig{
			Defaults: &FieldDefaults{
				Min: ptrFloat64(0),
				Max: ptrFloat64(100),
			},
		}
		panel.Thresholds = []*Threshold{
			{Value: 0, Color: "green"},
			{Value: 70, Color: "yellow"},
			{Value: 90, Color: "red"},
		}

	case PanelTimeseries:
		panel.Options["legend"] = map[string]interface{}{
			"displayMode": "list",
			"placement":   "bottom",
		}
		panel.Options["tooltip"] = map[string]interface{}{
			"mode": "single",
		}

	case PanelTable:
		panel.Options["showHeader"] = true

	case PanelPieChart:
		panel.Options["pieType"] = "pie"
		panel.Options["displayLabels"] = []string{"percent"}
	}

	return panel
}

// ToGrafanaJSON converts the dashboard spec to Grafana JSON format.
func (e *NLDashboardEngine) ToGrafanaJSON(spec *DashboardSpec) ([]byte, error) {
	// Build Grafana dashboard structure
	grafana := map[string]interface{}{
		"id":            nil,
		"uid":           spec.ID,
		"title":         spec.Title,
		"description":   spec.Description,
		"tags":          spec.Tags,
		"timezone":      "browser",
		"schemaVersion": 38,
		"version":       1,
		"refresh":       spec.Refresh,
		"time": map[string]string{
			"from": spec.TimeRange.From,
			"to":   spec.TimeRange.To,
		},
		"panels":      e.convertPanels(spec.Panels),
		"templating":  e.convertVariables(spec.Variables),
		"annotations": e.convertAnnotations(spec.Annotations),
		"links":       e.convertLinks(spec.Links),
	}

	return json.MarshalIndent(grafana, "", "  ")
}

func (e *NLDashboardEngine) convertPanels(panels []*PanelSpec) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(panels))

	for _, panel := range panels {
		p := map[string]interface{}{
			"id":        panel.ID,
			"title":     panel.Title,
			"type":      string(panel.Type),
			"gridPos":   panel.GridPos,
			"options":   panel.Options,
			"targets":   e.convertTargets(panel.Targets),
		}

		if panel.FieldConfig != nil {
			p["fieldConfig"] = panel.FieldConfig
		}

		if len(panel.Thresholds) > 0 {
			p["fieldConfig"] = map[string]interface{}{
				"defaults": map[string]interface{}{
					"thresholds": map[string]interface{}{
						"mode": "absolute",
						"steps": panel.Thresholds,
					},
				},
			}
		}

		result = append(result, p)
	}

	return result
}

func (e *NLDashboardEngine) convertTargets(targets []*TargetSpec) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(targets))

	for _, target := range targets {
		t := map[string]interface{}{
			"refId": target.RefID,
		}

		// Build Chronicle query
		query := target.Metric
		if target.Aggregation != "" {
			query = fmt.Sprintf("%s(%s)", target.Aggregation, target.Metric)
		}

		if len(target.Tags) > 0 {
			filters := make([]string, 0)
			for k, v := range target.Tags {
				filters = append(filters, fmt.Sprintf(`%s="%s"`, k, v))
			}
			query = fmt.Sprintf("%s{%s}", query, strings.Join(filters, ", "))
		}

		if len(target.GroupBy) > 0 {
			query = fmt.Sprintf("%s by (%s)", query, strings.Join(target.GroupBy, ", "))
		}

		t["expr"] = query
		t["legendFormat"] = target.Alias

		if target.RawQuery != "" {
			t["rawQuery"] = true
			t["query"] = target.RawQuery
		}

		result = append(result, t)
	}

	return result
}

func (e *NLDashboardEngine) convertVariables(variables []*VariableSpec) map[string]interface{} {
	list := make([]map[string]interface{}, 0)

	for _, v := range variables {
		list = append(list, map[string]interface{}{
			"name":    v.Name,
			"type":    v.Type,
			"label":   v.Label,
			"query":   v.Query,
			"multi":   v.Multi,
			"current": map[string]interface{}{"text": v.Current, "value": v.Current},
		})
	}

	return map[string]interface{}{"list": list}
}

func (e *NLDashboardEngine) convertAnnotations(annotations []*AnnotationSpec) map[string]interface{} {
	list := make([]map[string]interface{}, 0)

	for _, a := range annotations {
		list = append(list, map[string]interface{}{
			"name":      a.Name,
			"enable":    a.Enable,
			"query":     a.Query,
			"iconColor": a.IconColor,
		})
	}

	return map[string]interface{}{"list": list}
}

func (e *NLDashboardEngine) convertLinks(links []*LinkSpec) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	for _, l := range links {
		result = append(result, map[string]interface{}{
			"title": l.Title,
			"url":   l.URL,
			"type":  l.Type,
		})
	}

	return result
}

// GetDashboard returns a dashboard by ID.
func (e *NLDashboardEngine) GetDashboard(id string) (*DashboardSpec, error) {
	e.dashboardsMu.RLock()
	defer e.dashboardsMu.RUnlock()

	dashboard, ok := e.dashboards[id]
	if !ok {
		return nil, fmt.Errorf("dashboard not found: %s", id)
	}
	return dashboard, nil
}

// ListDashboards returns all dashboards.
func (e *NLDashboardEngine) ListDashboards() []*DashboardSpec {
	e.dashboardsMu.RLock()
	defer e.dashboardsMu.RUnlock()

	result := make([]*DashboardSpec, 0, len(e.dashboards))
	for _, d := range e.dashboards {
		result = append(result, d)
	}
	return result
}

// ProvideFeedback records feedback for a dashboard.
func (e *NLDashboardEngine) ProvideFeedback(dashboardID string, accepted bool, correction string) {
	if !e.config.EnableFeedback {
		return
	}

	e.dashboardsMu.RLock()
	dashboard, ok := e.dashboards[dashboardID]
	e.dashboardsMu.RUnlock()

	if !ok {
		return
	}

	e.feedbackMu.Lock()
	defer e.feedbackMu.Unlock()

	e.feedback = append(e.feedback, DashboardFeedback{
		OriginalNL:  dashboard.SourceNL,
		CorrectedNL: correction,
		Accepted:    accepted,
		Timestamp:   time.Now(),
	})
}

// Stats returns engine statistics.
func (e *NLDashboardEngine) Stats() NLDashboardStats {
	e.dashboardsMu.RLock()
	dashboardCount := len(e.dashboards)
	e.dashboardsMu.RUnlock()

	e.feedbackMu.Lock()
	feedbackCount := len(e.feedback)
	acceptedCount := 0
	for _, f := range e.feedback {
		if f.Accepted {
			acceptedCount++
		}
	}
	e.feedbackMu.Unlock()

	return NLDashboardStats{
		DashboardsGenerated: atomic.LoadInt64(&e.dashboardsGenerated),
		PanelsGenerated:     atomic.LoadInt64(&e.panelsGenerated),
		DashboardsCached:    dashboardCount,
		FeedbackReceived:    feedbackCount,
		AcceptanceRate:      float64(acceptedCount) / float64(max(feedbackCount, 1)),
	}
}

// NLDashboardStats contains engine statistics.
type NLDashboardStats struct {
	DashboardsGenerated int64   `json:"dashboards_generated"`
	PanelsGenerated     int64   `json:"panels_generated"`
	DashboardsCached    int     `json:"dashboards_cached"`
	FeedbackReceived    int     `json:"feedback_received"`
	AcceptanceRate      float64 `json:"acceptance_rate"`
}

// Helper functions

func ptrFloat64(f float64) *float64 {
	return &f
}

func nlContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func uniqueStrings(slice []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0)
	for _, s := range slice {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}
