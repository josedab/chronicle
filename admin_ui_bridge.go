package chronicle

import "github.com/chronicle-db/chronicle/internal/adminui"

// Re-export types
type AdminUI = adminui.AdminUI
type AdminConfig = adminui.AdminConfig

// dbAdminUIAdapter wraps *DB to satisfy adminui.AdminDB
type dbAdminUIAdapter struct {
	db *DB
}

func (a *dbAdminUIAdapter) Metrics() []string { return a.db.Metrics() }

func (a *dbAdminUIAdapter) Execute(q *adminui.Query) (*adminui.Result, error) {
	// Convert adminui.Query to chronicle.Query
	cq := &Query{
		Metric: q.Metric,
		Tags:   q.Tags,
		Start:  q.Start,
		End:    q.End,
		Limit:  q.Limit,
	}
	for _, tf := range q.TagFilters {
		cq.TagFilters = append(cq.TagFilters, TagFilter{Key: tf.Key, Values: tf.Values})
	}
	result, err := a.db.Execute(cq)
	if err != nil {
		return nil, err
	}
	// Convert result
	r := &adminui.Result{}
	for _, p := range result.Points {
		r.Points = append(r.Points, adminui.Point{
			Metric:    p.Metric,
			Tags:      p.Tags,
			Value:     p.Value,
			Timestamp: p.Timestamp,
		})
	}
	return r, nil
}

func (a *dbAdminUIAdapter) WriteBatch(points []adminui.Point) error {
	cpoints := make([]Point, len(points))
	for i, p := range points {
		cpoints[i] = Point{Metric: p.Metric, Tags: p.Tags, Value: p.Value, Timestamp: p.Timestamp}
	}
	return a.db.WriteBatch(cpoints)
}

func (a *dbAdminUIAdapter) Flush() error { return a.db.Flush() }

func (a *dbAdminUIAdapter) GetSchema(metric string) *adminui.MetricSchema {
	s := a.db.GetSchema(metric)
	if s == nil {
		return nil
	}
	ms := &adminui.MetricSchema{Name: s.Name, Description: s.Description}
	for _, t := range s.Tags {
		ms.Tags = append(ms.Tags, adminui.TagSchema{Name: t.Name})
	}
	return ms
}

func (a *dbAdminUIAdapter) Info() adminui.DBInfo {
	partCount := 0
	if a.db.index != nil {
		partCount = a.db.index.Count()
	}
	return adminui.DBInfo{
		Path:              a.db.config.Path,
		BufferSize:        a.db.config.Storage.BufferSize,
		PartitionDuration: a.db.config.Storage.PartitionDuration.String(),
		RetentionDuration: a.db.config.Retention.RetentionDuration.String(),
		WALSyncInterval:   a.db.config.WAL.SyncInterval.String(),
		PartitionCount:    partCount,
	}
}

func (a *dbAdminUIAdapter) IsClosed() bool { return a.db.closed }

func (a *dbAdminUIAdapter) ParseQuery(queryStr string) (*adminui.Query, error) {
	parser := &QueryParser{}
	q, err := parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}
	// Convert chronicle.Query to adminui.Query
	aq := &adminui.Query{
		Metric: q.Metric,
		Tags:   q.Tags,
		Start:  q.Start,
		End:    q.End,
		Limit:  q.Limit,
	}
	for _, tf := range q.TagFilters {
		aq.TagFilters = append(aq.TagFilters, adminui.TagFilter{Key: tf.Key, Values: tf.Values})
	}
	return aq, nil
}

// NewAdminUI creates an admin UI instance (bridge function).
func NewAdminUI(db *DB, config AdminConfig) *AdminUI {
	return adminui.NewAdminUI(&dbAdminUIAdapter{db: db}, config)
}
