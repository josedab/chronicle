package chronicle

import (
	"sync"
	"sync/atomic"
	"time"
)

// ContinuousQuery defines a materialized view refresh rule.
type ContinuousQuery struct {
	Name         string
	SourceMetric string
	Tags         map[string]string
	Function     AggFunc
	Window       time.Duration
	GroupBy      []string
	Every        time.Duration
	TargetMetric string
}

// ContinuousQueryRunnerStats tracks execution statistics for a continuous query runner.
type ContinuousQueryRunnerStats struct {
	Executions    int64     `json:"executions"`
	Errors        int64     `json:"errors"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorAt   time.Time `json:"last_error_at,omitempty"`
	LastRunAt     time.Time `json:"last_run_at,omitempty"`
	PointsWritten int64    `json:"points_written"`
}

type cqRunner struct {
	db      *DB
	cq      ContinuousQuery
	mu      sync.Mutex
	last    time.Time
	stop    chan struct{}
	onError func(name string, err error)

	executions    atomic.Int64
	errors        atomic.Int64
	pointsWritten atomic.Int64
	lastError     atomic.Value // stores string
	lastErrorAt   atomic.Value // stores time.Time
	lastRunAt     atomic.Value // stores time.Time
}

// Stats returns execution statistics for this runner.
func (r *cqRunner) Stats() ContinuousQueryRunnerStats {
	s := ContinuousQueryRunnerStats{
		Executions:    r.executions.Load(),
		Errors:        r.errors.Load(),
		PointsWritten: r.pointsWritten.Load(),
	}
	if v := r.lastError.Load(); v != nil {
		s.LastError = v.(string)
	}
	if v := r.lastErrorAt.Load(); v != nil {
		s.LastErrorAt = v.(time.Time)
	}
	if v := r.lastRunAt.Load(); v != nil {
		s.LastRunAt = v.(time.Time)
	}
	return s
}

func (r *cqRunner) run() {
	interval := r.cq.Every
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			if err := r.execute(); err != nil {
				r.errors.Add(1)
				r.lastError.Store(err.Error())
				r.lastErrorAt.Store(time.Now())
				if r.onError != nil {
					r.onError(r.cq.Name, err)
				}
			}
		}
	}
}

func (r *cqRunner) execute() error {
	r.mu.Lock()
	start := r.last
	now := time.Now()
	if start.IsZero() {
		start = now.Add(-r.cq.Window * 2)
	}
	r.last = now
	r.mu.Unlock()

	r.executions.Add(1)
	r.lastRunAt.Store(now)

	q := &Query{
		Metric: r.cq.SourceMetric,
		Tags:   cloneTags(r.cq.Tags),
		Start:  start.UnixNano(),
		End:    now.UnixNano(),
		Aggregation: &Aggregation{
			Function: r.cq.Function,
			Window:   r.cq.Window,
		},
		GroupBy: r.cq.GroupBy,
	}

	result, err := r.db.Execute(q)
	if err != nil {
		return err
	}

	if len(result.Points) == 0 {
		return nil
	}

	points := make([]Point, len(result.Points))
	for i, p := range result.Points {
		p.Metric = r.cq.TargetMetric
		points[i] = p
	}
	if err := r.db.WriteBatch(points); err != nil {
		return err
	}
	r.pointsWritten.Add(int64(len(points)))
	return nil
}

func (db *DB) startContinuousQueries() {
	if len(db.config.ContinuousQueries) == 0 {
		return
	}
	db.lifecycle.mu.Lock()
	if db.lifecycle.cqRunners != nil {
		db.lifecycle.mu.Unlock()
		return
	}
	runners := make([]*cqRunner, 0, len(db.config.ContinuousQueries))
	for _, cq := range db.config.ContinuousQueries {
		if cq.TargetMetric == "" {
			continue
		}
		runner := &cqRunner{db: db, cq: cq, stop: make(chan struct{})}
		runners = append(runners, runner)
		go runner.run()
	}
	db.lifecycle.cqRunners = runners
	db.lifecycle.mu.Unlock()
}

func (db *DB) stopContinuousQueries() {
	db.lifecycle.mu.Lock()
	runners := db.lifecycle.cqRunners
	db.lifecycle.cqRunners = nil
	db.lifecycle.mu.Unlock()

	for _, runner := range runners {
		close(runner.stop)
	}
}

// ContinuousQueryStats returns execution statistics for all active continuous queries.
func (db *DB) ContinuousQueryStats() map[string]ContinuousQueryRunnerStats {
	db.lifecycle.mu.Lock()
	runners := db.lifecycle.cqRunners
	db.lifecycle.mu.Unlock()

	stats := make(map[string]ContinuousQueryRunnerStats, len(runners))
	for _, r := range runners {
		name := r.cq.Name
		if name == "" {
			name = r.cq.SourceMetric + " → " + r.cq.TargetMetric
		}
		stats[name] = r.Stats()
	}
	return stats
}

func (db *DB) tryMaterialized(q *Query) *Query {
	if q == nil || q.Aggregation == nil {
		return q
	}
	for _, cq := range db.config.ContinuousQueries {
		if cq.TargetMetric == "" {
			continue
		}
		if cq.SourceMetric != "" && cq.SourceMetric != q.Metric {
			continue
		}
		if cq.Function != q.Aggregation.Function {
			continue
		}
		if cq.Window != q.Aggregation.Window {
			continue
		}
		if !equalStringSlice(cq.GroupBy, q.GroupBy) {
			continue
		}
		if !matchesTags(q.Tags, cq.Tags) {
			continue
		}

		materialized := *q
		materialized.Metric = cq.TargetMetric
		materialized.Aggregation = nil
		return &materialized
	}
	return q
}
