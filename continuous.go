package chronicle

import (
	"sync"
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

type cqRunner struct {
	db   *DB
	cq   ContinuousQuery
	mu   sync.Mutex
	last time.Time
	stop chan struct{}
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
			_ = r.execute()
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
	return r.db.WriteBatch(points)
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
