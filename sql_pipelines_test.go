package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestSQLPipelineCreate(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	def := SQLPipelineDefinition{
		ID:           "avg-cpu",
		Name:         "Average CPU Pipeline",
		TransformSQL: "SELECT avg(value) as value FROM source WHERE metric = 'cpu' GROUP BY time(1m)",
		DestMetric:   "cpu_avg_1m",
		WindowSize:   time.Minute,
	}

	if err := spe.CreatePipeline(def); err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}

	p := spe.GetPipeline("avg-cpu")
	if p == nil {
		t.Fatal("expected pipeline to exist")
	}
	if p.DestMetric != "cpu_avg_1m" {
		t.Errorf("expected dest metric cpu_avg_1m, got %s", p.DestMetric)
	}
}

func TestSQLPipelineCreateValidation(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	err := spe.CreatePipeline(SQLPipelineDefinition{TransformSQL: "SELECT 1"})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}

	err = spe.CreatePipeline(SQLPipelineDefinition{ID: "p1"})
	if err == nil {
		t.Fatal("expected error for empty SQL")
	}
}

func TestSQLPipelineMaxLimit(t *testing.T) {
	cfg := DefaultSQLPipelineConfig()
	cfg.MaxPipelines = 2
	spe := NewSQLPipelineEngine(nil, cfg)

	spe.CreatePipeline(SQLPipelineDefinition{ID: "p1", TransformSQL: "SELECT 1"})
	spe.CreatePipeline(SQLPipelineDefinition{ID: "p2", TransformSQL: "SELECT 2"})

	err := spe.CreatePipeline(SQLPipelineDefinition{ID: "p3", TransformSQL: "SELECT 3"})
	if err == nil {
		t.Fatal("expected error when exceeding max pipelines")
	}
}

func TestSQLPipelineLifecycle(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{
		ID: "p1", TransformSQL: "SELECT 1", WindowSize: time.Minute,
	})

	if err := spe.StartPipeline("p1"); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	metrics := spe.GetMetrics("p1")
	if metrics.State != SQLPipelineRunning {
		t.Errorf("expected running state, got %s", metrics.State)
	}

	if err := spe.StopPipeline("p1"); err != nil {
		t.Fatalf("StopPipeline failed: %v", err)
	}

	metrics = spe.GetMetrics("p1")
	if metrics.State != SQLPipelineStopped {
		t.Errorf("expected stopped state, got %s", metrics.State)
	}
}

func TestSQLPipelineExecuteOnce(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{
		ID: "p1", TransformSQL: "SELECT 1", WindowSize: time.Minute,
	})

	metrics, err := spe.ExecuteOnce(context.Background(), "p1")
	if err != nil {
		t.Fatalf("ExecuteOnce failed: %v", err)
	}
	if metrics.TotalRuns != 1 {
		t.Errorf("expected 1 run, got %d", metrics.TotalRuns)
	}
}

func TestSQLPipelineDelete(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{ID: "p1", TransformSQL: "SELECT 1"})
	spe.DeletePipeline("p1")

	if spe.GetPipeline("p1") != nil {
		t.Error("expected pipeline to be deleted")
	}
}

func TestSQLPipelineStats(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{ID: "p1", TransformSQL: "SELECT 1", WindowSize: time.Minute})
	spe.CreatePipeline(SQLPipelineDefinition{ID: "p2", TransformSQL: "SELECT 2", WindowSize: time.Minute})

	stats := spe.Stats()
	if stats.TotalPipelines != 2 {
		t.Errorf("expected 2 pipelines, got %d", stats.TotalPipelines)
	}
}

func TestSQLPipelineCheckpoint(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{ID: "p1", TransformSQL: "SELECT 1", WindowSize: time.Minute})
	spe.ExecuteOnce(context.Background(), "p1")

	cp := spe.GetCheckpoint("p1")
	if cp == nil {
		t.Fatal("expected checkpoint to exist")
	}
	if cp.CheckpointAt.IsZero() {
		t.Error("expected checkpoint time to be set")
	}
}

func TestSQLPipelinePause(t *testing.T) {
	spe := NewSQLPipelineEngine(nil, DefaultSQLPipelineConfig())

	spe.CreatePipeline(SQLPipelineDefinition{ID: "p1", TransformSQL: "SELECT 1", WindowSize: time.Minute})
	spe.StartPipeline("p1")

	if err := spe.PausePipeline("p1"); err != nil {
		t.Fatalf("PausePipeline failed: %v", err)
	}

	metrics := spe.GetMetrics("p1")
	if metrics.State != SQLPipelinePaused {
		t.Errorf("expected paused state, got %s", metrics.State)
	}
}
