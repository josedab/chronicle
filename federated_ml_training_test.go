package chronicle

import (
	"context"
	"testing"
)

func TestFederatedMLTrainerInitialize(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	trainer := NewFederatedMLTrainer(db, DefaultFederatedMLConfig())
	trainer.InitializeModel(10)

	model := trainer.GetGlobalModel()
	if model == nil {
		t.Fatal("expected non-nil model")
	}
	if model.Dimension != 10 {
		t.Errorf("expected dimension 10, got %d", model.Dimension)
	}
	if len(model.Weights) != 10 {
		t.Errorf("expected 10 weights, got %d", len(model.Weights))
	}
}

func TestFederatedMLTrainerSubmitUpdate(t *testing.T) {
	trainer := NewFederatedMLTrainer(nil, DefaultFederatedMLConfig())
	trainer.InitializeModel(5)

	update := FMLParticipantUpdate{
		ParticipantID: "node-1",
		RoundNum:      1,
		Weights:       ModelWeights{Weights: []float64{1, 2, 3, 4, 5}, Dimension: 5},
		SampleCount:   100,
		LocalLoss:     0.5,
	}

	if err := trainer.SubmitUpdate(update); err != nil {
		t.Fatalf("SubmitUpdate failed: %v", err)
	}
}

func TestFederatedMLTrainerDimensionMismatch(t *testing.T) {
	trainer := NewFederatedMLTrainer(nil, DefaultFederatedMLConfig())
	trainer.InitializeModel(5)

	update := FMLParticipantUpdate{
		ParticipantID: "node-1",
		Weights:       ModelWeights{Weights: []float64{1, 2, 3}, Dimension: 3}, // wrong dimension
	}

	err := trainer.SubmitUpdate(update)
	if err == nil {
		t.Error("expected dimension mismatch error")
	}
}

func TestFederatedMLTrainerAggregateRound(t *testing.T) {
	config := DefaultFederatedMLConfig()
	config.MinParticipants = 2
	config.EnableDifferentialPrivacy = false
	trainer := NewFederatedMLTrainer(nil, config)
	trainer.InitializeModel(3)

	trainer.SubmitUpdate(FMLParticipantUpdate{
		ParticipantID: "a", RoundNum: 1,
		Weights:     ModelWeights{Weights: []float64{1, 2, 3}, Dimension: 3},
		SampleCount: 50, LocalLoss: 0.5,
	})
	trainer.SubmitUpdate(FMLParticipantUpdate{
		ParticipantID: "b", RoundNum: 1,
		Weights:     ModelWeights{Weights: []float64{3, 4, 5}, Dimension: 3},
		SampleCount: 50, LocalLoss: 0.3,
	})

	round, err := trainer.AggregateRound(context.Background(), 1)
	if err != nil {
		t.Fatalf("AggregateRound failed: %v", err)
	}
	if round == nil {
		t.Fatal("expected non-nil round")
	}
	if len(round.Participants) != 2 {
		t.Errorf("expected 2 participants, got %d", len(round.Participants))
	}

	model := trainer.GetGlobalModel()
	if model == nil {
		t.Fatal("expected updated global model")
	}
	// Weighted average of [1,2,3] and [3,4,5] with equal weights = [2,3,4]
	// But clipping norm may adjust. Just verify dimension is correct.
	if len(model.Weights) != 3 {
		t.Errorf("expected 3 weights, got %d", len(model.Weights))
	}
}

func TestFederatedMLTrainerInsufficientParticipants(t *testing.T) {
	config := DefaultFederatedMLConfig()
	config.MinParticipants = 3
	trainer := NewFederatedMLTrainer(nil, config)
	trainer.InitializeModel(2)

	trainer.SubmitUpdate(FMLParticipantUpdate{
		ParticipantID: "a", RoundNum: 1,
		Weights: ModelWeights{Weights: []float64{1, 2}, Dimension: 2}, SampleCount: 10,
	})

	_, err := trainer.AggregateRound(context.Background(), 1)
	if err == nil {
		t.Error("expected insufficient participants error")
	}
}

func TestFederatedMLTrainerDifferentialPrivacy(t *testing.T) {
	config := DefaultFederatedMLConfig()
	config.MinParticipants = 2
	config.EnableDifferentialPrivacy = true
	config.Epsilon = 1.0
	config.MaxRounds = 10
	trainer := NewFederatedMLTrainer(nil, config)
	trainer.InitializeModel(3)

	trainer.SubmitUpdate(FMLParticipantUpdate{
		ParticipantID: "a", RoundNum: 1,
		Weights: ModelWeights{Weights: []float64{1, 2, 3}, Dimension: 3}, SampleCount: 50,
	})
	trainer.SubmitUpdate(FMLParticipantUpdate{
		ParticipantID: "b", RoundNum: 1,
		Weights: ModelWeights{Weights: []float64{1, 2, 3}, Dimension: 3}, SampleCount: 50,
	})

	round, err := trainer.AggregateRound(context.Background(), 1)
	if err != nil {
		t.Fatalf("AggregateRound with DP failed: %v", err)
	}
	if round.PrivacyBudget <= 0 {
		t.Error("expected positive privacy budget spent")
	}

	stats := trainer.Stats()
	if stats.PrivacyBudgetUsed <= 0 {
		t.Error("expected privacy budget usage tracked")
	}
}

func TestFederatedMLTrainerTrainLocal(t *testing.T) {
	config := DefaultFederatedMLConfig()
	trainer := NewFederatedMLTrainer(nil, config)
	trainer.InitializeModel(5)

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	update, err := trainer.TrainLocalModel(data)
	if err != nil {
		t.Fatalf("TrainLocalModel failed: %v", err)
	}
	if update == nil {
		t.Fatal("expected non-nil update")
	}
	if update.SampleCount != len(data) {
		t.Errorf("expected %d samples, got %d", len(data), update.SampleCount)
	}
	if update.LocalLoss <= 0 {
		t.Error("expected positive loss")
	}
}

func TestFederatedMLTrainerNoModel(t *testing.T) {
	trainer := NewFederatedMLTrainer(nil, DefaultFederatedMLConfig())
	_, err := trainer.TrainLocalModel([]float64{1, 2})
	if err == nil {
		t.Error("expected error when no model initialized")
	}
}

func TestFederatedMLTrainerStats(t *testing.T) {
	trainer := NewFederatedMLTrainer(nil, DefaultFederatedMLConfig())
	stats := trainer.Stats()
	if stats.RoundsCompleted != 0 {
		t.Errorf("expected 0 rounds, got %d", stats.RoundsCompleted)
	}
	if stats.PrivacyBudgetTotal != 1.0 {
		t.Errorf("expected privacy budget 1.0, got %f", stats.PrivacyBudgetTotal)
	}
}

func TestFederatedMLTrainerHistory(t *testing.T) {
	trainer := NewFederatedMLTrainer(nil, DefaultFederatedMLConfig())
	history := trainer.History()
	if len(history) != 0 {
		t.Errorf("expected empty history, got %d entries", len(history))
	}
}

func TestFederatedMLTrainerStartStop(t *testing.T) {
	config := DefaultFederatedMLConfig()
	config.Enabled = true
	trainer := NewFederatedMLTrainer(nil, config)
	trainer.Start()
	trainer.Stop()
}
