//go:build !experimental

package chronicle

import "fmt"

// MLModel is a stub interface for machine learning models.
// Build with -tags experimental for the full implementation.
type MLModel interface {
	Name() string
	Type() MLModelType
	Train(data []float64) error
	Predict(input []float64) ([]float64, error)
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}

// MLModelType identifies the type of ML model.
type MLModelType int

const (
	MLModelTypeForecaster       MLModelType = iota
	MLModelTypeAnomalyDetector
	MLModelTypeClassifier
)

func (t MLModelType) String() string {
	switch t {
	case MLModelTypeForecaster:
		return "forecaster"
	case MLModelTypeAnomalyDetector:
		return "anomaly_detector"
	case MLModelTypeClassifier:
		return "classifier"
	default:
		return "unknown"
	}
}

// TinyMLEngine is a stub for the experimental TinyML engine.
type TinyMLEngine struct{}

// TinyMLConfig is a stub configuration.
type TinyMLConfig struct {
	Enabled bool
}

// DefaultTinyMLConfig returns a stub configuration.
func DefaultTinyMLConfig() TinyMLConfig {
	return TinyMLConfig{}
}

// NewTinyMLEngine returns a stub engine.
func NewTinyMLEngine(_ *DB, _ TinyMLConfig) *TinyMLEngine {
	return nil
}

// TinyMLAnomalyResult is a stub result type.
type TinyMLAnomalyResult struct{}

// TinyMLAnomalyPoint is a stub anomaly point type.
type TinyMLAnomalyPoint struct{}

// IsolationForestModel is a stub for the isolation forest model.
type IsolationForestModel struct {
	name string
}

// NewIsolationForestModel returns a stub model.
func NewIsolationForestModel(name string, _, _ int) *IsolationForestModel {
	return &IsolationForestModel{name: name}
}

func (m *IsolationForestModel) Name() string                            { return m.name }
func (m *IsolationForestModel) Type() MLModelType                       { return MLModelTypeAnomalyDetector }
func (m *IsolationForestModel) Train(_ []float64) error                 { return fmt.Errorf("tinyml: build with -tags experimental") }
func (m *IsolationForestModel) Predict(_ []float64) ([]float64, error)  { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *IsolationForestModel) Serialize() ([]byte, error)              { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *IsolationForestModel) Deserialize(_ []byte) error              { return fmt.Errorf("tinyml: build with -tags experimental") }

// SimpleExponentialSmoothingModel is a stub for the SES forecasting model.
type SimpleExponentialSmoothingModel struct{ name string }

// NewSimpleExponentialSmoothingModel returns a stub model.
func NewSimpleExponentialSmoothingModel(name string, _ float64) *SimpleExponentialSmoothingModel {
	return &SimpleExponentialSmoothingModel{name: name}
}

func (m *SimpleExponentialSmoothingModel) Name() string                            { return m.name }
func (m *SimpleExponentialSmoothingModel) Type() MLModelType                       { return MLModelTypeForecaster }
func (m *SimpleExponentialSmoothingModel) Train(_ []float64) error                 { return fmt.Errorf("tinyml: build with -tags experimental") }
func (m *SimpleExponentialSmoothingModel) Predict(_ []float64) ([]float64, error)  { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *SimpleExponentialSmoothingModel) Serialize() ([]byte, error)              { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *SimpleExponentialSmoothingModel) Deserialize(_ []byte) error              { return fmt.Errorf("tinyml: build with -tags experimental") }

// KMeansModel is a stub for the K-Means clustering model.
type KMeansModel struct{ name string }

// NewKMeansModel returns a stub model.
func NewKMeansModel(name string, _, _ int) *KMeansModel {
	return &KMeansModel{name: name}
}

func (m *KMeansModel) Name() string                            { return m.name }
func (m *KMeansModel) Type() MLModelType                       { return MLModelTypeClassifier }
func (m *KMeansModel) Train(_ []float64) error                 { return fmt.Errorf("tinyml: build with -tags experimental") }
func (m *KMeansModel) Predict(_ []float64) ([]float64, error)  { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *KMeansModel) Serialize() ([]byte, error)              { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *KMeansModel) Deserialize(_ []byte) error              { return fmt.Errorf("tinyml: build with -tags experimental") }

// StatisticalAnomalyDetector is a stub for the statistical anomaly detector.
type StatisticalAnomalyDetector struct{ name string }

// NewStatisticalAnomalyDetector returns a stub detector.
func NewStatisticalAnomalyDetector(name string, _ float64) *StatisticalAnomalyDetector {
	return &StatisticalAnomalyDetector{name: name}
}

func (m *StatisticalAnomalyDetector) Name() string                            { return m.name }
func (m *StatisticalAnomalyDetector) Type() MLModelType                       { return MLModelTypeAnomalyDetector }
func (m *StatisticalAnomalyDetector) Train(_ []float64) error                 { return fmt.Errorf("tinyml: build with -tags experimental") }
func (m *StatisticalAnomalyDetector) Predict(_ []float64) ([]float64, error)  { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *StatisticalAnomalyDetector) Serialize() ([]byte, error)              { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *StatisticalAnomalyDetector) Deserialize(_ []byte) error              { return fmt.Errorf("tinyml: build with -tags experimental") }

// MedianAbsoluteDeviationDetector is a stub for the MAD anomaly detector.
type MedianAbsoluteDeviationDetector struct{ name string }

// NewMADDetector returns a stub detector.
func NewMADDetector(name string, _ float64) *MedianAbsoluteDeviationDetector {
	return &MedianAbsoluteDeviationDetector{name: name}
}

func (m *MedianAbsoluteDeviationDetector) Name() string                            { return m.name }
func (m *MedianAbsoluteDeviationDetector) Type() MLModelType                       { return MLModelTypeAnomalyDetector }
func (m *MedianAbsoluteDeviationDetector) Train(_ []float64) error                 { return fmt.Errorf("tinyml: build with -tags experimental") }
func (m *MedianAbsoluteDeviationDetector) Predict(_ []float64) ([]float64, error)  { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *MedianAbsoluteDeviationDetector) Serialize() ([]byte, error)              { return nil, fmt.Errorf("tinyml: build with -tags experimental") }
func (m *MedianAbsoluteDeviationDetector) Deserialize(_ []byte) error              { return fmt.Errorf("tinyml: build with -tags experimental") }
