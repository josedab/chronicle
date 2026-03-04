// Bridge: anomaly_bridge.go
//
// This file bridges internal/anomaly/ into the public chronicle package.
// It re-exports types via type aliases so that callers use the top-level
// chronicle API while implementation stays private.
//
// Pattern: internal/anomaly/ (implementation) → anomaly_bridge.go (public API)
// Related files: anomaly_correlation.go, anomaly_explainability.go, anomaly_pipeline.go

package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/anomaly"
)

// AnomalyConfig holds configuration for the anomaly detection system.
type AnomalyConfig = anomaly.AnomalyConfig

// AnomalyModel identifies the ML model used for anomaly detection.
type AnomalyModel = anomaly.AnomalyModel

// AnomalyDetector detects anomalies in time-series data using configured models.
type AnomalyDetector = anomaly.AnomalyDetector

// AnomalyResult contains the outcome of an anomaly detection evaluation.
type AnomalyResult = anomaly.AnomalyResult

// AnomalyStats contains runtime statistics for the anomaly detection system.
type AnomalyStats = anomaly.AnomalyStats

// ContributingFactor identifies a factor that contributed to an anomaly.
type ContributingFactor = anomaly.ContributingFactor

// Prediction is a forecasted value produced by an anomaly model.
type Prediction = anomaly.Prediction

// IsolationForest implements the Isolation Forest anomaly detection algorithm.
type IsolationForest = anomaly.IsolationForest

// IsolationTree is a single tree within an Isolation Forest ensemble.
type IsolationTree = anomaly.IsolationTree

// IsolationNode is a node within an Isolation Tree.
type IsolationNode = anomaly.IsolationNode

// LSTMModel implements LSTM-based anomaly detection.
type LSTMModel = anomaly.LSTMModel

// Autoencoder implements autoencoder-based anomaly detection.
type Autoencoder = anomaly.Autoencoder

// TransformerModel implements transformer-based anomaly detection.
type TransformerModel = anomaly.TransformerModel

// StatisticalModel implements classical statistical anomaly detection.
type StatisticalModel = anomaly.StatisticalModel

// AnomalyType classifies the kind of anomaly detected (spike, dip, drift, etc.).
type AnomalyType = anomaly.AnomalyType

// AnomalySeverity indicates the severity level of a detected anomaly.
type AnomalySeverity = anomaly.AnomalySeverity

// ClassifiedAnomaly is an anomaly that has been classified by type and severity.
type ClassifiedAnomaly = anomaly.ClassifiedAnomaly

// PatternAnalysis contains the results of pattern analysis on detected anomalies.
type PatternAnalysis = anomaly.PatternAnalysis

// Remediation describes a recommended action to address a detected anomaly.
type Remediation = anomaly.Remediation

// AnomalyClassifier classifies detected anomalies by type and severity.
type AnomalyClassifier = anomaly.AnomalyClassifier

// AnomalyClassifierConfig holds configuration for the anomaly classifier.
type AnomalyClassifierConfig = anomaly.AnomalyClassifierConfig

// RemediationRule defines a rule for automated anomaly remediation.
type RemediationRule = anomaly.RemediationRule

// Re-export constants.
const (
	AnomalyModelIsolationForest = anomaly.AnomalyModelIsolationForest
	AnomalyModelLSTM            = anomaly.AnomalyModelLSTM
	AnomalyModelAutoencoder     = anomaly.AnomalyModelAutoencoder
	AnomalyModelTransformer     = anomaly.AnomalyModelTransformer
	AnomalyModelEnsemble        = anomaly.AnomalyModelEnsemble
	AnomalyModelStatistical     = anomaly.AnomalyModelStatistical
)

const (
	AnomalyTypeNormal            = anomaly.AnomalyTypeNormal
	AnomalyTypeSpike             = anomaly.AnomalyTypeSpike
	AnomalyTypeDip               = anomaly.AnomalyTypeDip
	AnomalyTypeDrift             = anomaly.AnomalyTypeDrift
	AnomalyTypeLevelShift        = anomaly.AnomalyTypeLevelShift
	AnomalyTypeSeasonalDeviation = anomaly.AnomalyTypeSeasonalDeviation
	AnomalyTypeTrendChange       = anomaly.AnomalyTypeTrendChange
	AnomalyTypeMissing           = anomaly.AnomalyTypeMissing
	AnomalyTypeOutlier           = anomaly.AnomalyTypeOutlier
	AnomalyTypeVarianceChange    = anomaly.AnomalyTypeVarianceChange
)

const (
	AnomalySeverityInfo      = anomaly.AnomalySeverityInfo
	AnomalySeverityWarning   = anomaly.AnomalySeverityWarning
	AnomalySeverityCritical  = anomaly.AnomalySeverityCritical
	AnomalySeverityEmergency = anomaly.AnomalySeverityEmergency
)

// Re-export constructor functions.
var (
	DefaultAnomalyConfig           = anomaly.DefaultAnomalyConfig
	NewIsolationForest             = anomaly.NewIsolationForest
	NewLSTMModel                   = anomaly.NewLSTMModel
	NewAutoencoder                 = anomaly.NewAutoencoder
	NewTransformerModel            = anomaly.NewTransformerModel
	NewStatisticalModel            = anomaly.NewStatisticalModel
	DefaultAnomalyClassifierConfig = anomaly.DefaultAnomalyClassifierConfig
	NewAnomalyClassifier           = anomaly.NewAnomalyClassifier
)

// NewAnomalyDetector creates a new anomaly detector. If ds is nil, the detector
// operates without a data source (no auto-retraining).
func NewAnomalyDetector(db *DB, config AnomalyConfig) *AnomalyDetector {
	var ds anomaly.DataSource
	if db != nil {
		ds = &dbAnomalyAdapter{db: db}
	}
	return anomaly.NewAnomalyDetector(ds, config)
}

// AnomalyDetectionDB wraps a DB with anomaly detection capabilities.
type AnomalyDetectionDB struct {
	*DB
	inner *anomaly.AnomalyDetectionDB
}

// NewAnomalyDetectionDB creates a new anomaly detection enabled database.
func NewAnomalyDetectionDB(db *DB, config AnomalyConfig) *AnomalyDetectionDB {
	adapter := &dbAnomalyAdapter{db: db}
	inner := anomaly.NewAnomalyDetectionDB(adapter, adapter, config)
	return &AnomalyDetectionDB{
		DB:    db,
		inner: inner,
	}
}

// Start starts anomaly detection.
func (adb *AnomalyDetectionDB) Start() { adb.inner.Start() }

// Stop stops anomaly detection.
func (adb *AnomalyDetectionDB) Stop() { adb.inner.Stop() }

// Detector returns the anomaly detector.
func (adb *AnomalyDetectionDB) Detector() *AnomalyDetector { return adb.inner.Detector() }

// WriteAndDetect writes a point and performs anomaly detection.
func (adb *AnomalyDetectionDB) WriteAndDetect(p Point, context []float64) (*AnomalyResult, error) {
	// Write the point via the root DB directly
	if err := adb.DB.Write(p); err != nil {
		return nil, err
	}
	// Detect anomaly
	return adb.inner.Detector().Detect(p.Value, context)
}

// dbAnomalyAdapter makes *DB satisfy anomaly.DataSource and anomaly.PointWriter.
type dbAnomalyAdapter struct {
	db *DB
}

func (a *dbAnomalyAdapter) Metrics() []string {
	return a.db.Metrics()
}

func (a *dbAnomalyAdapter) QueryRange(metric string, start, end int64) ([]anomaly.DataPoint, error) {
	result, err := a.db.Execute(&Query{Metric: metric, Start: start, End: end})
	if err != nil {
		return nil, err
	}
	points := make([]anomaly.DataPoint, len(result.Points))
	for i, p := range result.Points {
		points[i] = anomaly.DataPoint{Value: p.Value, Timestamp: p.Timestamp}
	}
	return points, nil
}

func (a *dbAnomalyAdapter) WritePoint(p anomaly.PointData) error {
	return a.db.Write(Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags})
}
