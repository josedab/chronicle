package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
	"time"
)

// ProfileType identifies the type of profile.
type ProfileType int

const (
	// ProfileTypeCPU is CPU time profile.
	ProfileTypeCPU ProfileType = iota
	// ProfileTypeHeap is heap memory profile.
	ProfileTypeHeap
	// ProfileTypeGoroutine is goroutine profile.
	ProfileTypeGoroutine
	// ProfileTypeMutex is mutex contention profile.
	ProfileTypeMutex
	// ProfileTypeBlock is blocking profile.
	ProfileTypeBlock
	// ProfileTypeAllocs is allocations profile.
	ProfileTypeAllocs
)

// Profile represents a pprof profile sample.
type Profile struct {
	ID          string
	Type        ProfileType
	Timestamp   int64
	Duration    time.Duration
	Labels      map[string]string
	Data        []byte // Compressed pprof data
	Compressed  bool
	SampleCount int64
}

// ProfileStore manages continuous profiling data.
type ProfileStore struct {
	db       *DB
	mu       sync.RWMutex
	profiles map[string]*profileSeries
	config   ProfileConfig
}

type profileSeries struct {
	profileType ProfileType
	labels      map[string]string
	profiles    []storedProfile
}

type storedProfile struct {
	id        string
	timestamp int64
	duration  time.Duration
	data      []byte
	samples   int64
}

// ProfileConfig configures profile storage.
type ProfileConfig struct {
	// MaxProfiles limits storage per series.
	MaxProfiles int

	// RetentionDuration for profiles.
	RetentionDuration time.Duration

	// CompressionEnabled enables gzip compression.
	CompressionEnabled bool

	// MaxProfileSize limits individual profile size.
	MaxProfileSize int64
}

// DefaultProfileConfig returns default configuration.
func DefaultProfileConfig() ProfileConfig {
	return ProfileConfig{
		MaxProfiles:        10000,
		RetentionDuration:  24 * time.Hour,
		CompressionEnabled: true,
		MaxProfileSize:     10 * 1024 * 1024, // 10MB
	}
}

// NewProfileStore creates a profile store.
func NewProfileStore(db *DB, config ProfileConfig) *ProfileStore {
	if config.MaxProfiles <= 0 {
		config.MaxProfiles = 10000
	}
	return &ProfileStore{
		db:       db,
		profiles: make(map[string]*profileSeries),
		config:   config,
	}
}

// Write stores a profile.
func (ps *ProfileStore) Write(p Profile) error {
	if len(p.Data) == 0 {
		return errors.New("profile data cannot be empty")
	}
	if int64(len(p.Data)) > ps.config.MaxProfileSize {
		return errors.New("profile exceeds maximum size")
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	key := profileKey(p.Type, p.Labels)
	series, exists := ps.profiles[key]
	if !exists {
		series = &profileSeries{
			profileType: p.Type,
			labels:      cloneTags(p.Labels),
			profiles:    make([]storedProfile, 0),
		}
		ps.profiles[key] = series
	}

	// Enforce retention
	ps.enforceRetention(series)

	// Enforce max profiles
	if len(series.profiles) >= ps.config.MaxProfiles {
		series.profiles = series.profiles[1:]
	}

	// Compress if enabled and not already compressed
	data := p.Data
	if ps.config.CompressionEnabled && !p.Compressed {
		var err error
		data, err = compressProfile(p.Data)
		if err != nil {
			return err
		}
	}

	ts := p.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	id := p.ID
	if id == "" {
		id = generateProfileID(p.Type, ts)
	}

	series.profiles = append(series.profiles, storedProfile{
		id:        id,
		timestamp: ts,
		duration:  p.Duration,
		data:      data,
		samples:   p.SampleCount,
	})

	return nil
}

// Query retrieves profiles within a time range.
func (ps *ProfileStore) Query(profileType ProfileType, labels map[string]string, start, end int64) ([]Profile, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var results []Profile

	for _, series := range ps.profiles {
		if series.profileType != profileType {
			continue
		}
		if !tagsMatch(series.labels, labels) {
			continue
		}

		for _, sp := range series.profiles {
			if sp.timestamp >= start && sp.timestamp <= end {
				data := sp.data
				compressed := ps.config.CompressionEnabled

				results = append(results, Profile{
					ID:          sp.id,
					Type:        series.profileType,
					Timestamp:   sp.timestamp,
					Duration:    sp.duration,
					Labels:      cloneTags(series.labels),
					Data:        data,
					Compressed:  compressed,
					SampleCount: sp.samples,
				})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}

// QueryByMetricCondition finds profiles when a metric exceeds a threshold.
// This enables correlation like "show profiles when CPU > 90%".
func (ps *ProfileStore) QueryByMetricCondition(
	metric string,
	threshold float64,
	profileType ProfileType,
	start, end int64,
) ([]ProfileCorrelation, error) {
	// Query metric points
	result, err := ps.db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, err
	}

	// Find points exceeding threshold
	var correlations []ProfileCorrelation

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, point := range result.Points {
		if point.Value < threshold {
			continue
		}

		// Find profiles near this timestamp (within 60s window)
		windowStart := point.Timestamp - int64(60*time.Second)
		windowEnd := point.Timestamp + int64(60*time.Second)

		for _, series := range ps.profiles {
			if series.profileType != profileType {
				continue
			}

			for _, sp := range series.profiles {
				if sp.timestamp >= windowStart && sp.timestamp <= windowEnd {
					correlations = append(correlations, ProfileCorrelation{
						MetricPoint:   point,
						Profile:       sp.id,
						ProfileType:   profileType,
						TimestampDiff: sp.timestamp - point.Timestamp,
					})
				}
			}
		}
	}

	return correlations, nil
}

// ProfileCorrelation links a metric point to a profile.
type ProfileCorrelation struct {
	MetricPoint   Point
	Profile       string
	ProfileType   ProfileType
	TimestampDiff int64
}

// GetProfile retrieves a specific profile by ID.
func (ps *ProfileStore) GetProfile(id string) (*Profile, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, series := range ps.profiles {
		for _, sp := range series.profiles {
			if sp.id == id {
				return &Profile{
					ID:          sp.id,
					Type:        series.profileType,
					Timestamp:   sp.timestamp,
					Duration:    sp.duration,
					Labels:      cloneTags(series.labels),
					Data:        sp.data,
					Compressed:  ps.config.CompressionEnabled,
					SampleCount: sp.samples,
				}, nil
			}
		}
	}

	return nil, errors.New("profile not found")
}

// Decompress decompresses profile data.
func (p *Profile) Decompress() ([]byte, error) {
	if !p.Compressed {
		return p.Data, nil
	}
	return decompressProfile(p.Data)
}

// enforceRetention removes old profiles.
func (ps *ProfileStore) enforceRetention(series *profileSeries) {
	if ps.config.RetentionDuration == 0 {
		return
	}

	cutoff := time.Now().Add(-ps.config.RetentionDuration).UnixNano()
	
	var kept []storedProfile
	for _, sp := range series.profiles {
		if sp.timestamp >= cutoff {
			kept = append(kept, sp)
		}
	}
	series.profiles = kept
}

// Stats returns profile store statistics.
func (ps *ProfileStore) Stats() ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	stats := ProfileStats{
		TypeCounts: make(map[ProfileType]int64),
	}

	for _, series := range ps.profiles {
		stats.SeriesCount++
		count := int64(len(series.profiles))
		stats.ProfileCount += count
		stats.TypeCounts[series.profileType] += count
		
		for _, sp := range series.profiles {
			stats.TotalBytes += int64(len(sp.data))
		}
	}

	return stats
}

// ProfileStats contains profile store statistics.
type ProfileStats struct {
	SeriesCount  int
	ProfileCount int64
	TotalBytes   int64
	TypeCounts   map[ProfileType]int64
}

// --- Helper functions ---

func profileKey(profileType ProfileType, labels map[string]string) string {
	key := string(rune('0' + int(profileType)))
	for k, v := range labels {
		key += ";" + k + "=" + v
	}
	return key
}

func generateProfileID(profileType ProfileType, timestamp int64) string {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(timestamp))
	binary.BigEndian.PutUint64(buf[8:], uint64(profileType))
	// Simple hex encoding
	const hex = "0123456789abcdef"
	result := make([]byte, 32)
	for i, b := range buf {
		result[i*2] = hex[b>>4]
		result[i*2+1] = hex[b&0x0f]
	}
	return string(result)
}

func compressProfile(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressProfile(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// ProfileTypeName returns the string name of a profile type.
func ProfileTypeName(pt ProfileType) string {
	switch pt {
	case ProfileTypeCPU:
		return "cpu"
	case ProfileTypeHeap:
		return "heap"
	case ProfileTypeGoroutine:
		return "goroutine"
	case ProfileTypeMutex:
		return "mutex"
	case ProfileTypeBlock:
		return "block"
	case ProfileTypeAllocs:
		return "allocs"
	default:
		return "unknown"
	}
}

// ParseProfileType parses a profile type string.
func ParseProfileType(s string) ProfileType {
	switch s {
	case "cpu":
		return ProfileTypeCPU
	case "heap":
		return ProfileTypeHeap
	case "goroutine":
		return ProfileTypeGoroutine
	case "mutex":
		return ProfileTypeMutex
	case "block":
		return ProfileTypeBlock
	case "allocs":
		return ProfileTypeAllocs
	default:
		return ProfileTypeCPU
	}
}
