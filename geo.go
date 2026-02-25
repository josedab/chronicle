package chronicle

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// GeoConfig configures geographic/spatial support.
type GeoConfig struct {
	// Enabled enables geographic support.
	Enabled bool

	// IndexResolution is the geohash precision for indexing.
	IndexResolution int

	// MaxGeofences is the maximum number of active geofences.
	MaxGeofences int

	// GeofenceCheckInterval is how often to check geofence triggers.
	GeofenceCheckInterval time.Duration

	// EnableSpatialIndex enables the R-tree spatial index.
	EnableSpatialIndex bool

	// MaxPointsPerQuery limits points returned per spatial query.
	MaxPointsPerQuery int
}

// DefaultGeoConfig returns default geographic configuration.
func DefaultGeoConfig() GeoConfig {
	return GeoConfig{
		Enabled:               true,
		IndexResolution:       7, // ~153m precision
		MaxGeofences:          1000,
		GeofenceCheckInterval: time.Second,
		EnableSpatialIndex:    true,
		MaxPointsPerQuery:     10000,
	}
}

// GeoPoint represents a geographic point with timestamp.
type GeoPoint struct {
	Latitude   float64        `json:"latitude"`
	Longitude  float64        `json:"longitude"`
	Altitude   float64        `json:"altitude,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	Accuracy   float64        `json:"accuracy,omitempty"`
	Speed      float64        `json:"speed,omitempty"`
	Heading    float64        `json:"heading,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// BoundingBox represents a geographic bounding box.
type BoundingBox struct {
	MinLat float64 `json:"min_lat"`
	MaxLat float64 `json:"max_lat"`
	MinLon float64 `json:"min_lon"`
	MaxLon float64 `json:"max_lon"`
}

// Contains checks if a point is within the bounding box.
func (bb BoundingBox) Contains(lat, lon float64) bool {
	return lat >= bb.MinLat && lat <= bb.MaxLat &&
		lon >= bb.MinLon && lon <= bb.MaxLon
}

// Circle represents a geographic circle.
type Circle struct {
	Center GeoPoint `json:"center"`
	Radius float64  `json:"radius"` // meters
}

// Contains checks if a point is within the circle.
func (c Circle) Contains(lat, lon float64) bool {
	distance := haversineDistance(c.Center.Latitude, c.Center.Longitude, lat, lon)
	return distance <= c.Radius
}

// Polygon represents a geographic polygon.
type Polygon struct {
	Points []GeoPoint `json:"points"`
}

// Contains checks if a point is within the polygon using ray casting.
func (p Polygon) Contains(lat, lon float64) bool {
	if len(p.Points) < 3 {
		return false
	}

	inside := false
	j := len(p.Points) - 1

	for i := 0; i < len(p.Points); i++ {
		yi := p.Points[i].Latitude
		xi := p.Points[i].Longitude
		yj := p.Points[j].Latitude
		xj := p.Points[j].Longitude

		if ((yi > lat) != (yj > lat)) &&
			(lon < (xj-xi)*(lat-yi)/(yj-yi)+xi) {
			inside = !inside
		}
		j = i
	}

	return inside
}

// Geofence represents a geographic fence with triggers.
type Geofence struct {
	ID          string                    `json:"id"`
	Name        string                    `json:"name"`
	Type        GeofenceType              `json:"type"`
	BoundingBox *BoundingBox              `json:"bounding_box,omitempty"`
	Circle      *Circle                   `json:"circle,omitempty"`
	Polygon     *Polygon                  `json:"polygon,omitempty"`
	TriggerOn   []TriggerEvent            `json:"trigger_on"`
	Callback    func(event GeofenceEvent) `json:"-"`
	Enabled     bool                      `json:"enabled"`
	CreatedAt   time.Time                 `json:"created_at"`
}

// GeofenceType identifies the geofence shape type.
type GeofenceType int

const (
	GeofenceTypeBox GeofenceType = iota
	GeofenceTypeCircle
	GeofenceTypePolygon
)

// TriggerEvent identifies when a geofence should trigger.
type TriggerEvent int

const (
	TriggerOnEnter TriggerEvent = iota
	TriggerOnExit
	TriggerOnDwell
)

// GeofenceEvent represents a geofence trigger event.
type GeofenceEvent struct {
	GeofenceID string       `json:"geofence_id"`
	Trigger    TriggerEvent `json:"trigger"`
	Point      GeoPoint     `json:"point"`
	EntityID   string       `json:"entity_id"`
	Timestamp  time.Time    `json:"timestamp"`
}

// GeoEngine provides geographic/spatial capabilities.
type GeoEngine struct {
	config GeoConfig
	db     *DB

	// Spatial index (R-tree)
	index   *RTree
	indexMu sync.RWMutex

	// Geofences
	geofences   map[string]*Geofence
	geofencesMu sync.RWMutex

	// Entity tracking for geofence detection
	entityLocations   map[string]GeoPoint
	entityLocationsMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewGeoEngine creates a new geographic engine.
func NewGeoEngine(db *DB, config GeoConfig) *GeoEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &GeoEngine{
		config:          config,
		db:              db,
		index:           NewRTree(),
		geofences:       make(map[string]*Geofence),
		entityLocations: make(map[string]GeoPoint),
		ctx:             ctx,
		cancel:          cancel,
	}

	return engine
}

// Start starts the geo engine.
func (e *GeoEngine) Start() error {
	if e.config.GeofenceCheckInterval > 0 {
		e.wg.Add(1)
		go e.geofenceMonitorLoop()
	}
	return nil
}

// Stop stops the geo engine.
func (e *GeoEngine) Stop() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// AddPoint adds a geographic point to the index.
func (e *GeoEngine) AddPoint(entityID string, point GeoPoint) error {
	// Update entity location for geofence tracking
	e.entityLocationsMu.Lock()
	oldPoint, hadOld := e.entityLocations[entityID]
	e.entityLocations[entityID] = point
	e.entityLocationsMu.Unlock()

	// Add to spatial index
	if e.config.EnableSpatialIndex {
		e.indexMu.Lock()
		e.index.Insert(entityID, point)
		e.indexMu.Unlock()
	}

	// Check geofences
	e.checkGeofences(entityID, oldPoint, hadOld, point)

	return nil
}

// QueryBox queries points within a bounding box.
func (e *GeoEngine) QueryBox(bbox BoundingBox, start, end time.Time) ([]GeoPoint, error) {
	e.indexMu.RLock()
	defer e.indexMu.RUnlock()

	results := e.index.SearchBox(bbox)

	// Filter by time
	filtered := make([]GeoPoint, 0)
	for _, p := range results {
		if (start.IsZero() || p.Timestamp.After(start) || p.Timestamp.Equal(start)) &&
			(end.IsZero() || p.Timestamp.Before(end) || p.Timestamp.Equal(end)) {
			filtered = append(filtered, p)
		}
	}

	// Limit results
	if len(filtered) > e.config.MaxPointsPerQuery {
		filtered = filtered[:e.config.MaxPointsPerQuery]
	}

	return filtered, nil
}

// QueryRadius queries points within a radius from a center point.
func (e *GeoEngine) QueryRadius(lat, lon, radiusMeters float64, start, end time.Time) ([]GeoPoint, error) {
	// Calculate bounding box for initial filter
	latDelta := radiusMeters / 111320.0 // meters per degree latitude
	lonDelta := radiusMeters / (111320.0 * math.Cos(lat*math.Pi/180.0))

	bbox := BoundingBox{
		MinLat: lat - latDelta,
		MaxLat: lat + latDelta,
		MinLon: lon - lonDelta,
		MaxLon: lon + lonDelta,
	}

	// Get candidates from bounding box
	candidates, err := e.QueryBox(bbox, start, end)
	if err != nil {
		return nil, err
	}

	// Filter by actual distance
	results := make([]GeoPoint, 0)
	for _, p := range candidates {
		distance := haversineDistance(lat, lon, p.Latitude, p.Longitude)
		if distance <= radiusMeters {
			results = append(results, p)
		}
	}

	return results, nil
}

// QueryPolygon queries points within a polygon.
func (e *GeoEngine) QueryPolygon(polygon Polygon, start, end time.Time) ([]GeoPoint, error) {
	// Calculate bounding box
	minLat, maxLat := 90.0, -90.0
	minLon, maxLon := 180.0, -180.0

	for _, p := range polygon.Points {
		if p.Latitude < minLat {
			minLat = p.Latitude
		}
		if p.Latitude > maxLat {
			maxLat = p.Latitude
		}
		if p.Longitude < minLon {
			minLon = p.Longitude
		}
		if p.Longitude > maxLon {
			maxLon = p.Longitude
		}
	}

	bbox := BoundingBox{MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: maxLon}

	// Get candidates
	candidates, err := e.QueryBox(bbox, start, end)
	if err != nil {
		return nil, err
	}

	// Filter by polygon containment
	results := make([]GeoPoint, 0)
	for _, p := range candidates {
		if polygon.Contains(p.Latitude, p.Longitude) {
			results = append(results, p)
		}
	}

	return results, nil
}

// QueryNearestNeighbors finds the k nearest points to a location.
func (e *GeoEngine) QueryNearestNeighbors(lat, lon float64, k int) ([]GeoPoint, error) {
	e.indexMu.RLock()
	defer e.indexMu.RUnlock()

	return e.index.NearestNeighbors(lat, lon, k), nil
}

// GeoAggregate aggregates points by geohash cells.
func (e *GeoEngine) GeoAggregate(bbox BoundingBox, resolution int, start, end time.Time) (map[string]GeoAggregation, error) {
	points, err := e.QueryBox(bbox, start, end)
	if err != nil {
		return nil, err
	}

	// Group by geohash
	cells := make(map[string]*GeoAggregation)
	for _, p := range points {
		hash := encodeGeohash(p.Latitude, p.Longitude, resolution)

		if cells[hash] == nil {
			cells[hash] = &GeoAggregation{
				Geohash: hash,
			}
		}

		cells[hash].Count++
		cells[hash].SumLat += p.Latitude
		cells[hash].SumLon += p.Longitude
	}

	// Calculate averages
	result := make(map[string]GeoAggregation)
	for hash, agg := range cells {
		agg.CenterLat = agg.SumLat / float64(agg.Count)
		agg.CenterLon = agg.SumLon / float64(agg.Count)
		result[hash] = *agg
	}

	return result, nil
}

// GeoAggregation represents aggregated geo data.
type GeoAggregation struct {
	Geohash   string  `json:"geohash"`
	Count     int     `json:"count"`
	CenterLat float64 `json:"center_lat"`
	CenterLon float64 `json:"center_lon"`
	SumLat    float64 `json:"-"`
	SumLon    float64 `json:"-"`
}

// AddGeofence adds a geofence.
func (e *GeoEngine) AddGeofence(fence *Geofence) error {
	e.geofencesMu.Lock()
	defer e.geofencesMu.Unlock()

	if len(e.geofences) >= e.config.MaxGeofences {
		return errors.New("maximum geofences reached")
	}

	fence.CreatedAt = time.Now()
	fence.Enabled = true
	e.geofences[fence.ID] = fence
	return nil
}

// RemoveGeofence removes a geofence.
func (e *GeoEngine) RemoveGeofence(id string) error {
	e.geofencesMu.Lock()
	defer e.geofencesMu.Unlock()

	delete(e.geofences, id)
	return nil
}

// GetGeofence gets a geofence by ID.
func (e *GeoEngine) GetGeofence(id string) (*Geofence, bool) {
	e.geofencesMu.RLock()
	defer e.geofencesMu.RUnlock()

	fence, ok := e.geofences[id]
	return fence, ok
}

// ListGeofences lists all geofences.
func (e *GeoEngine) ListGeofences() []*Geofence {
	e.geofencesMu.RLock()
	defer e.geofencesMu.RUnlock()

	result := make([]*Geofence, 0, len(e.geofences))
	for _, fence := range e.geofences {
		result = append(result, fence)
	}
	return result
}

func (e *GeoEngine) checkGeofences(entityID string, oldPoint GeoPoint, hadOld bool, newPoint GeoPoint) {
	e.geofencesMu.RLock()
	defer e.geofencesMu.RUnlock()

	for _, fence := range e.geofences {
		if !fence.Enabled {
			continue
		}

		wasInside := false
		if hadOld {
			wasInside = e.isInsideGeofence(fence, oldPoint.Latitude, oldPoint.Longitude)
		}
		isInside := e.isInsideGeofence(fence, newPoint.Latitude, newPoint.Longitude)

		// Check enter trigger
		if !wasInside && isInside {
			for _, trigger := range fence.TriggerOn {
				if trigger == TriggerOnEnter && fence.Callback != nil {
					fence.Callback(GeofenceEvent{
						GeofenceID: fence.ID,
						Trigger:    TriggerOnEnter,
						Point:      newPoint,
						EntityID:   entityID,
						Timestamp:  time.Now(),
					})
				}
			}
		}

		// Check exit trigger
		if wasInside && !isInside {
			for _, trigger := range fence.TriggerOn {
				if trigger == TriggerOnExit && fence.Callback != nil {
					fence.Callback(GeofenceEvent{
						GeofenceID: fence.ID,
						Trigger:    TriggerOnExit,
						Point:      newPoint,
						EntityID:   entityID,
						Timestamp:  time.Now(),
					})
				}
			}
		}
	}
}

func (e *GeoEngine) isInsideGeofence(fence *Geofence, lat, lon float64) bool {
	switch fence.Type {
	case GeofenceTypeBox:
		if fence.BoundingBox != nil {
			return fence.BoundingBox.Contains(lat, lon)
		}
	case GeofenceTypeCircle:
		if fence.Circle != nil {
			return fence.Circle.Contains(lat, lon)
		}
	case GeofenceTypePolygon:
		if fence.Polygon != nil {
			return fence.Polygon.Contains(lat, lon)
		}
	}
	return false
}

func (e *GeoEngine) geofenceMonitorLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.GeofenceCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// Dwell detection handled here
			e.checkDwellTriggers()
		}
	}
}

func (e *GeoEngine) checkDwellTriggers() {
	e.geofencesMu.RLock()
	defer e.geofencesMu.RUnlock()

	e.entityLocationsMu.RLock()
	defer e.entityLocationsMu.RUnlock()

	for entityID, location := range e.entityLocations {
		for _, fence := range e.geofences {
			if !fence.Enabled {
				continue
			}

			// Check for dwell trigger
			for _, trigger := range fence.TriggerOn {
				if trigger != TriggerOnDwell {
					continue
				}

				if e.isInsideGeofence(fence, location.Latitude, location.Longitude) {
					if fence.Callback != nil {
						fence.Callback(GeofenceEvent{
							GeofenceID: fence.ID,
							Trigger:    TriggerOnDwell,
							Point:      location,
							EntityID:   entityID,
							Timestamp:  time.Now(),
						})
					}
				}
			}
		}
	}
}
