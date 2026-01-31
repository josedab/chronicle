package chronicle

import (
	"context"
	"errors"
	"math"
	"sort"
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
	Latitude   float64            `json:"latitude"`
	Longitude  float64            `json:"longitude"`
	Altitude   float64            `json:"altitude,omitempty"`
	Timestamp  time.Time          `json:"timestamp"`
	Accuracy   float64            `json:"accuracy,omitempty"`
	Speed      float64            `json:"speed,omitempty"`
	Heading    float64            `json:"heading,omitempty"`
	Properties map[string]any     `json:"properties,omitempty"`
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
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Type        GeofenceType   `json:"type"`
	BoundingBox *BoundingBox   `json:"bounding_box,omitempty"`
	Circle      *Circle        `json:"circle,omitempty"`
	Polygon     *Polygon       `json:"polygon,omitempty"`
	TriggerOn   []TriggerEvent `json:"trigger_on"`
	Callback    func(event GeofenceEvent) `json:"-"`
	Enabled     bool           `json:"enabled"`
	CreatedAt   time.Time      `json:"created_at"`
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

// CalculateDistance calculates the distance between two points in meters.
func (e *GeoEngine) CalculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	return haversineDistance(lat1, lon1, lat2, lon2)
}

// CalculateBearing calculates the bearing from point 1 to point 2.
func (e *GeoEngine) CalculateBearing(lat1, lon1, lat2, lon2 float64) float64 {
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lonDiff := (lon2 - lon1) * math.Pi / 180

	y := math.Sin(lonDiff) * math.Cos(lat2Rad)
	x := math.Cos(lat1Rad)*math.Sin(lat2Rad) -
		math.Sin(lat1Rad)*math.Cos(lat2Rad)*math.Cos(lonDiff)

	bearing := math.Atan2(y, x) * 180 / math.Pi
	return math.Mod(bearing+360, 360)
}

// DestinationPoint calculates the destination point given start, bearing, and distance.
func (e *GeoEngine) DestinationPoint(lat, lon, bearing, distance float64) (float64, float64) {
	R := 6371000.0 // Earth radius in meters
	d := distance / R

	latRad := lat * math.Pi / 180
	lonRad := lon * math.Pi / 180
	bearingRad := bearing * math.Pi / 180

	lat2 := math.Asin(math.Sin(latRad)*math.Cos(d) +
		math.Cos(latRad)*math.Sin(d)*math.Cos(bearingRad))
	lon2 := lonRad + math.Atan2(
		math.Sin(bearingRad)*math.Sin(d)*math.Cos(latRad),
		math.Cos(d)-math.Sin(latRad)*math.Sin(lat2))

	return lat2 * 180 / math.Pi, lon2 * 180 / math.Pi
}

// GeohashEncode encodes a lat/lon to geohash.
func (e *GeoEngine) GeohashEncode(lat, lon float64, precision int) string {
	return encodeGeohash(lat, lon, precision)
}

// GeohashDecode decodes a geohash to lat/lon.
func (e *GeoEngine) GeohashDecode(geohash string) (float64, float64, error) {
	return decodeGeohash(geohash)
}

// GeohashNeighbors returns all 8 neighbors of a geohash.
func (e *GeoEngine) GeohashNeighbors(geohash string) []string {
	lat, lon, err := decodeGeohash(geohash)
	if err != nil {
		return nil
	}

	precision := len(geohash)
	latErr, lonErr := geohashError(precision)

	neighbors := make([]string, 8)
	offsets := []struct{ dlat, dlon float64 }{
		{latErr * 2, 0},          // N
		{latErr * 2, lonErr * 2}, // NE
		{0, lonErr * 2},          // E
		{-latErr * 2, lonErr * 2}, // SE
		{-latErr * 2, 0},         // S
		{-latErr * 2, -lonErr * 2}, // SW
		{0, -lonErr * 2},         // W
		{latErr * 2, -lonErr * 2}, // NW
	}

	for i, off := range offsets {
		neighbors[i] = encodeGeohash(lat+off.dlat, lon+off.dlon, precision)
	}

	return neighbors
}

// Stats returns geographic engine statistics.
func (e *GeoEngine) Stats() GeoStats {
	e.indexMu.RLock()
	indexSize := e.index.Size()
	e.indexMu.RUnlock()

	e.geofencesMu.RLock()
	geofenceCount := len(e.geofences)
	e.geofencesMu.RUnlock()

	e.entityLocationsMu.RLock()
	entityCount := len(e.entityLocations)
	e.entityLocationsMu.RUnlock()

	return GeoStats{
		IndexSize:     indexSize,
		GeofenceCount: geofenceCount,
		EntityCount:   entityCount,
	}
}

// GeoStats contains geographic engine statistics.
type GeoStats struct {
	IndexSize     int `json:"index_size"`
	GeofenceCount int `json:"geofence_count"`
	EntityCount   int `json:"entity_count"`
}

// ========== R-Tree Implementation ==========

// RTree is a simple R-tree for spatial indexing.
type RTree struct {
	root *rtreeNode
	size int
	mu   sync.RWMutex
}

type rtreeNode struct {
	bbox     BoundingBox
	points   []rtreeEntry
	children []*rtreeNode
	isLeaf   bool
}

type rtreeEntry struct {
	entityID string
	point    GeoPoint
}

const maxEntriesPerNode = 16

// NewRTree creates a new R-tree.
func NewRTree() *RTree {
	return &RTree{
		root: &rtreeNode{
			isLeaf: true,
			points: make([]rtreeEntry, 0),
		},
	}
}

// Insert inserts a point into the R-tree.
func (rt *RTree) Insert(entityID string, point GeoPoint) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	entry := rtreeEntry{entityID: entityID, point: point}
	rt.insertIntoNode(rt.root, entry)
	rt.size++
}

func (rt *RTree) insertIntoNode(node *rtreeNode, entry rtreeEntry) {
	if node.isLeaf {
		node.points = append(node.points, entry)
		rt.updateBoundingBox(node)

		// Split if needed
		if len(node.points) > maxEntriesPerNode {
			rt.splitNode(node)
		}
	} else {
		// Find best child
		bestChild := rt.chooseBestChild(node, entry.point)
		rt.insertIntoNode(bestChild, entry)
		rt.updateBoundingBox(node)
	}
}

func (rt *RTree) chooseBestChild(node *rtreeNode, point GeoPoint) *rtreeNode {
	if len(node.children) == 0 {
		return node
	}

	best := node.children[0]
	bestEnlargement := rt.calculateEnlargement(best.bbox, point)

	for _, child := range node.children[1:] {
		enlargement := rt.calculateEnlargement(child.bbox, point)
		if enlargement < bestEnlargement {
			bestEnlargement = enlargement
			best = child
		}
	}

	return best
}

func (rt *RTree) calculateEnlargement(bbox BoundingBox, point GeoPoint) float64 {
	oldArea := (bbox.MaxLat - bbox.MinLat) * (bbox.MaxLon - bbox.MinLon)

	newBbox := bbox
	if point.Latitude < newBbox.MinLat {
		newBbox.MinLat = point.Latitude
	}
	if point.Latitude > newBbox.MaxLat {
		newBbox.MaxLat = point.Latitude
	}
	if point.Longitude < newBbox.MinLon {
		newBbox.MinLon = point.Longitude
	}
	if point.Longitude > newBbox.MaxLon {
		newBbox.MaxLon = point.Longitude
	}

	newArea := (newBbox.MaxLat - newBbox.MinLat) * (newBbox.MaxLon - newBbox.MinLon)
	return newArea - oldArea
}

func (rt *RTree) updateBoundingBox(node *rtreeNode) {
	if node.isLeaf {
		if len(node.points) == 0 {
			return
		}
		node.bbox.MinLat = node.points[0].point.Latitude
		node.bbox.MaxLat = node.points[0].point.Latitude
		node.bbox.MinLon = node.points[0].point.Longitude
		node.bbox.MaxLon = node.points[0].point.Longitude

		for _, entry := range node.points {
			if entry.point.Latitude < node.bbox.MinLat {
				node.bbox.MinLat = entry.point.Latitude
			}
			if entry.point.Latitude > node.bbox.MaxLat {
				node.bbox.MaxLat = entry.point.Latitude
			}
			if entry.point.Longitude < node.bbox.MinLon {
				node.bbox.MinLon = entry.point.Longitude
			}
			if entry.point.Longitude > node.bbox.MaxLon {
				node.bbox.MaxLon = entry.point.Longitude
			}
		}
	} else {
		if len(node.children) == 0 {
			return
		}
		node.bbox = node.children[0].bbox

		for _, child := range node.children[1:] {
			if child.bbox.MinLat < node.bbox.MinLat {
				node.bbox.MinLat = child.bbox.MinLat
			}
			if child.bbox.MaxLat > node.bbox.MaxLat {
				node.bbox.MaxLat = child.bbox.MaxLat
			}
			if child.bbox.MinLon < node.bbox.MinLon {
				node.bbox.MinLon = child.bbox.MinLon
			}
			if child.bbox.MaxLon > node.bbox.MaxLon {
				node.bbox.MaxLon = child.bbox.MaxLon
			}
		}
	}
}

func (rt *RTree) splitNode(node *rtreeNode) {
	// Simple split: divide points in half
	mid := len(node.points) / 2

	child1 := &rtreeNode{
		isLeaf: true,
		points: make([]rtreeEntry, len(node.points[:mid])),
	}
	copy(child1.points, node.points[:mid])
	rt.updateBoundingBox(child1)

	child2 := &rtreeNode{
		isLeaf: true,
		points: make([]rtreeEntry, len(node.points[mid:])),
	}
	copy(child2.points, node.points[mid:])
	rt.updateBoundingBox(child2)

	node.points = nil
	node.children = []*rtreeNode{child1, child2}
	node.isLeaf = false
	rt.updateBoundingBox(node)
}

// SearchBox searches for points within a bounding box.
func (rt *RTree) SearchBox(bbox BoundingBox) []GeoPoint {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var results []GeoPoint
	rt.searchBoxRecursive(rt.root, bbox, &results)
	return results
}

func (rt *RTree) searchBoxRecursive(node *rtreeNode, bbox BoundingBox, results *[]GeoPoint) {
	if node == nil {
		return
	}

	// Check if bounding boxes intersect
	if !rt.bboxIntersects(node.bbox, bbox) {
		return
	}

	if node.isLeaf {
		for _, entry := range node.points {
			if bbox.Contains(entry.point.Latitude, entry.point.Longitude) {
				*results = append(*results, entry.point)
			}
		}
	} else {
		for _, child := range node.children {
			rt.searchBoxRecursive(child, bbox, results)
		}
	}
}

func (rt *RTree) bboxIntersects(a, b BoundingBox) bool {
	return a.MinLat <= b.MaxLat && a.MaxLat >= b.MinLat &&
		a.MinLon <= b.MaxLon && a.MaxLon >= b.MinLon
}

// NearestNeighbors finds the k nearest neighbors to a point.
func (rt *RTree) NearestNeighbors(lat, lon float64, k int) []GeoPoint {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var allPoints []rtreeEntry
	rt.collectAllPoints(rt.root, &allPoints)

	// Sort by distance
	sort.Slice(allPoints, func(i, j int) bool {
		distI := haversineDistance(lat, lon, allPoints[i].point.Latitude, allPoints[i].point.Longitude)
		distJ := haversineDistance(lat, lon, allPoints[j].point.Latitude, allPoints[j].point.Longitude)
		return distI < distJ
	})

	// Return top k
	results := make([]GeoPoint, 0, k)
	for i := 0; i < k && i < len(allPoints); i++ {
		results = append(results, allPoints[i].point)
	}

	return results
}

func (rt *RTree) collectAllPoints(node *rtreeNode, results *[]rtreeEntry) {
	if node == nil {
		return
	}

	if node.isLeaf {
		*results = append(*results, node.points...)
	} else {
		for _, child := range node.children {
			rt.collectAllPoints(child, results)
		}
	}
}

// Size returns the number of points in the R-tree.
func (rt *RTree) Size() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.size
}

// ========== Utility Functions ==========

func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // Earth radius in meters

	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

func encodeGeohash(lat, lon float64, precision int) string {
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0

	var hash []byte
	var bit uint
	var ch byte

	for len(hash) < precision {
		if bit%2 == 0 {
			// Longitude
			mid := (minLon + maxLon) / 2
			if lon >= mid {
				ch |= 1 << (4 - (bit % 5))
				minLon = mid
			} else {
				maxLon = mid
			}
		} else {
			// Latitude
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch |= 1 << (4 - (bit % 5))
				minLat = mid
			} else {
				maxLat = mid
			}
		}

		bit++
		if bit%5 == 0 {
			hash = append(hash, base32[ch])
			ch = 0
		}
	}

	return string(hash)
}

func decodeGeohash(hash string) (float64, float64, error) {
	if len(hash) == 0 {
		return 0, 0, errors.New("empty geohash")
	}

	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0

	isLon := true
	for _, c := range hash {
		idx := -1
		for i, b := range base32 {
			if byte(b) == byte(c) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return 0, 0, errors.New("invalid geohash character")
		}

		for mask := 16; mask > 0; mask >>= 1 {
			if isLon {
				mid := (minLon + maxLon) / 2
				if idx&mask != 0 {
					minLon = mid
				} else {
					maxLon = mid
				}
			} else {
				mid := (minLat + maxLat) / 2
				if idx&mask != 0 {
					minLat = mid
				} else {
					maxLat = mid
				}
			}
			isLon = !isLon
		}
	}

	return (minLat + maxLat) / 2, (minLon + maxLon) / 2, nil
}

func geohashError(precision int) (float64, float64) {
	latErr := 90.0
	lonErr := 180.0

	for i := 0; i < precision*5; i++ {
		if i%2 == 0 {
			lonErr /= 2
		} else {
			latErr /= 2
		}
	}

	return latErr, lonErr
}

// GeoDB provides a database wrapper with geographic capabilities.
type GeoDB struct {
	*DB
	geo *GeoEngine
}

// NewGeoDB creates a database with geographic capabilities.
func NewGeoDB(db *DB, config GeoConfig) *GeoDB {
	return &GeoDB{
		DB:  db,
		geo: NewGeoEngine(db, config),
	}
}

// Geo returns the geographic engine.
func (db *GeoDB) Geo() *GeoEngine {
	return db.geo
}

// WriteGeoPoint writes a geographic point as a time-series data point.
func (db *GeoDB) WriteGeoPoint(measurement string, entityID string, point GeoPoint) error {
	// Add to geo index
	if err := db.geo.AddPoint(entityID, point); err != nil {
		return err
	}

	// Write as time-series point
	tags := map[string]string{
		"entity_id": entityID,
	}

	return db.DB.Write(Point{
		Metric:    measurement,
		Tags:      tags,
		Value:     point.Latitude, // Primary value is latitude
		Timestamp: point.Timestamp.UnixNano(),
	})
}

// Start starts the geographic database.
func (db *GeoDB) Start() error {
	return db.geo.Start()
}

// Stop stops the geographic database.
func (db *GeoDB) Stop() error {
	return db.geo.Stop()
}
