package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// PluginCategory identifies the marketplace category of a plugin.
type PluginCategory string

const (
	CatCompression   PluginCategory = "compression"
	CatStorage       PluginCategory = "storage"
	CatQuery         PluginCategory = "query"
	CatIntegration   PluginCategory = "integration"
	CatAuth          PluginCategory = "auth"
	CatVisualization PluginCategory = "visualization"
	CatAnalytics     PluginCategory = "analytics"
)

// InstallStatus represents the state of an installed plugin.
type InstallStatus string

const (
	StatusInstalled       InstallStatus = "installed"
	StatusActive          InstallStatus = "active"
	StatusDisabled        InstallStatus = "disabled"
	StatusUpdateAvailable InstallStatus = "update_available"
	StatusFailed          InstallStatus = "failed"
)

// MarketplaceConfig configures the marketplace engine.
type MarketplaceConfig struct {
	MarketplaceURL     string        `json:"marketplace_url"`
	CacheTTL           time.Duration `json:"cache_ttl"`
	AutoUpdate         bool          `json:"auto_update"`
	VerifySignatures   bool          `json:"verify_signatures"`
	RevenueSharePct    float64       `json:"revenue_share_pct"`
	MaxPluginSize      int64         `json:"max_plugin_size"`
	EnableRatings      bool          `json:"enable_ratings"`
	EnableReviews      bool          `json:"enable_reviews"`
	SandboxEnabled     bool          `json:"sandbox_enabled"`
}

// DefaultMarketplaceConfig returns sensible defaults for the marketplace.
func DefaultMarketplaceConfig() MarketplaceConfig {
	return MarketplaceConfig{
		MarketplaceURL:   "https://marketplace.chronicle-db.io/api/v1",
		CacheTTL:         15 * time.Minute,
		AutoUpdate:       false,
		VerifySignatures: true,
		RevenueSharePct:  30.0,
		MaxPluginSize:    100 * 1024 * 1024, // 100MB
		EnableRatings:    true,
		EnableReviews:    true,
		SandboxEnabled:   true,
	}
}

// MktListingEntry is a marketplace listing with extended metadata.
type MktListingEntry struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	Author       string         `json:"author"`
	Version      string         `json:"version"`
	Category     PluginCategory `json:"category"`
	Downloads    int64          `json:"downloads"`
	Rating       float64        `json:"rating"`
	ReviewsCount int            `json:"reviews_count"`
	Verified     bool           `json:"verified"`
	Price        float64        `json:"price"` // 0 = free
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	Tags         []string       `json:"tags,omitempty"`
	Dependencies []string       `json:"dependencies,omitempty"`
	SizeBytes    int64          `json:"size_bytes"`
	Checksum     string         `json:"checksum"`
}

// PluginReview represents a user review for a marketplace plugin.
type PluginReview struct {
	ID           string    `json:"id"`
	PluginID     string    `json:"plugin_id"`
	User         string    `json:"user"`
	Rating       int       `json:"rating"` // 1-5
	Title        string    `json:"title"`
	Body         string    `json:"body"`
	CreatedAt    time.Time `json:"created_at"`
	HelpfulCount int       `json:"helpful_count"`
}

// PluginInstallation tracks a locally installed marketplace plugin.
type PluginInstallation struct {
	PluginID    string            `json:"plugin_id"`
	Version     string            `json:"version"`
	InstalledAt time.Time         `json:"installed_at"`
	Status      InstallStatus     `json:"status"`
	Config      map[string]string `json:"config,omitempty"`
}

// PluginUpdateInfo describes an available update for an installed plugin.
type PluginUpdateInfo struct {
	PluginID           string `json:"plugin_id"`
	CurrentVersion     string `json:"current_version"`
	LatestVersion      string `json:"latest_version"`
	Changelog          string `json:"changelog"`
	BreakingChanges    bool   `json:"breaking_changes"`
	AutoUpdateAvailable bool  `json:"auto_update_available"`
}

// MarketplaceSearchQuery defines search/filter criteria for marketplace listings.
type MarketplaceSearchQuery struct {
	Query    string         `json:"query,omitempty"`
	Category PluginCategory `json:"category,omitempty"`
	MinRating float64       `json:"min_rating,omitempty"`
	FreeOnly bool           `json:"free_only,omitempty"`
	SortBy   string         `json:"sort_by,omitempty"` // "downloads", "rating", "updated", "name"
	Page     int            `json:"page,omitempty"`
	PerPage  int            `json:"per_page,omitempty"`
}

// PublishRequest is the payload for publishing a new plugin to the marketplace.
type PublishRequest struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Category    PluginCategory `json:"category"`
	Version     string         `json:"version"`
	BinaryPath  string         `json:"binary_path"`
	Readme      string         `json:"readme"`
	License     string         `json:"license"`
	Tags        []string       `json:"tags,omitempty"`
	Price       float64        `json:"price"`
}

// MarketplaceStats aggregates marketplace-wide statistics.
type MarketplaceStats struct {
	TotalListings  int                    `json:"total_listings"`
	TotalInstalls  int64                  `json:"total_installs"`
	ActivePlugins  int                    `json:"active_plugins"`
	Categories     map[PluginCategory]int `json:"categories"`
	AvgRating      float64                `json:"avg_rating"`
	TotalRevenue   float64                `json:"total_revenue"`
}

// MarketplaceEngine provides a full marketplace and plugin ecosystem.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type MarketplaceEngine struct {
	db        *DB
	config    MarketplaceConfig
	listings  map[string]*MktListingEntry
	installed map[string]*PluginInstallation
	reviews   map[string][]*PluginReview
	mu        sync.RWMutex
}

// NewMarketplaceEngine creates a new marketplace engine.
func NewMarketplaceEngine(db *DB, config MarketplaceConfig) *MarketplaceEngine {
	return &MarketplaceEngine{
		db:        db,
		config:    config,
		listings:  make(map[string]*MktListingEntry),
		installed: make(map[string]*PluginInstallation),
		reviews:   make(map[string][]*PluginReview),
	}
}

// Search searches marketplace listings based on the given query.
func (me *MarketplaceEngine) Search(query MarketplaceSearchQuery) []*MktListingEntry {
	me.mu.RLock()
	defer me.mu.RUnlock()

	var results []*MktListingEntry
	for _, l := range me.listings {
		if query.Category != "" && l.Category != query.Category {
			continue
		}
		if query.MinRating > 0 && l.Rating < query.MinRating {
			continue
		}
		if query.FreeOnly && l.Price > 0 {
			continue
		}
		if query.Query != "" {
			q := strings.ToLower(query.Query)
			if !strings.Contains(strings.ToLower(l.Name), q) &&
				!strings.Contains(strings.ToLower(l.Description), q) &&
				!strings.Contains(strings.ToLower(l.ID), q) {
				continue
			}
		}
		results = append(results, l)
	}

	switch query.SortBy {
	case "downloads":
		sort.Slice(results, func(i, j int) bool { return results[i].Downloads > results[j].Downloads })
	case "rating":
		sort.Slice(results, func(i, j int) bool { return results[i].Rating > results[j].Rating })
	case "updated":
		sort.Slice(results, func(i, j int) bool { return results[i].UpdatedAt.After(results[j].UpdatedAt) })
	default:
		sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	}

	perPage := query.PerPage
	if perPage <= 0 {
		perPage = 20
	}
	start := query.Page * perPage
	if start >= len(results) {
		return nil
	}
	end := start + perPage
	if end > len(results) {
		end = len(results)
	}
	return results[start:end]
}

// GetListing returns a single listing by ID.
func (me *MarketplaceEngine) GetListing(id string) (*MktListingEntry, error) {
	me.mu.RLock()
	defer me.mu.RUnlock()

	l, ok := me.listings[id]
	if !ok {
		return nil, fmt.Errorf("marketplace: listing %q not found", id)
	}
	cp := *l
	return &cp, nil
}

// ListCategories returns a map of categories with the count of listings in each.
func (me *MarketplaceEngine) ListCategories() map[PluginCategory]int {
	me.mu.RLock()
	defer me.mu.RUnlock()

	cats := make(map[PluginCategory]int)
	for _, l := range me.listings {
		cats[l.Category]++
	}
	return cats
}

// Featured returns the most popular plugins sorted by downloads.
func (me *MarketplaceEngine) Featured() []*MktListingEntry {
	me.mu.RLock()
	defer me.mu.RUnlock()

	all := make([]*MktListingEntry, 0, len(me.listings))
	for _, l := range me.listings {
		all = append(all, l)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Downloads > all[j].Downloads })

	limit := 10
	if len(all) < limit {
		limit = len(all)
	}
	return all[:limit]
}

// InstallPlugin installs a plugin from the marketplace.
func (me *MarketplaceEngine) InstallPlugin(listingID string) (*PluginInstallation, error) {
	me.mu.Lock()
	defer me.mu.Unlock()

	listing, ok := me.listings[listingID]
	if !ok {
		return nil, fmt.Errorf("marketplace: listing %q not found", listingID)
	}

	if _, exists := me.installed[listingID]; exists {
		return nil, fmt.Errorf("marketplace: plugin %q already installed", listingID)
	}

	if me.config.MaxPluginSize > 0 && listing.SizeBytes > me.config.MaxPluginSize {
		return nil, fmt.Errorf("marketplace: plugin size %d exceeds limit %d", listing.SizeBytes, me.config.MaxPluginSize)
	}

	inst := &PluginInstallation{
		PluginID:    listingID,
		Version:     listing.Version,
		InstalledAt: time.Now(),
		Status:      StatusInstalled,
		Config:      make(map[string]string),
	}
	me.installed[listingID] = inst
	listing.Downloads++

	cp := *inst
	return &cp, nil
}

// UninstallPlugin removes an installed plugin.
func (me *MarketplaceEngine) UninstallPlugin(pluginID string) error {
	me.mu.Lock()
	defer me.mu.Unlock()

	if _, exists := me.installed[pluginID]; !exists {
		return fmt.Errorf("marketplace: plugin %q not installed", pluginID)
	}
	delete(me.installed, pluginID)
	return nil
}

// EnablePlugin transitions an installed plugin to active status.
func (me *MarketplaceEngine) EnablePlugin(pluginID string) error {
	me.mu.Lock()
	defer me.mu.Unlock()

	inst, exists := me.installed[pluginID]
	if !exists {
		return fmt.Errorf("marketplace: plugin %q not installed", pluginID)
	}
	inst.Status = StatusActive
	return nil
}

// DisablePlugin transitions a plugin to disabled status.
func (me *MarketplaceEngine) DisablePlugin(pluginID string) error {
	me.mu.Lock()
	defer me.mu.Unlock()

	inst, exists := me.installed[pluginID]
	if !exists {
		return fmt.Errorf("marketplace: plugin %q not installed", pluginID)
	}
	inst.Status = StatusDisabled
	return nil
}

// ListInstalled returns all installed plugins.
func (me *MarketplaceEngine) ListInstalled() []*PluginInstallation {
	me.mu.RLock()
	defer me.mu.RUnlock()

	result := make([]*PluginInstallation, 0, len(me.installed))
	for _, inst := range me.installed {
		cp := *inst
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PluginID < result[j].PluginID })
	return result
}

// CheckUpdates returns available updates for all installed plugins.
func (me *MarketplaceEngine) CheckUpdates() []*PluginUpdateInfo {
	me.mu.RLock()
	defer me.mu.RUnlock()

	var updates []*PluginUpdateInfo
	for id, inst := range me.installed {
		listing, ok := me.listings[id]
		if !ok {
			continue
		}
		if listing.Version != inst.Version {
			updates = append(updates, &PluginUpdateInfo{
				PluginID:            id,
				CurrentVersion:      inst.Version,
				LatestVersion:       listing.Version,
				Changelog:           "See release notes",
				BreakingChanges:     false,
				AutoUpdateAvailable: me.config.AutoUpdate,
			})
		}
	}
	return updates
}

// UpdatePlugin updates an installed plugin to the latest marketplace version.
func (me *MarketplaceEngine) UpdatePlugin(pluginID string) error {
	me.mu.Lock()
	defer me.mu.Unlock()

	inst, exists := me.installed[pluginID]
	if !exists {
		return fmt.Errorf("marketplace: plugin %q not installed", pluginID)
	}
	listing, ok := me.listings[pluginID]
	if !ok {
		return fmt.Errorf("marketplace: listing %q not found", pluginID)
	}
	if listing.Version == inst.Version {
		return fmt.Errorf("marketplace: plugin %q already at latest version %s", pluginID, inst.Version)
	}

	inst.Version = listing.Version
	inst.Status = StatusInstalled
	return nil
}

// PublishPlugin publishes a new plugin to the marketplace.
func (me *MarketplaceEngine) PublishPlugin(req PublishRequest) (*MktListingEntry, error) {
	me.mu.Lock()
	defer me.mu.Unlock()

	if req.Name == "" {
		return nil, fmt.Errorf("marketplace: plugin name required")
	}
	if req.Version == "" {
		return nil, fmt.Errorf("marketplace: plugin version required")
	}

	id := strings.ToLower(strings.ReplaceAll(req.Name, " ", "-"))
	if _, exists := me.listings[id]; exists {
		return nil, fmt.Errorf("marketplace: plugin %q already exists", id)
	}

	now := time.Now()
	listing := &MktListingEntry{
		ID:          id,
		Name:        req.Name,
		Description: req.Description,
		Author:      "publisher",
		Version:     req.Version,
		Category:    req.Category,
		Price:       req.Price,
		Tags:        req.Tags,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	me.listings[id] = listing

	cp := *listing
	return &cp, nil
}

// AddReview adds a review for a marketplace plugin.
func (me *MarketplaceEngine) AddReview(pluginID string, review PluginReview) error {
	me.mu.Lock()
	defer me.mu.Unlock()

	listing, ok := me.listings[pluginID]
	if !ok {
		return fmt.Errorf("marketplace: plugin %q not found", pluginID)
	}

	if review.Rating < 1 || review.Rating > 5 {
		return fmt.Errorf("marketplace: rating must be between 1 and 5")
	}

	review.PluginID = pluginID
	if review.ID == "" {
		review.ID = fmt.Sprintf("review-%s-%d", pluginID, len(me.reviews[pluginID])+1)
	}
	if review.CreatedAt.IsZero() {
		review.CreatedAt = time.Now()
	}
	me.reviews[pluginID] = append(me.reviews[pluginID], &review)

	// Update listing rating (weighted average)
	total := listing.Rating * float64(listing.ReviewsCount)
	listing.ReviewsCount++
	listing.Rating = (total + float64(review.Rating)) / float64(listing.ReviewsCount)

	return nil
}

// GetReviews returns all reviews for a plugin.
func (me *MarketplaceEngine) GetReviews(pluginID string) []*PluginReview {
	me.mu.RLock()
	defer me.mu.RUnlock()

	revs := me.reviews[pluginID]
	result := make([]*PluginReview, len(revs))
	for i, r := range revs {
		cp := *r
		result[i] = &cp
	}
	return result
}

// Stats returns aggregate marketplace statistics.
func (me *MarketplaceEngine) Stats() MarketplaceStats {
	me.mu.RLock()
	defer me.mu.RUnlock()

	stats := MarketplaceStats{
		TotalListings: len(me.listings),
		Categories:    make(map[PluginCategory]int),
	}

	var totalRating float64
	var ratedCount int
	for _, l := range me.listings {
		stats.TotalInstalls += l.Downloads
		stats.Categories[l.Category]++
		if l.Price > 0 {
			stats.TotalRevenue += l.Price * float64(l.Downloads) * (me.config.RevenueSharePct / 100.0)
		}
		if l.ReviewsCount > 0 {
			totalRating += l.Rating
			ratedCount++
		}
	}
	if ratedCount > 0 {
		stats.AvgRating = totalRating / float64(ratedCount)
	}

	for _, inst := range me.installed {
		if inst.Status == StatusActive {
			stats.ActivePlugins++
		}
	}

	return stats
}

// RegisterHTTPHandlers registers marketplace HTTP API endpoints on the given mux.
func (me *MarketplaceEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/marketplace/search", me.handleSearch)
	mux.HandleFunc("/api/v1/marketplace/featured", me.handleFeatured)
	mux.HandleFunc("/api/v1/marketplace/categories", me.handleCategories)
	mux.HandleFunc("/api/v1/marketplace/publish", me.handlePublish)
	mux.HandleFunc("/api/v1/marketplace/install", me.handleInstall)
	mux.HandleFunc("/api/v1/marketplace/stats", me.handleMktStats)
}

func (me *MarketplaceEngine) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := MarketplaceSearchQuery{
		Query:    r.URL.Query().Get("q"),
		SortBy:   r.URL.Query().Get("sort"),
	}
	if cat := r.URL.Query().Get("category"); cat != "" {
		q.Category = PluginCategory(cat)
	}
	writeJSON(w, me.Search(q))
}

func (me *MarketplaceEngine) handleFeatured(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, me.Featured())
}

func (me *MarketplaceEngine) handleCategories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, me.ListCategories())
}

func (me *MarketplaceEngine) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	listing, err := me.PublishPlugin(req)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONStatus(w, http.StatusCreated, listing)
}

func (me *MarketplaceEngine) handleInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ListingID string `json:"listing_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	inst, err := me.InstallPlugin(req.ListingID)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONStatus(w, http.StatusCreated, inst)
}

func (me *MarketplaceEngine) handleMktStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, me.Stats())
}
