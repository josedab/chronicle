package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestMarketplaceEngine() *MarketplaceEngine {
	cfg := DefaultMarketplaceConfig()
	return NewMarketplaceEngine(nil, cfg)
}

func seedListings(me *MarketplaceEngine) {
	me.mu.Lock()
	defer me.mu.Unlock()

	entries := []MktListingEntry{
		{ID: "snappy-compress", Name: "Snappy Compress", Description: "Fast compression plugin", Author: "acme", Version: "1.0.0", Category: CatCompression, Downloads: 500, Rating: 4.5, ReviewsCount: 10, Verified: true, Price: 0},
		{ID: "s3-storage", Name: "S3 Storage", Description: "AWS S3 backend", Author: "cloudco", Version: "2.1.0", Category: CatStorage, Downloads: 1200, Rating: 4.8, ReviewsCount: 25, Verified: true, Price: 9.99},
		{ID: "graphql-query", Name: "GraphQL Query", Description: "GraphQL query interface", Author: "devtools", Version: "1.2.0", Category: CatQuery, Downloads: 300, Rating: 4.0, ReviewsCount: 5, Verified: false, Price: 0},
		{ID: "slack-integration", Name: "Slack Integration", Description: "Slack alerts", Author: "integrations", Version: "3.0.0", Category: CatIntegration, Downloads: 800, Rating: 4.2, ReviewsCount: 15, Verified: true, Price: 0},
		{ID: "oauth-auth", Name: "OAuth Auth", Description: "OAuth2 authentication", Author: "securityco", Version: "1.1.0", Category: CatAuth, Downloads: 600, Rating: 4.6, ReviewsCount: 12, Verified: true, Price: 4.99},
	}
	for i := range entries {
		me.listings[entries[i].ID] = &entries[i]
	}
}

func TestNewMarketplaceEngine(t *testing.T) {
	me := newTestMarketplaceEngine()
	if me == nil {
		t.Fatal("expected non-nil MarketplaceEngine")
	}
	if me.config.MarketplaceURL != "https://marketplace.chronicle-db.io/api/v1" {
		t.Errorf("unexpected marketplace URL: %s", me.config.MarketplaceURL)
	}
	if !me.config.VerifySignatures {
		t.Error("expected verify signatures to be true by default")
	}
	if me.config.RevenueSharePct != 30.0 {
		t.Errorf("expected 30%% revenue share, got %f", me.config.RevenueSharePct)
	}
}

func TestMarketplaceSearch(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	// Search all
	results := me.Search(MarketplaceSearchQuery{})
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	// Search by category
	results = me.Search(MarketplaceSearchQuery{Category: CatCompression})
	if len(results) != 1 {
		t.Fatalf("expected 1 compression result, got %d", len(results))
	}
	if results[0].ID != "snappy-compress" {
		t.Errorf("expected snappy-compress, got %s", results[0].ID)
	}

	// Search by query string
	results = me.Search(MarketplaceSearchQuery{Query: "slack"})
	if len(results) != 1 {
		t.Fatalf("expected 1 result for 'slack', got %d", len(results))
	}

	// Search free only
	results = me.Search(MarketplaceSearchQuery{FreeOnly: true})
	if len(results) != 3 {
		t.Fatalf("expected 3 free results, got %d", len(results))
	}

	// Search by min rating
	results = me.Search(MarketplaceSearchQuery{MinRating: 4.5})
	if len(results) != 3 {
		t.Fatalf("expected 3 results with rating >= 4.5, got %d", len(results))
	}

	// Sort by downloads
	results = me.Search(MarketplaceSearchQuery{SortBy: "downloads"})
	if results[0].ID != "s3-storage" {
		t.Errorf("expected s3-storage first when sorted by downloads, got %s", results[0].ID)
	}
}

func TestMarketplaceInstallPlugin(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	inst, err := me.InstallPlugin("snappy-compress")
	if err != nil {
		t.Fatalf("install error: %v", err)
	}
	if inst.PluginID != "snappy-compress" {
		t.Errorf("expected plugin ID snappy-compress, got %s", inst.PluginID)
	}
	if inst.Status != StatusInstalled {
		t.Errorf("expected status installed, got %s", inst.Status)
	}
	if inst.Version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", inst.Version)
	}

	// Verify downloads incremented
	listing, _ := me.GetListing("snappy-compress")
	if listing.Downloads != 501 {
		t.Errorf("expected 501 downloads, got %d", listing.Downloads)
	}

	// Duplicate install should fail
	_, err = me.InstallPlugin("snappy-compress")
	if err == nil {
		t.Error("expected error on duplicate install")
	}

	// Install non-existent should fail
	_, err = me.InstallPlugin("nonexistent")
	if err == nil {
		t.Error("expected error on non-existent listing")
	}
}

func TestMarketplaceUninstallPlugin(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	me.InstallPlugin("snappy-compress")

	err := me.UninstallPlugin("snappy-compress")
	if err != nil {
		t.Fatalf("uninstall error: %v", err)
	}

	installed := me.ListInstalled()
	if len(installed) != 0 {
		t.Errorf("expected 0 installed, got %d", len(installed))
	}

	// Uninstall non-existent
	err = me.UninstallPlugin("nonexistent")
	if err == nil {
		t.Error("expected error on uninstalling non-existent plugin")
	}
}

func TestMarketplaceEnableDisable(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)
	me.InstallPlugin("snappy-compress")

	// Enable
	if err := me.EnablePlugin("snappy-compress"); err != nil {
		t.Fatalf("enable error: %v", err)
	}
	installed := me.ListInstalled()
	if installed[0].Status != StatusActive {
		t.Errorf("expected active status, got %s", installed[0].Status)
	}

	// Disable
	if err := me.DisablePlugin("snappy-compress"); err != nil {
		t.Fatalf("disable error: %v", err)
	}
	installed = me.ListInstalled()
	if installed[0].Status != StatusDisabled {
		t.Errorf("expected disabled status, got %s", installed[0].Status)
	}

	// Enable/Disable non-existent
	if err := me.EnablePlugin("nonexistent"); err == nil {
		t.Error("expected error on enabling non-existent plugin")
	}
	if err := me.DisablePlugin("nonexistent"); err == nil {
		t.Error("expected error on disabling non-existent plugin")
	}
}

func TestMarketplacePublishPlugin(t *testing.T) {
	me := newTestMarketplaceEngine()

	listing, err := me.PublishPlugin(PublishRequest{
		Name:        "My Plugin",
		Description: "A test plugin",
		Category:    CatAnalytics,
		Version:     "0.1.0",
		License:     "MIT",
		Tags:        []string{"analytics", "test"},
	})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	if listing.ID != "my-plugin" {
		t.Errorf("expected ID my-plugin, got %s", listing.ID)
	}
	if listing.Category != CatAnalytics {
		t.Errorf("expected category analytics, got %s", listing.Category)
	}

	// Duplicate publish
	_, err = me.PublishPlugin(PublishRequest{Name: "My Plugin", Version: "0.2.0"})
	if err == nil {
		t.Error("expected error on duplicate publish")
	}

	// Missing name
	_, err = me.PublishPlugin(PublishRequest{Version: "1.0.0"})
	if err == nil {
		t.Error("expected error on missing name")
	}

	// Missing version
	_, err = me.PublishPlugin(PublishRequest{Name: "Another"})
	if err == nil {
		t.Error("expected error on missing version")
	}
}

func TestMarketplaceReviews(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	// Add review
	err := me.AddReview("snappy-compress", PluginReview{
		User:   "alice",
		Rating: 5,
		Title:  "Great plugin",
		Body:   "Works perfectly",
	})
	if err != nil {
		t.Fatalf("add review error: %v", err)
	}

	reviews := me.GetReviews("snappy-compress")
	if len(reviews) != 1 {
		t.Fatalf("expected 1 review, got %d", len(reviews))
	}
	if reviews[0].User != "alice" {
		t.Errorf("expected user alice, got %s", reviews[0].User)
	}
	if reviews[0].Rating != 5 {
		t.Errorf("expected rating 5, got %d", reviews[0].Rating)
	}

	// Invalid rating
	err = me.AddReview("snappy-compress", PluginReview{User: "bob", Rating: 0})
	if err == nil {
		t.Error("expected error on rating 0")
	}
	err = me.AddReview("snappy-compress", PluginReview{User: "bob", Rating: 6})
	if err == nil {
		t.Error("expected error on rating 6")
	}

	// Review for non-existent plugin
	err = me.AddReview("nonexistent", PluginReview{User: "bob", Rating: 3})
	if err == nil {
		t.Error("expected error on non-existent plugin review")
	}

	// Reviews for non-existent plugin returns empty
	reviews = me.GetReviews("nonexistent")
	if len(reviews) != 0 {
		t.Errorf("expected 0 reviews for nonexistent, got %d", len(reviews))
	}
}

func TestMarketplaceCheckUpdates(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)
	me.InstallPlugin("snappy-compress")

	// No updates when versions match
	updates := me.CheckUpdates()
	if len(updates) != 0 {
		t.Fatalf("expected 0 updates, got %d", len(updates))
	}

	// Simulate a newer version in marketplace
	me.mu.Lock()
	me.listings["snappy-compress"].Version = "2.0.0"
	me.mu.Unlock()

	updates = me.CheckUpdates()
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].CurrentVersion != "1.0.0" {
		t.Errorf("expected current 1.0.0, got %s", updates[0].CurrentVersion)
	}
	if updates[0].LatestVersion != "2.0.0" {
		t.Errorf("expected latest 2.0.0, got %s", updates[0].LatestVersion)
	}
}

func TestMarketplaceUpdatePlugin(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)
	me.InstallPlugin("snappy-compress")

	// No update needed
	err := me.UpdatePlugin("snappy-compress")
	if err == nil {
		t.Error("expected error when already at latest version")
	}

	// Simulate a newer version
	me.mu.Lock()
	me.listings["snappy-compress"].Version = "2.0.0"
	me.mu.Unlock()

	err = me.UpdatePlugin("snappy-compress")
	if err != nil {
		t.Fatalf("update error: %v", err)
	}

	installed := me.ListInstalled()
	if installed[0].Version != "2.0.0" {
		t.Errorf("expected version 2.0.0 after update, got %s", installed[0].Version)
	}

	// Update non-existent
	err = me.UpdatePlugin("nonexistent")
	if err == nil {
		t.Error("expected error on updating non-existent plugin")
	}
}

func TestMarketplaceFeatured(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	featured := me.Featured()
	if len(featured) != 5 {
		t.Fatalf("expected 5 featured, got %d", len(featured))
	}
	// First should be most downloaded
	if featured[0].ID != "s3-storage" {
		t.Errorf("expected s3-storage as top featured, got %s", featured[0].ID)
	}
}

func TestMarketplaceStats(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)
	me.InstallPlugin("snappy-compress")
	me.EnablePlugin("snappy-compress")

	stats := me.Stats()
	if stats.TotalListings != 5 {
		t.Errorf("expected 5 listings, got %d", stats.TotalListings)
	}
	if stats.ActivePlugins != 1 {
		t.Errorf("expected 1 active plugin, got %d", stats.ActivePlugins)
	}
	if len(stats.Categories) == 0 {
		t.Error("expected non-empty categories")
	}
	if stats.AvgRating <= 0 {
		t.Error("expected positive average rating")
	}
	if stats.TotalRevenue <= 0 {
		t.Error("expected positive total revenue from paid plugins")
	}
}

func TestMarketplaceHTTPHandlers(t *testing.T) {
	me := newTestMarketplaceEngine()
	seedListings(me)

	mux := http.NewServeMux()
	me.RegisterHTTPHandlers(mux)

	// GET /api/v1/marketplace/search
	req := httptest.NewRequest(http.MethodGet, "/api/v1/marketplace/search?q=snappy", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("search: expected 200, got %d", rec.Code)
	}
	var searchResults []*MktListingEntry
	json.NewDecoder(rec.Body).Decode(&searchResults)
	if len(searchResults) != 1 {
		t.Errorf("search: expected 1 result, got %d", len(searchResults))
	}

	// GET /api/v1/marketplace/featured
	req = httptest.NewRequest(http.MethodGet, "/api/v1/marketplace/featured", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("featured: expected 200, got %d", rec.Code)
	}

	// GET /api/v1/marketplace/categories
	req = httptest.NewRequest(http.MethodGet, "/api/v1/marketplace/categories", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("categories: expected 200, got %d", rec.Code)
	}

	// POST /api/v1/marketplace/publish
	pubBody, _ := json.Marshal(PublishRequest{
		Name:     "HTTP Plugin",
		Version:  "1.0.0",
		Category: CatIntegration,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/marketplace/publish", bytes.NewReader(pubBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("publish: expected 201, got %d", rec.Code)
	}

	// POST /api/v1/marketplace/install
	instBody, _ := json.Marshal(map[string]string{"listing_id": "snappy-compress"})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/marketplace/install", bytes.NewReader(instBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("install: expected 201, got %d", rec.Code)
	}

	// GET /api/v1/marketplace/stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/marketplace/stats", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("stats: expected 200, got %d", rec.Code)
	}

	// Method not allowed
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/marketplace/search", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}
