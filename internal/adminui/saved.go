package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// Phase 10: Saved Queries API
func (ui *AdminUI) handleAPISavedQueries(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "SavedQueriesAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		queries := make([]savedQuery, len(ui.savedQueries))
		copy(queries, ui.savedQueries)
		ui.mu.RUnlock()

		// Sort by usage count descending
		sort.Slice(queries, func(i, j int) bool {
			return queries[i].UsageCount > queries[j].UsageCount
		})
		writeJSON(w, queries)

	case http.MethodPost:
		var query savedQuery
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		query.ID = fmt.Sprintf("query_%d", time.Now().UnixNano())
		query.CreatedAt = time.Now()
		query.UsageCount = 0

		ui.mu.Lock()
		ui.savedQueries = append(ui.savedQueries, query)
		ui.mu.Unlock()

		ui.logActivity("Query Saved", query.Name)
		writeJSON(w, query)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing query id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, q := range ui.savedQueries {
			if q.ID == id {
				ui.savedQueries = append(ui.savedQueries[:i], ui.savedQueries[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Query Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 10: Favorites API
func (ui *AdminUI) handleAPIFavorites(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		favs := make([]favoriteItem, len(ui.favorites))
		copy(favs, ui.favorites)
		ui.mu.RUnlock()
		writeJSON(w, favs)

	case http.MethodPost:
		var fav favoriteItem
		if err := json.NewDecoder(r.Body).Decode(&fav); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		fav.ID = fmt.Sprintf("fav_%d", time.Now().UnixNano())
		fav.CreatedAt = time.Now()

		ui.mu.Lock()
		// Check for duplicates
		exists := false
		for _, f := range ui.favorites {
			if f.Type == fav.Type && f.Name == fav.Name {
				exists = true
				break
			}
		}
		if !exists {
			ui.favorites = append(ui.favorites, fav)
		}
		ui.mu.Unlock()

		writeJSON(w, fav)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing favorite id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, f := range ui.favorites {
			if f.ID == id {
				ui.favorites = append(ui.favorites[:i], ui.favorites[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 10: Recent Items API
func (ui *AdminUI) handleAPIRecent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 20
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 50 {
				limit = n
			}
		}

		ui.mu.RLock()
		items := make([]recentItem, 0, limit)
		start := len(ui.recentItems) - limit
		if start < 0 {
			start = 0
		}
		for i := len(ui.recentItems) - 1; i >= start; i-- {
			items = append(items, ui.recentItems[i])
		}
		ui.mu.RUnlock()
		writeJSON(w, items)

	case http.MethodPost:
		var item recentItem
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		item.AccessedAt = time.Now()
		ui.addRecentItem(item)
		writeJSON(w, item)

	case http.MethodDelete:
		// Clear all recent items
		ui.mu.Lock()
		ui.recentItems = make([]recentItem, 0, 50)
		ui.mu.Unlock()
		writeJSON(w, map[string]string{"status": "cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (ui *AdminUI) addRecentItem(item recentItem) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	// Remove duplicate if exists
	for i, r := range ui.recentItems {
		if r.Type == item.Type && r.Name == item.Name {
			ui.recentItems = append(ui.recentItems[:i], ui.recentItems[i+1:]...)
			break
		}
	}

	ui.recentItems = append(ui.recentItems, item)
	if len(ui.recentItems) > 50 {
		ui.recentItems = ui.recentItems[1:]
	}
}
