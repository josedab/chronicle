package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Phase 11: Initialize default roles
func initDefaultRoles() []userRole {
	return []userRole{
		{
			ID:          "role_admin",
			Name:        "Admin",
			Description: "Full access to all features",
			Permissions: []string{"read", "write", "delete", "admin", "export", "import", "config", "users"},
		},
		{
			ID:          "role_operator",
			Name:        "Operator",
			Description: "Can view and manage data, but not configure system",
			Permissions: []string{"read", "write", "delete", "export"},
		},
		{
			ID:          "role_analyst",
			Name:        "Analyst",
			Description: "Read-only access with export capability",
			Permissions: []string{"read", "export"},
		},
		{
			ID:          "role_viewer",
			Name:        "Viewer",
			Description: "Read-only access to dashboards and metrics",
			Permissions: []string{"read"},
		},
	}
}

// Phase 11: Role Management API
func (ui *AdminUI) handleAPIRoles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		roles := make([]userRole, len(ui.roles))
		copy(roles, ui.roles)
		ui.mu.RUnlock()
		writeJSON(w, roles)

	case http.MethodPost:
		var role userRole
		if err := json.NewDecoder(r.Body).Decode(&role); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if role.Name == "" {
			http.Error(w, "Name is required", http.StatusBadRequest)
			return
		}

		role.ID = fmt.Sprintf("role_%d", time.Now().UnixNano())

		ui.mu.Lock()
		ui.roles = append(ui.roles, role)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateRole", role.Name)
		writeJSON(w, role)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		// Prevent deleting built-in roles
		if strings.HasPrefix(id, "role_admin") || strings.HasPrefix(id, "role_operator") ||
			strings.HasPrefix(id, "role_analyst") || strings.HasPrefix(id, "role_viewer") {
			http.Error(w, "Cannot delete built-in roles", http.StatusForbidden)
			return
		}

		ui.mu.Lock()
		for i, r := range ui.roles {
			if r.ID == id {
				ui.roles = append(ui.roles[:i], ui.roles[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteRole", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 11: User Permissions API
func (ui *AdminUI) handleAPIPermissions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		user := r.URL.Query().Get("user")
		ui.mu.RLock()
		if user != "" {
			if access, ok := ui.userPermissions[user]; ok {
				ui.mu.RUnlock()
				writeJSON(w, access)
				return
			}
			ui.mu.RUnlock()
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}

		// Return all user permissions
		perms := make([]*userAccess, 0, len(ui.userPermissions))
		for _, v := range ui.userPermissions {
			perms = append(perms, v)
		}
		ui.mu.RUnlock()
		writeJSON(w, perms)

	case http.MethodPost:
		var access userAccess
		if err := json.NewDecoder(r.Body).Decode(&access); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if access.User == "" || access.Role == "" {
			http.Error(w, "User and role are required", http.StatusBadRequest)
			return
		}

		// Find role and get permissions
		ui.mu.RLock()
		var rolePerms []string
		for _, role := range ui.roles {
			if role.ID == access.Role || role.Name == access.Role {
				rolePerms = role.Permissions
				access.Role = role.ID
				break
			}
		}
		ui.mu.RUnlock()

		if rolePerms == nil {
			http.Error(w, "Role not found", http.StatusBadRequest)
			return
		}

		access.Permissions = rolePerms
		access.CreatedAt = time.Now()

		ui.mu.Lock()
		ui.userPermissions[access.User] = &access
		ui.mu.Unlock()

		ui.logAudit(r, "AssignRole", fmt.Sprintf("%s -> %s", access.User, access.Role))
		writeJSON(w, access)

	case http.MethodDelete:
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "Missing user parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		delete(ui.userPermissions, user)
		ui.mu.Unlock()

		ui.logAudit(r, "RevokeAccess", user)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HasPermission checks if a user has a specific permission
func (ui *AdminUI) HasPermission(user, permission string) bool {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	access, ok := ui.userPermissions[user]
	if !ok {
		return false
	}

	for _, p := range access.Permissions {
		if p == permission || p == "admin" {
			return true
		}
	}
	return false
}
