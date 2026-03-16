package chronicle

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDataMaskingEngine(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultDataMaskingConfig()
	engine := NewDataMaskingEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	t.Run("AddRule", func(t *testing.T) {
		err := engine.AddRule(MaskingRule{
			ID:            "r1",
			MetricPattern: "cpu.*",
			TagKey:        "host",
			Action:        "redact",
			Priority:      1,
		})
		if err != nil {
			t.Fatalf("AddRule failed: %v", err)
		}

		rules := engine.ListRules()
		if len(rules) != 1 {
			t.Errorf("expected 1 rule, got %d", len(rules))
		}
	})

	t.Run("AddDuplicateRule", func(t *testing.T) {
		err := engine.AddRule(MaskingRule{
			ID:     "r1",
			TagKey: "host",
			Action: "redact",
		})
		if err == nil {
			t.Error("expected error for duplicate rule ID")
		}
	})

	t.Run("RemoveRule", func(t *testing.T) {
		engine.AddRule(MaskingRule{ID: "r-remove", TagKey: "env", Action: "hash"})
		err := engine.RemoveRule("r-remove")
		if err != nil {
			t.Fatalf("RemoveRule failed: %v", err)
		}
		err = engine.RemoveRule("nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent rule")
		}
	})

	t.Run("ApplyRedact", func(t *testing.T) {
		db2 := setupTestDB(t)
		defer db2.Close()
		e2 := NewDataMaskingEngine(db2, cfg)
		e2.Start()
		defer e2.Stop()

		e2.AddRule(MaskingRule{
			ID:            "redact-host",
			MetricPattern: "*",
			TagKey:        "host",
			Action:        "redact",
		})

		points := []Point{
			{Metric: "cpu.usage", Value: 80, Tags: map[string]string{"host": "server1", "dc": "us-east"}},
		}
		ctx := MaskingContext{UserRole: "analyst", Purpose: "report"}

		masked, result := e2.Apply(points, ctx)
		if result.MaskedPointCount != 1 {
			t.Errorf("expected 1 masked point, got %d", result.MaskedPointCount)
		}
		if masked[0].Tags["host"] != "***" {
			t.Errorf("expected host to be '***', got %s", masked[0].Tags["host"])
		}
		if masked[0].Tags["dc"] != "us-east" {
			t.Errorf("expected dc to be unchanged, got %s", masked[0].Tags["dc"])
		}
	})

	t.Run("ApplyHash", func(t *testing.T) {
		db3 := setupTestDB(t)
		defer db3.Close()
		e3 := NewDataMaskingEngine(db3, cfg)
		e3.Start()
		defer e3.Stop()

		e3.AddRule(MaskingRule{
			ID:            "hash-user",
			MetricPattern: "*",
			TagKey:        "user",
			Action:        "hash",
		})

		points := []Point{
			{Metric: "api.calls", Value: 1, Tags: map[string]string{"user": "alice"}},
		}
		ctx := MaskingContext{UserRole: "admin"}

		masked, result := e3.Apply(points, ctx)
		if result.MaskedPointCount != 1 {
			t.Errorf("expected 1 masked point, got %d", result.MaskedPointCount)
		}
		if masked[0].Tags["user"] == "alice" {
			t.Error("expected user tag to be hashed, got original value")
		}
		if masked[0].Tags["user"] == "" {
			t.Error("expected non-empty hashed value")
		}
	})

	t.Run("ApplyTruncate", func(t *testing.T) {
		db4 := setupTestDB(t)
		defer db4.Close()
		e4 := NewDataMaskingEngine(db4, cfg)
		e4.Start()
		defer e4.Stop()

		e4.AddRule(MaskingRule{
			ID:            "trunc-email",
			MetricPattern: "*",
			TagKey:        "email",
			Action:        "truncate",
		})

		points := []Point{
			{Metric: "login.count", Value: 1, Tags: map[string]string{"email": "alice@example.com"}},
		}
		ctx := MaskingContext{UserRole: "viewer"}

		masked, _ := e4.Apply(points, ctx)
		if masked[0].Tags["email"] != "ali..." {
			t.Errorf("expected email to be 'ali...', got %s", masked[0].Tags["email"])
		}
	})

	t.Run("NoRulesPassThrough", func(t *testing.T) {
		db5 := setupTestDB(t)
		defer db5.Close()
		e5 := NewDataMaskingEngine(db5, cfg)
		e5.Start()
		defer e5.Stop()

		points := []Point{
			{Metric: "cpu.usage", Value: 50, Tags: map[string]string{"host": "server1"}},
		}
		ctx := MaskingContext{UserRole: "admin"}

		masked, result := e5.Apply(points, ctx)
		if result.MaskedPointCount != 0 {
			t.Errorf("expected 0 masked points, got %d", result.MaskedPointCount)
		}
		if masked[0].Tags["host"] != "server1" {
			t.Errorf("expected host unchanged, got %s", masked[0].Tags["host"])
		}
	})

	t.Run("RoleBasedFiltering", func(t *testing.T) {
		db6 := setupTestDB(t)
		defer db6.Close()
		e6 := NewDataMaskingEngine(db6, cfg)
		e6.Start()
		defer e6.Stop()

		e6.AddRule(MaskingRule{
			ID:            "role-rule",
			MetricPattern: "*",
			TagKey:        "secret",
			Action:        "redact",
			Role:          "viewer", // only applies to viewers
		})

		points := []Point{
			{Metric: "data", Value: 1, Tags: map[string]string{"secret": "value123"}},
		}

		// admin should not be masked
		adminCtx := MaskingContext{UserRole: "admin"}
		masked, result := e6.Apply(points, adminCtx)
		if result.MaskedPointCount != 0 {
			t.Errorf("expected 0 masked for admin, got %d", result.MaskedPointCount)
		}
		if masked[0].Tags["secret"] != "value123" {
			t.Errorf("expected unchanged for admin, got %s", masked[0].Tags["secret"])
		}

		// viewer should be masked
		viewerCtx := MaskingContext{UserRole: "viewer"}
		masked, result = e6.Apply(points, viewerCtx)
		if result.MaskedPointCount != 1 {
			t.Errorf("expected 1 masked for viewer, got %d", result.MaskedPointCount)
		}
		if masked[0].Tags["secret"] != "***" {
			t.Errorf("expected '***' for viewer, got %s", masked[0].Tags["secret"])
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats.RuleCount == 0 {
			t.Error("expected non-zero rule count")
		}
	})

	t.Run("MaxRules", func(t *testing.T) {
		db7 := setupTestDB(t)
		defer db7.Close()
		smallCfg := DefaultDataMaskingConfig()
		smallCfg.MaxRules = 1
		e7 := NewDataMaskingEngine(db7, smallCfg)
		e7.Start()
		defer e7.Stop()

		e7.AddRule(MaskingRule{ID: "first", TagKey: "a", Action: "redact"})
		err := e7.AddRule(MaskingRule{ID: "second", TagKey: "b", Action: "hash"})
		if err == nil {
			t.Error("expected error when max rules exceeded")
		}
	})

	t.Run("HashUsesHMACSHA256WithSecret", func(t *testing.T) {
		db8 := setupTestDB(t)
		defer db8.Close()
		secretCfg := DefaultDataMaskingConfig()
		secretCfg.HashSecret = "test-secret-key"
		e8 := NewDataMaskingEngine(db8, secretCfg)
		e8.Start()
		defer e8.Stop()

		e8.AddRule(MaskingRule{ID: "hmac-rule", MetricPattern: "*", TagKey: "user", Action: "hash"})

		points := []Point{
			{Metric: "api.calls", Value: 1, Tags: map[string]string{"user": "alice"}},
		}
		ctx := MaskingContext{UserRole: "admin"}
		masked, _ := e8.Apply(points, ctx)

		// Verify the hash matches HMAC-SHA256
		mac := hmac.New(sha256.New, []byte("test-secret-key"))
		mac.Write([]byte("alice"))
		expected := hex.EncodeToString(mac.Sum(nil))

		if masked[0].Tags["user"] != expected {
			t.Errorf("expected HMAC-SHA256 hash %s, got %s", expected, masked[0].Tags["user"])
		}

		// Same input should produce same output (deterministic)
		masked2, _ := e8.Apply(points, ctx)
		if masked2[0].Tags["user"] != masked[0].Tags["user"] {
			t.Error("expected deterministic hashing")
		}
	})

	t.Run("HashUsesSHA256WithoutSecret", func(t *testing.T) {
		db9 := setupTestDB(t)
		defer db9.Close()
		noSecretCfg := DefaultDataMaskingConfig()
		// HashSecret is empty
		e9 := NewDataMaskingEngine(db9, noSecretCfg)
		e9.Start()
		defer e9.Stop()

		e9.AddRule(MaskingRule{ID: "sha-rule", MetricPattern: "*", TagKey: "user", Action: "hash"})

		points := []Point{
			{Metric: "api.calls", Value: 1, Tags: map[string]string{"user": "bob"}},
		}
		ctx := MaskingContext{UserRole: "admin"}
		masked, _ := e9.Apply(points, ctx)

		h := sha256.Sum256([]byte("bob"))
		expected := hex.EncodeToString(h[:])

		if masked[0].Tags["user"] != expected {
			t.Errorf("expected SHA-256 hash %s, got %s", expected, masked[0].Tags["user"])
		}

		// Hash should be 64 hex chars (256 bits)
		if len(masked[0].Tags["user"]) != 64 {
			t.Errorf("expected 64-char hex hash, got %d chars", len(masked[0].Tags["user"]))
		}
	})

	t.Run("DifferentSecretsProduceDifferentHashes", func(t *testing.T) {
		results := make([]string, 2)
		for i, secret := range []string{"secret-a", "secret-b"} {
			dbN := setupTestDB(t)
			defer dbN.Close()
			cfg := DefaultDataMaskingConfig()
			cfg.HashSecret = secret
			eN := NewDataMaskingEngine(dbN, cfg)
			eN.Start()
			defer eN.Stop()
			eN.AddRule(MaskingRule{ID: "r1", MetricPattern: "*", TagKey: "user", Action: "hash"})
			points := []Point{{Metric: "m", Value: 1, Tags: map[string]string{"user": "same-input"}}}
			masked, _ := eN.Apply(points, MaskingContext{})
			results[i] = masked[0].Tags["user"]
		}
		if results[0] == results[1] {
			t.Error("different secrets should produce different hashes")
		}
	})
}

func TestDataMaskingHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultDataMaskingConfig()
	engine := NewDataMaskingEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	t.Run("POST_CreateRule", func(t *testing.T) {
		rule := MaskingRule{
			ID:            "http-r1",
			MetricPattern: "cpu.*",
			TagKey:        "host",
			Action:        "redact",
		}
		body, _ := json.Marshal(rule)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/masking/rules", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
		}

		rules := engine.ListRules()
		if len(rules) != 1 || rules[0].ID != "http-r1" {
			t.Errorf("expected rule to be created, got %d rules", len(rules))
		}
	})

	t.Run("POST_DuplicateRule", func(t *testing.T) {
		rule := MaskingRule{ID: "http-r1", TagKey: "host", Action: "redact"}
		body, _ := json.Marshal(rule)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/masking/rules", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusConflict {
			t.Errorf("expected 409 for duplicate, got %d", w.Code)
		}
	})

	t.Run("POST_MissingID", func(t *testing.T) {
		rule := MaskingRule{TagKey: "host", Action: "redact"}
		body, _ := json.Marshal(rule)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/masking/rules", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for missing ID, got %d", w.Code)
		}
	})

	t.Run("POST_InvalidAction", func(t *testing.T) {
		rule := MaskingRule{ID: "bad-action", TagKey: "host", Action: "encrypt"}
		body, _ := json.Marshal(rule)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/masking/rules", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid action, got %d", w.Code)
		}
	})

	t.Run("GET_ListRules", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/masking/rules", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var rules []MaskingRule
		json.Unmarshal(w.Body.Bytes(), &rules)
		if len(rules) == 0 {
			t.Error("expected at least one rule")
		}
	})

	t.Run("DELETE_Rule", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/masking/rules?id=http-r1", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("expected 204, got %d", w.Code)
		}
	})

	t.Run("DELETE_NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/masking/rules?id=nonexistent", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", w.Code)
		}
	})

	t.Run("DELETE_MissingID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/masking/rules", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("GET_Stats", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/masking/stats", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/masking/rules", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}
