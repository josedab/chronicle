package chronicle

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"crypto/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// API token management, rate limiting, auth middleware, and auth manager for TLS authentication.

// --- API Token Manager ---

// APITokenInfo represents a managed API token.
type APITokenInfo struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	TokenHash string    `json:"token_hash,omitempty"`
	Scopes    []string  `json:"scopes"`
	RateLimit float64   `json:"rate_limit"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Revoked   bool      `json:"revoked"`
}

// APITokenCreateResult is returned when a new token is created.
type APITokenCreateResult struct {
	Token string       `json:"token"`
	Info  APITokenInfo `json:"info"`
}

// APITokenManager manages API tokens with hashing and rate limiting.
type APITokenManager struct {
	config  APITokenConfig
	mu      sync.RWMutex
	tokens  map[string]*APITokenInfo // keyed by token hash
	byID    map[string]*APITokenInfo // keyed by token ID
	nextID  int64
	running bool
	stopCh  chan struct{}
}

// NewAPITokenManager creates a new API token manager.
func NewAPITokenManager(cfg APITokenConfig) *APITokenManager {
	return &APITokenManager{
		config: cfg,
		tokens: make(map[string]*APITokenInfo),
		byID:   make(map[string]*APITokenInfo),
		stopCh: make(chan struct{}),
	}
}

// CreateToken creates a new API token with the given name, scopes, and rate limit.
func (m *APITokenManager) CreateToken(name string, scopes []string, rateLimit float64) (*APITokenCreateResult, error) {
	if name == "" {
		return nil, errors.New("token name is required")
	}
	if rateLimit <= 0 {
		rateLimit = m.config.DefaultRateLimit
	}

	rawToken := make([]byte, 32)
	if _, err := rand.Read(rawToken); err != nil {
		return nil, fmt.Errorf("generating token: %w", err)
	}
	tokenStr := base64.URLEncoding.EncodeToString(rawToken)
	tokenHash := hashToken(tokenStr)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	info := &APITokenInfo{
		ID:        fmt.Sprintf("tok_%d", m.nextID),
		Name:      name,
		TokenHash: tokenHash,
		Scopes:    scopes,
		RateLimit: rateLimit,
		CreatedAt: time.Now(),
	}

	m.tokens[tokenHash] = info
	m.byID[info.ID] = info

	return &APITokenCreateResult{
		Token: tokenStr,
		Info:  *info,
	}, nil
}

// ValidateToken validates a raw token string and returns its info.
// Uses constant-time comparison to prevent timing attacks.
func (m *APITokenManager) ValidateToken(rawToken string) (*APITokenInfo, error) {
	tokenHash := hashToken(rawToken)

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Constant-time scan to prevent timing side-channel attacks
	var matched *APITokenInfo
	for storedHash, info := range m.tokens {
		if subtle.ConstantTimeCompare([]byte(storedHash), []byte(tokenHash)) == 1 {
			matched = info
		}
	}
	if matched == nil {
		return nil, errors.New("invalid API token")
	}
	if matched.Revoked {
		return nil, errors.New("token has been revoked")
	}
	if !matched.ExpiresAt.IsZero() && time.Now().After(matched.ExpiresAt) {
		return nil, errors.New("token has expired")
	}
	return matched, nil
}

// RevokeToken revokes a token by its ID.
func (m *APITokenManager) RevokeToken(tokenID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.byID[tokenID]
	if !ok {
		return fmt.Errorf("token %q not found", tokenID)
	}
	info.Revoked = true
	return nil
}

// ListTokens returns all non-revoked tokens (without hashes).
func (m *APITokenManager) ListTokens() []APITokenInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []APITokenInfo
	for _, info := range m.byID {
		if !info.Revoked {
			safe := *info
			safe.TokenHash = ""
			result = append(result, safe)
		}
	}
	return result
}

func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return base64.URLEncoding.EncodeToString(h[:])
}

// --- Token Bucket Rate Limiter ---

type tokenBucket struct {
	tokens     float64
	capacity   float64
	rate       float64
	lastRefill time.Time
}

// TokenBucketLimiter provides per-token rate limiting using the token bucket algorithm.
type TokenBucketLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	stopCh  chan struct{}
	running bool
}

// NewTokenBucketLimiter creates a new per-token rate limiter.
func NewTokenBucketLimiter() *TokenBucketLimiter {
	return &TokenBucketLimiter{
		buckets: make(map[string]*tokenBucket),
		stopCh:  make(chan struct{}),
	}
}

// Allow checks whether a request for the given tokenID is permitted under rate limits.
func (l *TokenBucketLimiter) Allow(tokenID string, rate float64, burst int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[tokenID]
	if !ok {
		b = &tokenBucket{
			tokens:     float64(burst),
			capacity:   float64(burst),
			rate:       rate,
			lastRefill: time.Now(),
		}
		l.buckets[tokenID] = b
	}

	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Start begins the stale bucket cleanup goroutine.
func (l *TokenBucketLimiter) Start() {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return
	}
	l.running = true
	l.stopCh = make(chan struct{})
	l.mu.Unlock()

	go l.cleanupLoop()
}

// Stop stops the cleanup goroutine.
func (l *TokenBucketLimiter) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return
	}
	l.running = false
	close(l.stopCh)
}

func (l *TokenBucketLimiter) cleanupLoop() {
	ticker := time.NewTicker(defaultBucketCleanup)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.cleanup()
		}
	}
}

func (l *TokenBucketLimiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()
	threshold := time.Now().Add(-defaultStaleBucketAge)
	for id, b := range l.buckets {
		if b.lastRefill.Before(threshold) {
			delete(l.buckets, id)
		}
	}
}

// --- Auth Middleware ---

// AuthMiddleware provides HTTP authentication middleware.
type AuthMiddleware struct {
	jwksValidator   *JWKSValidator
	tokenManager    *APITokenManager
	rateLimiter     *TokenBucketLimiter
	oauthEnabled    bool
	apiTokenEnabled bool
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(jwks *JWKSValidator, tokens *APITokenManager, limiter *TokenBucketLimiter, cfg TLSAuthConfig) *AuthMiddleware {
	return &AuthMiddleware{
		jwksValidator:   jwks,
		tokenManager:    tokens,
		rateLimiter:     limiter,
		oauthEnabled:    cfg.OAuth2.Enabled,
		apiTokenEnabled: cfg.APIToken.Enabled,
	}
}

// Authenticate returns middleware that validates Bearer tokens (JWT) and API keys.
func (a *AuthMiddleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.oauthEnabled && !a.apiTokenEnabled {
			next.ServeHTTP(w, r)
			return
		}

		// Try API key first (X-API-Key header)
		if a.apiTokenEnabled {
			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				info, err := a.tokenManager.ValidateToken(apiKey)
				if err != nil {
					http.Error(w, "invalid API key", http.StatusUnauthorized)
					return
				}
				if !a.rateLimiter.Allow(info.ID, info.RateLimit, defaultTokenBurst) {
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
		}

		// Try Bearer token (Authorization header)
		if a.oauthEnabled {
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
				claims, err := a.jwksValidator.ValidateToken(tokenStr)
				if err != nil {
					http.Error(w, "invalid bearer token", http.StatusUnauthorized)
					return
				}
				if !a.checkScopes(claims.Scopes) {
					http.Error(w, "insufficient scopes", http.StatusForbidden)
					return
				}
				// Rate limit JWT tokens using the subject claim as key
				rateLimitKey := "jwt:" + claims.Subject
				if rateLimitKey == "jwt:" {
					rateLimitKey = "jwt:" + claims.Issuer
				}
				if !a.rateLimiter.Allow(rateLimitKey, defaultTokenRateLimit, defaultTokenBurst) {
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
		}

		http.Error(w, "authentication required", http.StatusUnauthorized)
	})
}

func (a *AuthMiddleware) checkScopes(tokenScopes []string) bool {
	if a.jwksValidator == nil || len(a.jwksValidator.config.AllowedScopes) == 0 {
		return true
	}
	required := make(map[string]bool, len(a.jwksValidator.config.AllowedScopes))
	for _, s := range a.jwksValidator.config.AllowedScopes {
		required[s] = true
	}
	for _, s := range tokenScopes {
		if required[s] {
			return true
		}
	}
	return false
}

// --- Auth Manager (Engine) ---

// AuthManager combines TLS, JWT, and API token authentication into a single engine.
type AuthManager struct {
	db           *DB
	config       TLSAuthConfig
	mu           sync.RWMutex
	running      bool
	stopCh       chan struct{}
	tlsProvider  *TLSProvider
	jwksValid    *JWKSValidator
	tokenManager *APITokenManager
	rateLimiter  *TokenBucketLimiter
	middleware   *AuthMiddleware
}

// NewAuthManager creates a new AuthManager.
func NewAuthManager(db *DB, cfg TLSAuthConfig) *AuthManager {
	tProv := NewTLSProvider(cfg.TLS)
	jv := NewJWKSValidator(cfg.OAuth2)
	tm := NewAPITokenManager(cfg.APIToken)
	rl := NewTokenBucketLimiter()

	return &AuthManager{
		db:           db,
		config:       cfg,
		stopCh:       make(chan struct{}),
		tlsProvider:  tProv,
		jwksValid:    jv,
		tokenManager: tm,
		rateLimiter:  rl,
		middleware:   NewAuthMiddleware(jv, tm, rl, cfg),
	}
}

// Start starts the auth manager and its sub-components.
func (m *AuthManager) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()

	if m.config.TLS.Enabled {
		m.tlsProvider.Start()
	}
	m.rateLimiter.Start()
}

// Stop stops the auth manager and its sub-components.
func (m *AuthManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	m.running = false
	close(m.stopCh)
	m.tlsProvider.Stop()
	m.rateLimiter.Stop()
}

// TLSConfig returns the built TLS configuration.
func (m *AuthManager) TLSConfig() (*tls.Config, error) {
	return m.tlsProvider.BuildTLSConfig()
}

// Middleware returns the HTTP authentication middleware.
func (m *AuthManager) Middleware() *AuthMiddleware {
	return m.middleware
}

// RegisterAuthRoutes registers auth-related HTTP endpoints.
func (m *AuthManager) RegisterAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/auth/tokens", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			m.handleCreateToken(w, r)
		case http.MethodGet:
			m.handleListTokens(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/auth/tokens/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		m.handleRevokeToken(w, r)
	})

	mux.HandleFunc("/api/v1/auth/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		m.handleAuthStatus(w, r)
	})
}

type createTokenRequest struct {
	Name      string   `json:"name"`
	Scopes    []string `json:"scopes"`
	RateLimit float64  `json:"rate_limit"`
}

func (m *AuthManager) handleCreateToken(w http.ResponseWriter, r *http.Request) {
	if !m.config.APIToken.Enabled {
		http.Error(w, "API token management is disabled", http.StatusNotFound)
		return
	}

	var req createTokenRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, MaxAuthBodySize)).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	result, err := m.tokenManager.CreateToken(req.Name, req.Scopes, req.RateLimit)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("chronicle: json encode error: %v", err)
	}
}

func (m *AuthManager) handleListTokens(w http.ResponseWriter, _ *http.Request) {
	if !m.config.APIToken.Enabled {
		http.Error(w, "API token management is disabled", http.StatusNotFound)
		return
	}

	tokens := m.tokenManager.ListTokens()
	if tokens == nil {
		tokens = []APITokenInfo{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tokens); err != nil {
		log.Printf("chronicle: json encode error: %v", err)
	}
}

func (m *AuthManager) handleRevokeToken(w http.ResponseWriter, r *http.Request) {
	if !m.config.APIToken.Enabled {
		http.Error(w, "API token management is disabled", http.StatusNotFound)
		return
	}

	// Extract token ID from path: /api/v1/auth/tokens/{id}
	tokenID := strings.TrimPrefix(r.URL.Path, "/api/v1/auth/tokens/")
	if tokenID == "" {
		http.Error(w, "token ID is required", http.StatusBadRequest)
		return
	}

	if err := m.tokenManager.RevokeToken(tokenID); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "revoked", "id": tokenID}); err != nil {
		log.Printf("chronicle: json encode error: %v", err)
	}
}

func (m *AuthManager) handleAuthStatus(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]any{
		"running": m.running,
		"tls": map[string]any{
			"enabled":             m.config.TLS.Enabled,
			"require_client_cert": m.config.TLS.RequireClientCert,
			"min_tls_version":     m.config.TLS.MinTLSVersion,
		},
		"oauth2": map[string]any{
			"enabled":    m.config.OAuth2.Enabled,
			"issuer_url": m.config.OAuth2.IssuerURL,
		},
		"api_tokens": map[string]any{
			"enabled":      m.config.APIToken.Enabled,
			"active_count": len(m.tokenManager.ListTokens()),
		},
		"auto_cert": map[string]any{
			"enabled": m.config.AutoCert.Enabled,
			"domains": m.config.AutoCert.Domains,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// --- Helpers ---

func base64URLDecode(s string) ([]byte, error) {
	// Add padding if needed
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.URLEncoding.DecodeString(s)
}

// loadPEMCertificates loads PEM-encoded certificates from a file path.
// It returns the parsed x509 certificates. This is useful for loading
// CA bundles for client certificate verification.
func loadPEMCertificates(path string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading PEM file: %w", err)
	}

	var certs []*x509.Certificate
	for len(data) > 0 {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.New("no certificates found in PEM file")
	}
	return certs, nil
}
