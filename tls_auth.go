package chronicle

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
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
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// TLS version constants for configuration.
const (
	TLSVersion12 = "1.2"
	TLSVersion13 = "1.3"
)

// Token hash algorithm constants.
const (
	TokenHashSHA256 = "sha256"
)

// Default configuration values.
const (
	defaultTokenRateLimit    = 100.0
	defaultTokenBurst        = 50
	defaultJWKSCacheDuration = 15 * time.Minute
	defaultBucketCleanup     = 5 * time.Minute
	defaultStaleBucketAge    = 10 * time.Minute
)

// TLSAuthConfig configures TLS/mTLS, OAuth2/OIDC, and API token authentication.
type TLSAuthConfig struct {
	TLS      TLSConfig      `json:"tls"`
	OAuth2   OAuth2Config   `json:"oauth2"`
	APIToken APITokenConfig `json:"api_token"`
	AutoCert AutoCertConfig `json:"auto_cert"`
}

// TLSConfig holds TLS/mTLS settings.
type TLSConfig struct {
	Enabled           bool   `json:"enabled"`
	CertFile          string `json:"cert_file"`
	KeyFile           string `json:"key_file"`
	ClientCAFile      string `json:"client_ca_file"`
	MinTLSVersion     string `json:"min_tls_version"`
	RequireClientCert bool   `json:"require_client_cert"`
}

// OAuth2Config holds OAuth2/OIDC settings.
type OAuth2Config struct {
	Enabled       bool     `json:"enabled"`
	IssuerURL     string   `json:"issuer_url"`
	JWKSURL       string   `json:"jwks_url"`
	Audience      string   `json:"audience"`
	AllowedScopes []string `json:"allowed_scopes"`
}

// APITokenConfig holds API token management settings.
type APITokenConfig struct {
	Enabled            bool    `json:"enabled"`
	DefaultRateLimit   float64 `json:"default_rate_limit"`
	TokenHashAlgorithm string  `json:"token_hash_algorithm"`
}

// AutoCertConfig holds automatic certificate provisioning settings.
type AutoCertConfig struct {
	Enabled  bool     `json:"enabled"`
	Domains  []string `json:"domains"`
	CacheDir string   `json:"cache_dir"`
}

// DefaultTLSAuthConfig returns sensible defaults.
func DefaultTLSAuthConfig() TLSAuthConfig {
	return TLSAuthConfig{
		TLS: TLSConfig{
			Enabled:       false,
			MinTLSVersion: TLSVersion12,
		},
		OAuth2: OAuth2Config{
			Enabled: false,
		},
		APIToken: APITokenConfig{
			Enabled:            false,
			DefaultRateLimit:   defaultTokenRateLimit,
			TokenHashAlgorithm: TokenHashSHA256,
		},
		AutoCert: AutoCertConfig{
			Enabled:  false,
			CacheDir: "/var/lib/chronicle/certs",
		},
	}
}

// --- TLS Provider ---

// TLSProvider manages TLS configuration and certificate rotation.
type TLSProvider struct {
	config    TLSConfig
	mu        sync.RWMutex
	tlsConfig *tls.Config
	clientCAs *x509.CertPool
	cert      *tls.Certificate
	stopCh    chan struct{}
	running   bool
}

// NewTLSProvider creates a new TLS provider.
func NewTLSProvider(cfg TLSConfig) *TLSProvider {
	return &TLSProvider{
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// BuildTLSConfig returns a *tls.Config with proper cipher suites and min version.
func (p *TLSProvider) BuildTLSConfig() (*tls.Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.config.Enabled {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: p.parseTLSVersion(),
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		},
		GetCertificate: p.getCertificate,
	}

	if p.config.CertFile != "" && p.config.KeyFile != "" {
		if err := p.loadCertificate(); err != nil {
			return nil, fmt.Errorf("loading certificate: %w", err)
		}
	}

	if p.config.RequireClientCert && p.config.ClientCAFile != "" {
		pool, err := p.loadClientCAs()
		if err != nil {
			return nil, fmt.Errorf("loading client CAs: %w", err)
		}
		p.clientCAs = pool
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	p.tlsConfig = tlsCfg
	return tlsCfg, nil
}

func (p *TLSProvider) parseTLSVersion() uint16 {
	switch p.config.MinTLSVersion {
	case TLSVersion13:
		return tls.VersionTLS13
	default:
		return tls.VersionTLS12
	}
}

func (p *TLSProvider) loadCertificate() error {
	cert, err := tls.LoadX509KeyPair(p.config.CertFile, p.config.KeyFile)
	if err != nil {
		return err
	}
	p.cert = &cert
	return nil
}

func (p *TLSProvider) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cert == nil {
		return nil, errors.New("no certificate loaded")
	}
	return p.cert, nil
}

func (p *TLSProvider) loadClientCAs() (*x509.CertPool, error) {
	data, err := os.ReadFile(p.config.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("reading client CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("failed to parse client CA certificates")
	}
	return pool, nil
}

// Start begins certificate file watching for rotation.
func (p *TLSProvider) Start() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.mu.Unlock()

	if p.config.CertFile != "" {
		go p.watchCertificates()
	}
}

// Stop stops certificate watching.
func (p *TLSProvider) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.running {
		return
	}
	p.running = false
	close(p.stopCh)
}

func (p *TLSProvider) watchCertificates() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.mu.Lock()
			_ = p.loadCertificate()
			p.mu.Unlock()
		}
	}
}

// --- JWKS Validator ---

// JWKSKey represents a single key from a JWKS endpoint.
type JWKSKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// JWKSResponse represents the JSON Web Key Set response.
type JWKSResponse struct {
	Keys []JWKSKey `json:"keys"`
}

// JWTClaims represents validated JWT claims.
type JWTClaims struct {
	Issuer    string   `json:"iss"`
	Subject   string   `json:"sub"`
	Audience  string   `json:"aud"`
	ExpiresAt int64    `json:"exp"`
	IssuedAt  int64    `json:"iat"`
	Scopes    []string `json:"scopes"`
	KeyID     string   `json:"-"`
}

// JWKSValidator validates JWT tokens using JWKS.
type JWKSValidator struct {
	config     OAuth2Config
	mu         sync.RWMutex
	keys       map[string]JWKSKey
	lastFetch  time.Time
	httpClient *http.Client
}

// NewJWKSValidator creates a new JWKS validator.
func NewJWKSValidator(cfg OAuth2Config) *JWKSValidator {
	return &JWKSValidator{
		config: cfg,
		keys:   make(map[string]JWKSKey),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ValidateToken validates a JWT token string and returns claims.
func (v *JWKSValidator) ValidateToken(tokenString string) (*JWTClaims, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid JWT format: expected 3 parts")
	}

	headerJSON, err := base64URLDecode(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, fmt.Errorf("parsing JWT header: %w", err)
	}

	payloadJSON, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT payload: %w", err)
	}
	var claims JWTClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("parsing JWT claims: %w", err)
	}
	claims.KeyID = header.Kid

	key, err := v.getKey(header.Kid)
	if err != nil {
		return nil, fmt.Errorf("fetching signing key: %w", err)
	}

	signingInput := parts[0] + "." + parts[1]
	signature, err := base64URLDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("decoding JWT signature: %w", err)
	}

	if err := v.verifySignature(header.Alg, signingInput, signature, key); err != nil {
		return nil, fmt.Errorf("signature verification failed: %w", err)
	}

	if err := v.validateClaims(&claims); err != nil {
		return nil, err
	}

	return &claims, nil
}

func (v *JWKSValidator) getKey(kid string) (JWKSKey, error) {
	v.mu.RLock()
	key, ok := v.keys[kid]
	needsRefresh := time.Since(v.lastFetch) > defaultJWKSCacheDuration
	v.mu.RUnlock()

	if ok && !needsRefresh {
		return key, nil
	}

	if err := v.fetchJWKS(); err != nil {
		if ok {
			return key, nil
		}
		return JWKSKey{}, fmt.Errorf("fetching JWKS: %w", err)
	}

	v.mu.RLock()
	defer v.mu.RUnlock()
	key, ok = v.keys[kid]
	if !ok {
		return JWKSKey{}, fmt.Errorf("key %q not found in JWKS", kid)
	}
	return key, nil
}

func (v *JWKSValidator) fetchJWKS() error {
	resp, err := v.httpClient.Get(v.config.JWKSURL)
	if err != nil {
		return fmt.Errorf("requesting JWKS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
	if err != nil {
		return fmt.Errorf("reading JWKS response: %w", err)
	}

	var jwks JWKSResponse
	if err := json.Unmarshal(body, &jwks); err != nil {
		return fmt.Errorf("parsing JWKS response: %w", err)
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.keys = make(map[string]JWKSKey, len(jwks.Keys))
	for _, k := range jwks.Keys {
		if k.Use == "" || k.Use == "sig" {
			v.keys[k.Kid] = k
		}
	}
	v.lastFetch = time.Now()
	return nil
}

func (v *JWKSValidator) verifySignature(alg, signingInput string, signature []byte, key JWKSKey) error {
	hash := sha256.Sum256([]byte(signingInput))

	switch alg {
	case "RS256":
		return v.verifyRSA(hash[:], signature, key)
	case "ES256":
		return v.verifyECDSA(hash[:], signature, key)
	default:
		return fmt.Errorf("unsupported algorithm: %s", alg)
	}
}

func (v *JWKSValidator) verifyRSA(hash, signature []byte, key JWKSKey) error {
	if key.Kty != "RSA" {
		return errors.New("key type mismatch: expected RSA")
	}
	nBytes, err := base64URLDecode(key.N)
	if err != nil {
		return fmt.Errorf("decoding RSA modulus: %w", err)
	}
	eBytes, err := base64URLDecode(key.E)
	if err != nil {
		return fmt.Errorf("decoding RSA exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := 0
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}

	pubKey := &rsa.PublicKey{N: n, E: e}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hash, signature)
}

func (v *JWKSValidator) verifyECDSA(hash, signature []byte, key JWKSKey) error {
	if key.Kty != "EC" {
		return errors.New("key type mismatch: expected EC")
	}
	xBytes, err := base64URLDecode(key.X)
	if err != nil {
		return fmt.Errorf("decoding EC X coordinate: %w", err)
	}
	yBytes, err := base64URLDecode(key.Y)
	if err != nil {
		return fmt.Errorf("decoding EC Y coordinate: %w", err)
	}

	pubKey := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}

	// ES256 signatures are r || s, each 32 bytes
	keySize := 32
	if len(signature) != 2*keySize {
		return errors.New("invalid ECDSA signature length")
	}
	r := new(big.Int).SetBytes(signature[:keySize])
	s := new(big.Int).SetBytes(signature[keySize:])

	if !ecdsa.Verify(pubKey, hash, r, s) {
		return errors.New("ECDSA signature verification failed")
	}
	return nil
}

func (v *JWKSValidator) validateClaims(claims *JWTClaims) error {
	now := time.Now().Unix()
	if claims.ExpiresAt > 0 && now > claims.ExpiresAt {
		return errors.New("token has expired")
	}
	if v.config.IssuerURL != "" && claims.Issuer != v.config.IssuerURL {
		return fmt.Errorf("invalid issuer: got %q, want %q", claims.Issuer, v.config.IssuerURL)
	}
	if v.config.Audience != "" && claims.Audience != v.config.Audience {
		return fmt.Errorf("invalid audience: got %q, want %q", claims.Audience, v.config.Audience)
	}
	return nil
}

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
					http.Error(w, "invalid API key: "+err.Error(), http.StatusUnauthorized)
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
					http.Error(w, "invalid bearer token: "+err.Error(), http.StatusUnauthorized)
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
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	result, err := m.tokenManager.CreateToken(req.Name, req.Scopes, req.RateLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
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
	json.NewEncoder(w).Encode(tokens)
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
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "revoked", "id": tokenID})
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
