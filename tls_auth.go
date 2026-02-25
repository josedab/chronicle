package chronicle

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
			_ = p.loadCertificate() //nolint:errcheck // best-effort certificate reload
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
	if claims.ExpiresAt == 0 {
		return errors.New("token must have an expiration time")
	}
	if now > claims.ExpiresAt {
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
