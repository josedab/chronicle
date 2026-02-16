package chronicle

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// helper: generate a self-signed CA + leaf cert/key pair and write PEM files.
func generateTestCerts(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()

	// CA key and cert
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caCertDER)

	caFile = filepath.Join(dir, "ca.pem")
	writePEM(t, caFile, "CERTIFICATE", caCertDER)

	// Leaf key and cert signed by CA
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating leaf cert: %v", err)
	}

	certFile = filepath.Join(dir, "cert.pem")
	writePEM(t, certFile, "CERTIFICATE", leafCertDER)

	keyFile = filepath.Join(dir, "key.pem")
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshaling leaf key: %v", err)
	}
	writePEM(t, keyFile, "EC PRIVATE KEY", leafKeyDER)

	return certFile, keyFile, caFile
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}); err != nil {
		t.Fatalf("encoding PEM %s: %v", path, err)
	}
}

// --- Tests ---

func TestDefaultTLSAuthConfig(t *testing.T) {
	cfg := DefaultTLSAuthConfig()

	if cfg.TLS.Enabled {
		t.Error("TLS should be disabled by default")
	}
	if cfg.TLS.MinTLSVersion != TLSVersion12 {
		t.Errorf("MinTLSVersion = %q, want %q", cfg.TLS.MinTLSVersion, TLSVersion12)
	}
	if cfg.OAuth2.Enabled {
		t.Error("OAuth2 should be disabled by default")
	}
	if cfg.APIToken.Enabled {
		t.Error("APIToken should be disabled by default")
	}
	if cfg.APIToken.DefaultRateLimit != defaultTokenRateLimit {
		t.Errorf("DefaultRateLimit = %v, want %v", cfg.APIToken.DefaultRateLimit, defaultTokenRateLimit)
	}
	if cfg.APIToken.TokenHashAlgorithm != TokenHashSHA256 {
		t.Errorf("TokenHashAlgorithm = %q, want %q", cfg.APIToken.TokenHashAlgorithm, TokenHashSHA256)
	}
	if cfg.AutoCert.Enabled {
		t.Error("AutoCert should be disabled by default")
	}
	if cfg.AutoCert.CacheDir != "/var/lib/chronicle/certs" {
		t.Errorf("CacheDir = %q, want %q", cfg.AutoCert.CacheDir, "/var/lib/chronicle/certs")
	}
}

func TestTLSProvider_BuildTLSConfig(t *testing.T) {
	t.Run("disabled returns nil", func(t *testing.T) {
		p := NewTLSProvider(TLSConfig{Enabled: false})
		tlsCfg, err := p.BuildTLSConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tlsCfg != nil {
			t.Error("expected nil tls.Config when TLS disabled")
		}
	})

	t.Run("enabled with certs", func(t *testing.T) {
		dir := t.TempDir()
		certFile, keyFile, _ := generateTestCerts(t, dir)

		p := NewTLSProvider(TLSConfig{
			Enabled:       true,
			CertFile:      certFile,
			KeyFile:        keyFile,
			MinTLSVersion: TLSVersion12,
		})
		tlsCfg, err := p.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig failed: %v", err)
		}
		if tlsCfg == nil {
			t.Fatal("expected non-nil tls.Config")
		}
		if tlsCfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %d, want %d", tlsCfg.MinVersion, tls.VersionTLS12)
		}
		if len(tlsCfg.CipherSuites) == 0 {
			t.Error("expected cipher suites to be set")
		}
		if tlsCfg.GetCertificate == nil {
			t.Error("expected GetCertificate to be set")
		}
	})

	t.Run("TLS 1.3 min version", func(t *testing.T) {
		dir := t.TempDir()
		certFile, keyFile, _ := generateTestCerts(t, dir)

		p := NewTLSProvider(TLSConfig{
			Enabled:       true,
			CertFile:      certFile,
			KeyFile:        keyFile,
			MinTLSVersion: TLSVersion13,
		})
		tlsCfg, err := p.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig failed: %v", err)
		}
		if tlsCfg.MinVersion != tls.VersionTLS13 {
			t.Errorf("MinVersion = %d, want %d", tlsCfg.MinVersion, tls.VersionTLS13)
		}
	})

	t.Run("mTLS with client CA", func(t *testing.T) {
		dir := t.TempDir()
		certFile, keyFile, caFile := generateTestCerts(t, dir)

		p := NewTLSProvider(TLSConfig{
			Enabled:           true,
			CertFile:          certFile,
			KeyFile:            keyFile,
			ClientCAFile:      caFile,
			MinTLSVersion:     TLSVersion12,
			RequireClientCert: true,
		})
		tlsCfg, err := p.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig failed: %v", err)
		}
		if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", tlsCfg.ClientAuth)
		}
		if tlsCfg.ClientCAs == nil {
			t.Error("expected ClientCAs to be set")
		}
	})

	t.Run("invalid cert path", func(t *testing.T) {
		p := NewTLSProvider(TLSConfig{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		})
		_, err := p.BuildTLSConfig()
		if err == nil {
			t.Error("expected error for invalid cert path")
		}
	})
}

func TestAPITokenManager_CreateToken(t *testing.T) {
	mgr := NewAPITokenManager(APITokenConfig{
		Enabled:            true,
		DefaultRateLimit:   100,
		TokenHashAlgorithm: TokenHashSHA256,
	})

	result, err := mgr.CreateToken("test-token", []string{"read", "write"}, 50)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}
	if result.Token == "" {
		t.Error("expected non-empty token string")
	}
	if result.Info.ID == "" {
		t.Error("expected non-empty token ID")
	}
	if result.Info.Name != "test-token" {
		t.Errorf("Name = %q, want %q", result.Info.Name, "test-token")
	}
	if result.Info.RateLimit != 50 {
		t.Errorf("RateLimit = %v, want 50", result.Info.RateLimit)
	}
	if len(result.Info.Scopes) != 2 {
		t.Errorf("Scopes length = %d, want 2", len(result.Info.Scopes))
	}
	if result.Info.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}

	// Default rate limit when 0 is provided
	result2, err := mgr.CreateToken("default-rate", nil, 0)
	if err != nil {
		t.Fatalf("CreateToken with default rate failed: %v", err)
	}
	if result2.Info.RateLimit != 100 {
		t.Errorf("default RateLimit = %v, want 100", result2.Info.RateLimit)
	}

	// Empty name should fail
	_, err = mgr.CreateToken("", nil, 10)
	if err == nil {
		t.Error("expected error for empty token name")
	}
}

func TestAPITokenManager_ValidateToken(t *testing.T) {
	mgr := NewAPITokenManager(APITokenConfig{
		Enabled:            true,
		DefaultRateLimit:   100,
		TokenHashAlgorithm: TokenHashSHA256,
	})

	result, err := mgr.CreateToken("valid-token", []string{"read"}, 50)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	// Valid token
	info, err := mgr.ValidateToken(result.Token)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}
	if info.ID != result.Info.ID {
		t.Errorf("ID = %q, want %q", info.ID, result.Info.ID)
	}

	// Invalid token
	_, err = mgr.ValidateToken("bogus-token-string")
	if err == nil {
		t.Error("expected error for invalid token")
	}
}

func TestAPITokenManager_RevokeToken(t *testing.T) {
	mgr := NewAPITokenManager(APITokenConfig{
		Enabled:            true,
		DefaultRateLimit:   100,
		TokenHashAlgorithm: TokenHashSHA256,
	})

	result, err := mgr.CreateToken("revoke-me", []string{"admin"}, 10)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	// Validate before revoke
	_, err = mgr.ValidateToken(result.Token)
	if err != nil {
		t.Fatalf("ValidateToken before revoke failed: %v", err)
	}

	// Revoke
	if err := mgr.RevokeToken(result.Info.ID); err != nil {
		t.Fatalf("RevokeToken failed: %v", err)
	}

	// Validate after revoke should fail
	_, err = mgr.ValidateToken(result.Token)
	if err == nil {
		t.Error("expected error for revoked token")
	}

	// Revoke nonexistent token
	err = mgr.RevokeToken("nonexistent-id")
	if err == nil {
		t.Error("expected error for nonexistent token ID")
	}
}

func TestAPITokenManager_ListTokens(t *testing.T) {
	mgr := NewAPITokenManager(APITokenConfig{
		Enabled:            true,
		DefaultRateLimit:   100,
		TokenHashAlgorithm: TokenHashSHA256,
	})

	// Empty list initially
	tokens := mgr.ListTokens()
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens, got %d", len(tokens))
	}

	// Create tokens
	r1, _ := mgr.CreateToken("token-1", []string{"read"}, 10)
	mgr.CreateToken("token-2", []string{"write"}, 20)
	mgr.CreateToken("token-3", []string{"admin"}, 30)

	tokens = mgr.ListTokens()
	if len(tokens) != 3 {
		t.Errorf("expected 3 tokens, got %d", len(tokens))
	}

	// Token hashes should be cleared in listing
	for _, tok := range tokens {
		if tok.TokenHash != "" {
			t.Error("ListTokens should not return token hashes")
		}
	}

	// Revoke one, list should return 2
	mgr.RevokeToken(r1.Info.ID)
	tokens = mgr.ListTokens()
	if len(tokens) != 2 {
		t.Errorf("expected 2 tokens after revoke, got %d", len(tokens))
	}
}

func TestTokenBucketLimiter(t *testing.T) {
	limiter := NewTokenBucketLimiter()

	tokenID := "test-bucket"
	rate := 10.0
	burst := 5

	// Should allow up to burst requests immediately
	for i := 0; i < burst; i++ {
		if !limiter.Allow(tokenID, rate, burst) {
			t.Errorf("request %d should be allowed within burst", i)
		}
	}

	// Next request should be denied (bucket exhausted)
	if limiter.Allow(tokenID, rate, burst) {
		t.Error("request beyond burst should be denied")
	}

	// After waiting, tokens should refill
	time.Sleep(200 * time.Millisecond)
	if !limiter.Allow(tokenID, rate, burst) {
		t.Error("request after refill should be allowed")
	}
}

func TestTokenBucketLimiter_StartStop(t *testing.T) {
	limiter := NewTokenBucketLimiter()
	limiter.Start()
	// Double start should be safe
	limiter.Start()
	limiter.Stop()
	// Double stop should be safe
	limiter.Stop()
}

func TestAuthMiddleware(t *testing.T) {
	tokenMgr := NewAPITokenManager(APITokenConfig{
		Enabled:            true,
		DefaultRateLimit:   100,
		TokenHashAlgorithm: TokenHashSHA256,
	})
	limiter := NewTokenBucketLimiter()
	jwks := NewJWKSValidator(OAuth2Config{Enabled: false})

	cfg := TLSAuthConfig{
		APIToken: APITokenConfig{Enabled: true},
		OAuth2:   OAuth2Config{Enabled: false},
	}
	mw := NewAuthMiddleware(jwks, tokenMgr, limiter, cfg)

	handler := mw.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	// Create a valid token
	result, err := tokenMgr.CreateToken("mw-test", []string{"read"}, 100)
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	t.Run("valid API key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", result.Token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("invalid API key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", "invalid-key")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("no auth header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("auth disabled passes through", func(t *testing.T) {
		disabledCfg := TLSAuthConfig{
			APIToken: APITokenConfig{Enabled: false},
			OAuth2:   OAuth2Config{Enabled: false},
		}
		disabledMW := NewAuthMiddleware(jwks, tokenMgr, limiter, disabledCfg)
		h := disabledMW.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d (auth disabled)", rec.Code, http.StatusOK)
		}
	})

	t.Run("revoked token rejected", func(t *testing.T) {
		res2, _ := tokenMgr.CreateToken("revoke-mw", []string{"read"}, 100)
		tokenMgr.RevokeToken(res2.Info.ID)

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", res2.Token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d for revoked token", rec.Code, http.StatusUnauthorized)
		}
	})
}

func TestNewAuthManager(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	authCfg := DefaultTLSAuthConfig()
	mgr := NewAuthManager(db, authCfg)
	if mgr == nil {
		t.Fatal("expected non-nil AuthManager")
	}
	if mgr.tlsProvider == nil {
		t.Error("expected tlsProvider to be set")
	}
	if mgr.jwksValid == nil {
		t.Error("expected jwksValidator to be set")
	}
	if mgr.tokenManager == nil {
		t.Error("expected tokenManager to be set")
	}
	if mgr.rateLimiter == nil {
		t.Error("expected rateLimiter to be set")
	}
	if mgr.middleware == nil {
		t.Error("expected middleware to be set")
	}
	if mgr.Middleware() == nil {
		t.Error("Middleware() should return non-nil")
	}
}

func TestAuthManager_StartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	authCfg := DefaultTLSAuthConfig()
	mgr := NewAuthManager(db, authCfg)

	// Start
	mgr.Start()
	mgr.mu.RLock()
	if !mgr.running {
		t.Error("expected running = true after Start")
	}
	mgr.mu.RUnlock()

	// Double start should be safe
	mgr.Start()

	// Stop
	mgr.Stop()
	mgr.mu.RLock()
	if mgr.running {
		t.Error("expected running = false after Stop")
	}
	mgr.mu.RUnlock()

	// Double stop should be safe
	mgr.Stop()
}

// Suppress unused import warnings for rsa - used indirectly via cert generation.
var _ = (*rsa.PublicKey)(nil)
