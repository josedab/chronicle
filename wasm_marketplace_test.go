package chronicle

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"
)

func TestWASMMarketplace(t *testing.T) {
	runtime := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())
	marketplace := NewWASMMarketplace(runtime, DefaultWASMMarketplaceConfig())

	wasmBytes := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00} // minimal WASM header

	t.Run("publish plugin", func(t *testing.T) {
		manifest := WASMPluginManifest{
			Name:        "test-plugin",
			Version:     "1.0.0",
			Description: "A test plugin",
			Author:      "test",
			PluginType:  WASMPluginTransform,
			MaxMemoryMB: 32,
		}
		err := marketplace.PublishPlugin(manifest, wasmBytes, nil)
		if err != nil {
			t.Fatalf("publish failed: %v", err)
		}
	})

	t.Run("publish empty name", func(t *testing.T) {
		err := marketplace.PublishPlugin(WASMPluginManifest{}, wasmBytes, nil)
		if err == nil {
			t.Error("expected error for empty name")
		}
	})

	t.Run("publish empty wasm", func(t *testing.T) {
		err := marketplace.PublishPlugin(WASMPluginManifest{Name: "x"}, nil, nil)
		if err == nil {
			t.Error("expected error for empty WASM")
		}
	})

	t.Run("install plugin", func(t *testing.T) {
		err := marketplace.InstallPlugin("test-plugin")
		if err != nil {
			t.Fatalf("install failed: %v", err)
		}

		installed := marketplace.ListInstalled()
		if len(installed) != 1 {
			t.Errorf("expected 1 installed, got %d", len(installed))
		}
	})

	t.Run("install nonexistent", func(t *testing.T) {
		err := marketplace.InstallPlugin("nonexistent")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("enable disable plugin", func(t *testing.T) {
		err := marketplace.DisablePlugin("test-plugin")
		if err != nil {
			t.Fatal(err)
		}
		inst, _ := marketplace.GetPluginInfo("test-plugin")
		if inst.Enabled {
			t.Error("expected disabled")
		}

		err = marketplace.EnablePlugin("test-plugin")
		if err != nil {
			t.Fatal(err)
		}
		inst, _ = marketplace.GetPluginInfo("test-plugin")
		if !inst.Enabled {
			t.Error("expected enabled")
		}
	})

	t.Run("pin version", func(t *testing.T) {
		err := marketplace.PinVersion("test-plugin")
		if err != nil {
			t.Fatal(err)
		}
		inst, _ := marketplace.GetPluginInfo("test-plugin")
		if !inst.Pinned {
			t.Error("expected pinned")
		}
	})

	t.Run("hot reload pinned fails", func(t *testing.T) {
		err := marketplace.HotReload("test-plugin", wasmBytes)
		if err == nil {
			t.Error("expected error for pinned plugin")
		}
	})

	t.Run("uninstall plugin", func(t *testing.T) {
		err := marketplace.UninstallPlugin("test-plugin")
		if err != nil {
			t.Fatalf("uninstall failed: %v", err)
		}
		installed := marketplace.ListInstalled()
		if len(installed) != 0 {
			t.Errorf("expected 0 installed, got %d", len(installed))
		}
	})

	t.Run("uninstall nonexistent", func(t *testing.T) {
		err := marketplace.UninstallPlugin("nonexistent")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("list registry", func(t *testing.T) {
		entries := marketplace.ListRegistry()
		if len(entries) == 0 {
			t.Error("expected entries in registry")
		}
		// WASM bytes should not be included in listing
		for _, e := range entries {
			if len(e.WASMBytes) > 0 {
				t.Error("WASM bytes should be excluded from listing")
			}
		}
	})

	t.Run("search plugins", func(t *testing.T) {
		results := marketplace.SearchPlugins("test", "")
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}

		results = marketplace.SearchPlugins("", WASMPluginTransform)
		if len(results) != 1 {
			t.Errorf("expected 1 transform plugin, got %d", len(results))
		}

		results = marketplace.SearchPlugins("nonexistent_query", "")
		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})

	t.Run("dependency check", func(t *testing.T) {
		manifest := WASMPluginManifest{
			Name:       "dependent-plugin",
			Version:    "1.0.0",
			PluginType: WASMPluginFilter,
			Dependencies: []PluginDependency{
				{Name: "required-dep", MinVersion: "1.0.0", Optional: false},
			},
		}
		marketplace.PublishPlugin(manifest, wasmBytes, nil)
		err := marketplace.InstallPlugin("dependent-plugin")
		if err == nil {
			t.Error("expected error for missing dependency")
		}
	})
}

func TestPluginSignatureVerification(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	data := []byte("test wasm data")
	sig := ed25519.Sign(priv, data)

	t.Run("valid signature", func(t *testing.T) {
		config := DefaultWASMMarketplaceConfig()
		config.VerifySignatures = true
		marketplace := NewWASMMarketplace(nil, config)

		pluginSig := &PluginSignature{
			PublicKeyHex: hex.EncodeToString(pub),
			SignatureHex: hex.EncodeToString(sig),
		}

		err := marketplace.PublishPlugin(
			WASMPluginManifest{Name: "signed-plugin", PluginType: WASMPluginAlert},
			data,
			pluginSig,
		)
		if err != nil {
			t.Fatalf("publish with valid sig should succeed: %v", err)
		}

		entries := marketplace.ListRegistry()
		for _, e := range entries {
			if e.Manifest.Name == "signed-plugin" && !e.Verified {
				t.Error("expected verified flag")
			}
		}
	})

	t.Run("invalid signature", func(t *testing.T) {
		config := DefaultWASMMarketplaceConfig()
		config.VerifySignatures = true
		marketplace := NewWASMMarketplace(nil, config)

		pluginSig := &PluginSignature{
			PublicKeyHex: hex.EncodeToString(pub),
			SignatureHex: hex.EncodeToString([]byte("invalid")),
		}

		err := marketplace.PublishPlugin(
			WASMPluginManifest{Name: "bad-sig", PluginType: WASMPluginAlert},
			data,
			pluginSig,
		)
		// Should succeed but not be verified
		if err != nil {
			t.Fatalf("publish should succeed even with bad sig: %v", err)
		}
	})

	t.Run("add trusted key", func(t *testing.T) {
		marketplace := NewWASMMarketplace(nil, DefaultWASMMarketplaceConfig())
		err := marketplace.AddTrustedKey("test", hex.EncodeToString(pub))
		if err != nil {
			t.Fatalf("add key failed: %v", err)
		}
	})

	t.Run("add invalid key", func(t *testing.T) {
		marketplace := NewWASMMarketplace(nil, DefaultWASMMarketplaceConfig())
		err := marketplace.AddTrustedKey("test", "invalid-hex")
		if err == nil {
			t.Error("expected error for invalid hex")
		}
		err = marketplace.AddTrustedKey("test", hex.EncodeToString([]byte{1, 2, 3}))
		if err == nil {
			t.Error("expected error for wrong key size")
		}
	})
}

func TestHotReload(t *testing.T) {
	config := DefaultWASMMarketplaceConfig()
	config.EnableHotReload = true
	marketplace := NewWASMMarketplace(nil, config)

	wasmBytes := []byte{0x00, 0x61, 0x73, 0x6d}
	marketplace.PublishPlugin(WASMPluginManifest{Name: "reload-plugin", PluginType: WASMPluginTransform}, wasmBytes, nil)
	marketplace.InstallPlugin("reload-plugin")

	newWasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x02}
	err := marketplace.HotReload("reload-plugin", newWasm)
	if err != nil {
		t.Fatalf("hot reload failed: %v", err)
	}

	// Disabled hot reload
	config2 := DefaultWASMMarketplaceConfig()
	config2.EnableHotReload = false
	marketplace2 := NewWASMMarketplace(nil, config2)
	err = marketplace2.HotReload("x", newWasm)
	if err == nil {
		t.Error("expected error when hot reload disabled")
	}
}
