package chronicle

import (
	"bytes"
	"testing"
)

func TestEncryptor_EncryptDecrypt(t *testing.T) {
	enc, err := NewEncryptor(EncryptionConfig{
		Enabled:     true,
		KeyPassword: "test-password-123",
	})
	if err != nil {
		t.Fatalf("NewEncryptor failed: %v", err)
	}

	plaintext := []byte("hello world, this is secret data!")

	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if bytes.Equal(ciphertext, plaintext) {
		t.Error("ciphertext should not equal plaintext")
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("decrypted data does not match: got %s, want %s", decrypted, plaintext)
	}
}

func TestEncryptor_WithRawKey(t *testing.T) {
	key := make([]byte, EncryptionKeySize)
	for i := range key {
		key[i] = byte(i)
	}

	enc, err := NewEncryptorWithKey(key)
	if err != nil {
		t.Fatalf("NewEncryptorWithKey failed: %v", err)
	}

	plaintext := []byte("secret data")

	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("decrypted data does not match")
	}
}

func TestEncryptor_WithSalt(t *testing.T) {
	password := "my-secret-password"

	enc1, err := NewEncryptor(EncryptionConfig{
		Enabled:     true,
		KeyPassword: password,
	})
	if err != nil {
		t.Fatalf("NewEncryptor failed: %v", err)
	}

	plaintext := []byte("important data")

	ciphertext, err := enc1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Create new encryptor with same password and salt
	enc2, err := NewEncryptorWithSalt(password, enc1.Salt())
	if err != nil {
		t.Fatalf("NewEncryptorWithSalt failed: %v", err)
	}

	decrypted, err := enc2.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("decrypted data does not match")
	}
}

func TestEncryptor_InvalidKeySize(t *testing.T) {
	_, err := NewEncryptorWithKey([]byte("too-short"))
	if err == nil {
		t.Error("expected error for invalid key size")
	}
}

func TestEncryptor_InvalidCiphertext(t *testing.T) {
	enc, _ := NewEncryptor(EncryptionConfig{
		Enabled:     true,
		KeyPassword: "test",
	})

	_, err := enc.Decrypt([]byte("short"))
	if err == nil {
		t.Error("expected error for short ciphertext")
	}

	_, err = enc.Decrypt(make([]byte, 50)) // Wrong key
	if err == nil {
		t.Error("expected error for invalid ciphertext")
	}
}

func TestEncryptedHeader(t *testing.T) {
	salt := make([]byte, EncryptionSaltSize)
	for i := range salt {
		salt[i] = byte(i)
	}

	var buf bytes.Buffer
	if err := WriteEncryptedHeader(&buf, salt); err != nil {
		t.Fatalf("WriteEncryptedHeader failed: %v", err)
	}

	if buf.Len() != EncryptedHeaderSize {
		t.Errorf("unexpected header size: got %d, want %d", buf.Len(), EncryptedHeaderSize)
	}

	header, err := ReadEncryptedHeader(&buf)
	if err != nil {
		t.Fatalf("ReadEncryptedHeader failed: %v", err)
	}

	if header.Magic != MagicEncrypted {
		t.Error("magic mismatch")
	}

	if header.Version != 1 {
		t.Errorf("version mismatch: got %d, want 1", header.Version)
	}

	for i := range salt {
		if header.Salt[i] != salt[i] {
			t.Errorf("salt mismatch at index %d", i)
			break
		}
	}
}

func TestEncryptor_BlockEncryption(t *testing.T) {
	enc, _ := NewEncryptor(EncryptionConfig{
		Enabled:     true,
		KeyPassword: "test",
	})

	data := []byte("block data for partition 42")

	ciphertext, err := enc.EncryptBlock(data, 42)
	if err != nil {
		t.Fatalf("EncryptBlock failed: %v", err)
	}

	decrypted, err := enc.DecryptBlock(ciphertext)
	if err != nil {
		t.Fatalf("DecryptBlock failed: %v", err)
	}

	if !bytes.Equal(decrypted, data) {
		t.Error("block decryption mismatch")
	}
}

func TestEncryptor_Disabled(t *testing.T) {
	enc, err := NewEncryptor(EncryptionConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enc != nil {
		t.Error("expected nil encryptor when disabled")
	}
}

func TestEncryptor_NoKeyOrPassword(t *testing.T) {
	_, err := NewEncryptor(EncryptionConfig{Enabled: true})
	if err == nil {
		t.Error("expected error when no key or password provided")
	}
}
