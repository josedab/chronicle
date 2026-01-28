package chronicle

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	"golang.org/x/crypto/pbkdf2"
)

const (
	// EncryptionNonceSize is the nonce size for AES-GCM
	EncryptionNonceSize = 12
	// EncryptionSaltSize is the salt size for key derivation
	EncryptionSaltSize = 32
	// EncryptionKeySize is the AES-256 key size
	EncryptionKeySize = 32
	// PBKDF2Iterations is the number of iterations for key derivation
	PBKDF2Iterations = 100000
)

// EncryptionConfig configures encryption at rest.
type EncryptionConfig struct {
	// Enabled turns on encryption for data files
	Enabled bool
	// Key is the encryption key (must be 32 bytes for AES-256)
	// If empty, KeyPassword is used to derive a key
	Key []byte
	// KeyPassword is used to derive the encryption key via PBKDF2
	KeyPassword string
}

// Encryptor provides encryption/decryption for data blocks.
type Encryptor struct {
	gcm  cipher.AEAD
	salt []byte
}

// NewEncryptor creates a new encryptor from a key or password.
func NewEncryptor(cfg EncryptionConfig) (*Encryptor, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	var key []byte
	var salt []byte

	if len(cfg.Key) > 0 {
		if len(cfg.Key) != EncryptionKeySize {
			return nil, errors.New("encryption key must be 32 bytes for AES-256")
		}
		key = cfg.Key
		salt = make([]byte, EncryptionSaltSize)
		if _, err := rand.Read(salt); err != nil {
			return nil, err
		}
	} else if cfg.KeyPassword != "" {
		salt = make([]byte, EncryptionSaltSize)
		if _, err := rand.Read(salt); err != nil {
			return nil, err
		}
		key = pbkdf2.Key([]byte(cfg.KeyPassword), salt, PBKDF2Iterations, EncryptionKeySize, sha256.New)
	} else {
		return nil, errors.New("encryption enabled but no key or password provided")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &Encryptor{gcm: gcm, salt: salt}, nil
}

// NewEncryptorWithSalt creates an encryptor using an existing salt (for decryption).
func NewEncryptorWithSalt(password string, salt []byte) (*Encryptor, error) {
	if len(salt) != EncryptionSaltSize {
		return nil, errors.New("invalid salt size")
	}

	key := pbkdf2.Key([]byte(password), salt, PBKDF2Iterations, EncryptionKeySize, sha256.New)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &Encryptor{gcm: gcm, salt: salt}, nil
}

// NewEncryptorWithKey creates an encryptor with a raw key.
func NewEncryptorWithKey(key []byte) (*Encryptor, error) {
	if len(key) != EncryptionKeySize {
		return nil, errors.New("encryption key must be 32 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &Encryptor{gcm: gcm}, nil
}

// Salt returns the salt used for key derivation.
func (e *Encryptor) Salt() []byte {
	return e.salt
}

// Encrypt encrypts plaintext and returns ciphertext with prepended nonce.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, EncryptionNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Prepend nonce to ciphertext
	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts ciphertext (with prepended nonce) and returns plaintext.
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < EncryptionNonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce := ciphertext[:EncryptionNonceSize]
	ciphertext = ciphertext[EncryptionNonceSize:]

	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// EncryptedHeader is written to encrypted files.
type EncryptedHeader struct {
	Magic   [4]byte // "CENC"
	Version byte
	Salt    [EncryptionSaltSize]byte
}

// MagicEncrypted is the magic bytes for encrypted files.
var MagicEncrypted = [4]byte{'C', 'E', 'N', 'C'}

// EncryptedHeaderSize is the size of the encrypted file header.
const EncryptedHeaderSize = 4 + 1 + EncryptionSaltSize

// WriteEncryptedHeader writes the encryption header to a writer.
func WriteEncryptedHeader(w io.Writer, salt []byte) error {
	header := EncryptedHeader{
		Magic:   MagicEncrypted,
		Version: 1,
	}
	copy(header.Salt[:], salt)

	buf := make([]byte, EncryptedHeaderSize)
	copy(buf[0:4], header.Magic[:])
	buf[4] = header.Version
	copy(buf[5:], header.Salt[:])

	_, err := w.Write(buf)
	return err
}

// ReadEncryptedHeader reads the encryption header from a reader.
func ReadEncryptedHeader(r io.Reader) (*EncryptedHeader, error) {
	buf := make([]byte, EncryptedHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	header := &EncryptedHeader{
		Version: buf[4],
	}
	copy(header.Magic[:], buf[0:4])
	copy(header.Salt[:], buf[5:])

	if header.Magic != MagicEncrypted {
		return nil, errors.New("invalid encrypted file magic")
	}

	return header, nil
}

// EncryptBlock encrypts a data block. The blockIndex parameter is retained for API
// compatibility but is not used - a fully random nonce is generated for security.
// The nonce is prepended to the ciphertext and extracted during decryption.
func (e *Encryptor) EncryptBlock(data []byte, blockIndex uint64) ([]byte, error) {
	// Generate fully random nonce for GCM security (nonces must never repeat)
	nonce := make([]byte, EncryptionNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	ciphertext := e.gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// DecryptBlock decrypts a data block.
func (e *Encryptor) DecryptBlock(ciphertext []byte) ([]byte, error) {
	return e.Decrypt(ciphertext)
}
