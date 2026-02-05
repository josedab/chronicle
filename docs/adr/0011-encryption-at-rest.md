# ADR-0011: AES-256-GCM Encryption at Rest with PBKDF2 Key Derivation

## Status

Accepted

## Context

Chronicle targets edge deployments where physical device security cannot be guaranteed:

1. **Edge device theft**: Devices deployed in remote locations may be physically compromised
2. **Compliance requirements**: Regulations like GDPR, HIPAA, and SOC2 require data encryption at rest
3. **Multi-tenant environments**: Shared infrastructure requires isolation guarantees
4. **Air-gapped deployments**: Edge devices may not have network access to external key management systems

We evaluated several encryption approaches:

- **External KMS integration** (AWS KMS, HashiCorp Vault): Strong key management but requires network connectivity and adds latency
- **Hardware security modules**: Strongest guarantees but not available on commodity edge hardware
- **Software encryption with password-derived keys**: Self-contained, portable, works offline
- **Filesystem-level encryption** (LUKS, BitLocker): OS-dependent, requires privileged setup

Key considerations:
- Edge devices often lack TPM or HSM hardware
- Network connectivity may be intermittent or absent
- Deployment simplicity is critical (no external dependencies)
- Must protect both at-rest data and backup files

## Decision

Chronicle implements **application-level encryption at rest** using:

### Key Derivation

```go
type EncryptionConfig struct {
    Enabled     bool
    Key         []byte    // 32 bytes for AES-256 (direct key)
    KeyPassword string    // Alternative: derive key from password
}
```

When `KeyPassword` is provided, the key is derived using PBKDF2:

```go
func deriveKey(password string, salt []byte) []byte {
    return pbkdf2.Key(
        []byte(password),
        salt,
        100000,        // 100K iterations (OWASP recommendation)
        32,            // 256-bit key
        sha256.New,
    )
}
```

### Encryption Algorithm

**AES-256-GCM** (Galois/Counter Mode):
- 256-bit key strength
- Authenticated encryption (confidentiality + integrity)
- 12-byte random nonce per encryption operation
- 16-byte authentication tag

### Encryption Scope

| Component | Encrypted | Rationale |
|-----------|-----------|-----------|
| Partition data | Yes | Contains all metric values |
| Index metadata | Yes | Contains metric names and tag values |
| WAL entries | Yes | Contains recent writes |
| Configuration | No | Needed to initialize encryption |
| File headers | No | Contains version, magic bytes |

### Implementation

```go
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
    nonce := make([]byte, 12)
    if _, err := rand.Read(nonce); err != nil {
        return nil, err
    }

    ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
    return ciphertext, nil
}

func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
    nonce := ciphertext[:12]
    data := ciphertext[12:]
    return e.gcm.Open(nil, nonce, data, nil)
}
```

### Salt Storage

The 32-byte salt for PBKDF2 is stored in the database file header:
- Generated randomly on database creation
- Persisted unencrypted (salt is not secret)
- Read at startup for key derivation

## Consequences

### Positive

- **Self-contained security**: No external KMS required, works offline
- **Portable encryption**: Encrypted files can be copied/backed up; same password decrypts anywhere
- **Strong cryptography**: AES-256-GCM is NIST-approved, provides authenticated encryption
- **Brute-force resistance**: 100K PBKDF2 iterations make password guessing expensive
- **Integrity protection**: GCM authentication tag detects tampering
- **Simple key management**: Password-based keys are human-manageable

### Negative

- **Password strength dependency**: Security is only as strong as the chosen password
- **No key rotation without re-encryption**: Changing the key requires rewriting all data
- **Performance overhead**: Encryption adds ~5-10% CPU overhead for writes
- **Memory pressure**: Decryption buffers required during reads
- **No hardware acceleration guarantee**: Software-only, though Go uses AES-NI when available

### Security Considerations

**Threats mitigated**:
- Physical device theft (data unreadable without password)
- Unauthorized file access (even root cannot read plaintext)
- Backup exposure (backups are encrypted)
- Man-in-the-middle on disk (authentication tag validates integrity)

**Threats NOT mitigated**:
- Memory dumps (plaintext exists in memory during processing)
- Key logging (password capture at entry time)
- Weak passwords (user responsibility)
- Side-channel attacks (software implementation)

### Key Management Recommendations

For production deployments:

1. **Use strong passwords**: Minimum 16 characters, high entropy
2. **Consider direct keys**: For programmatic use, provide 32-byte keys directly
3. **Secure key distribution**: Use secure channels for initial key provisioning
4. **Document recovery procedures**: Ensure password recovery processes exist

### Enabled

- Compliance with data protection regulations
- Secure edge deployments without network dependencies
- Encrypted backups without additional tooling
- Multi-tenant isolation with per-database encryption

### Prevented

- Key rotation without downtime
- Hardware-backed key protection
- Centralized key management integration
- Recovery without password (by design)
