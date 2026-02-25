package chronicle

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// PITRConfig configures point-in-time recovery and backup deduplication.
type PITRConfig struct {
	WALArchiveDir       string        `json:"wal_archive_dir"`
	WALArchiveInterval  time.Duration `json:"wal_archive_interval"`
	EnableDeduplication bool          `json:"enable_deduplication"`
	EnableEncryption    bool          `json:"enable_encryption"`
	EncryptionKeyPath   string        `json:"encryption_key_path"`
	RetentionDays       int           `json:"retention_days"`
	MaxSegmentSizeMB    int           `json:"max_segment_size_mb"`
	CheckpointInterval  time.Duration `json:"checkpoint_interval"`
}

// DefaultPITRConfig returns a PITRConfig with sensible production defaults.
func DefaultPITRConfig() PITRConfig {
	return PITRConfig{
		WALArchiveDir:       "/var/lib/chronicle/wal-archive",
		WALArchiveInterval:  30 * time.Second,
		EnableDeduplication: true,
		EnableEncryption:    false,
		EncryptionKeyPath:   "",
		RetentionDays:       7,
		MaxSegmentSizeMB:    64,
		CheckpointInterval:  15 * time.Minute,
	}
}

// ---------------------------------------------------------------------------
// Content-Addressable Storage
// ---------------------------------------------------------------------------

// ContentBlock represents a content-addressable storage block keyed by its
// SHA-256 hash.
type ContentBlock struct {
	Hash     string `json:"hash"`
	Data     []byte `json:"-"`
	Size     int64  `json:"size"`
	RefCount int64  `json:"ref_count"`
}

// ContentAddressableStore provides deduplication via content-addressable
// storage. Blocks are keyed by their SHA-256 digest and reference-counted
// so that unreferenced blocks can be garbage-collected.
type ContentAddressableStore struct {
	mu     sync.RWMutex
	blocks map[string]*ContentBlock
	size   int64
}

// NewContentAddressableStore creates an empty content-addressable store.
func NewContentAddressableStore() *ContentAddressableStore {
	return &ContentAddressableStore{
		blocks: make(map[string]*ContentBlock),
	}
}

// Put stores data and returns its SHA-256 hash. If the block already exists
// only the reference count is incremented.
func (s *ContentAddressableStore) Put(data []byte) (string, error) {
	if len(data) == 0 {
		return "", errors.New("pitr: cannot store empty block")
	}
	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	if blk, ok := s.blocks[hash]; ok {
		blk.RefCount++
		return hash, nil
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	s.blocks[hash] = &ContentBlock{
		Hash:     hash,
		Data:     cp,
		Size:     int64(len(cp)),
		RefCount: 1,
	}
	s.size += int64(len(cp))
	return hash, nil
}

// Get returns the data for the given hash.
func (s *ContentAddressableStore) Get(hash string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blk, ok := s.blocks[hash]
	if !ok {
		return nil, fmt.Errorf("pitr: block not found: %s", hash)
	}
	cp := make([]byte, len(blk.Data))
	copy(cp, blk.Data)
	return cp, nil
}

// Release decrements the reference count for the given hash.
func (s *ContentAddressableStore) Release(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if blk, ok := s.blocks[hash]; ok {
		blk.RefCount--
	}
}

// GC removes all blocks with a reference count ≤ 0 and returns the number
// of blocks collected.
func (s *ContentAddressableStore) GC() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	collected := 0
	for hash, blk := range s.blocks {
		if blk.RefCount <= 0 {
			s.size -= blk.Size
			delete(s.blocks, hash)
			collected++
		}
	}
	return collected
}

// Stats returns the number of blocks and total stored bytes.
func (s *ContentAddressableStore) Stats() (blocks int, totalBytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.blocks), s.size
}

// ---------------------------------------------------------------------------
// WAL Archiver
// ---------------------------------------------------------------------------

// WALSegmentInfo describes an archived WAL segment.
type WALSegmentInfo struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ArchivedAt time.Time `json:"archived_at"`
}

// WALArchiver manages archival and purging of WAL segments for PITR.
type WALArchiver struct {
	archiveDir string
	mu         sync.RWMutex
	segments   []WALSegmentInfo
}

// NewWALArchiver creates a WALArchiver that stores segments in archiveDir.
func NewWALArchiver(archiveDir string) (*WALArchiver, error) {
	if err := os.MkdirAll(archiveDir, 0o750); err != nil {
		return nil, fmt.Errorf("pitr: create archive dir: %w", err)
	}
	a := &WALArchiver{
		archiveDir: archiveDir,
	}
	if err := a.loadExistingSegments(); err != nil {
		return nil, err
	}
	return a, nil
}

// loadExistingSegments scans the archive directory for previously archived
// WAL segments.
func (a *WALArchiver) loadExistingSegments() error {
	entries, err := os.ReadDir(a.archiveDir)
	if err != nil {
		return fmt.Errorf("pitr: read archive dir: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.segments = a.segments[:0]
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		a.segments = append(a.segments, WALSegmentInfo{
			Name:       e.Name(),
			Path:       filepath.Join(a.archiveDir, e.Name()),
			Size:       info.Size(),
			ArchivedAt: info.ModTime(),
		})
	}
	sort.Slice(a.segments, func(i, j int) bool {
		return a.segments[i].ArchivedAt.Before(a.segments[j].ArchivedAt)
	})
	return nil
}

// Archive copies a WAL segment file into the archive directory.
func (a *WALArchiver) Archive(segmentPath string) (*WALSegmentInfo, error) {
	src, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("pitr: open segment: %w", err)
	}
	defer src.Close()

	stat, err := src.Stat()
	if err != nil {
		return nil, fmt.Errorf("pitr: stat segment: %w", err)
	}

	name := filepath.Base(segmentPath)
	dstPath := filepath.Join(a.archiveDir, name)

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return nil, fmt.Errorf("pitr: create archive file: %w", err)
	}

	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		os.Remove(dstPath)
		return nil, fmt.Errorf("pitr: copy segment: %w", err)
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		return nil, fmt.Errorf("pitr: sync archive file: %w", err)
	}
	dst.Close()

	seg := WALSegmentInfo{
		Name:       name,
		Path:       dstPath,
		Size:       stat.Size(),
		ArchivedAt: time.Now().UTC(),
	}

	a.mu.Lock()
	a.segments = append(a.segments, seg)
	a.mu.Unlock()

	return &seg, nil
}

// ListSegments returns all archived segments ordered by archive time.
func (a *WALArchiver) ListSegments() []WALSegmentInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]WALSegmentInfo, len(a.segments))
	copy(out, a.segments)
	return out
}

// PurgeOlderThan removes archived segments older than the given duration and
// returns the number of segments purged.
func (a *WALArchiver) PurgeOlderThan(d time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-d)

	a.mu.Lock()
	defer a.mu.Unlock()

	var kept []WALSegmentInfo
	purged := 0
	for _, seg := range a.segments {
		if seg.ArchivedAt.Before(cutoff) {
			if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
				kept = append(kept, seg)
				continue
			}
			purged++
		} else {
			kept = append(kept, seg)
		}
	}
	a.segments = kept
	return purged, nil
}

// ---------------------------------------------------------------------------
// Encrypted Writer / Reader (AES-256-GCM)
// ---------------------------------------------------------------------------

const (
	pitrAESKeySize   = 32 // AES-256
	pitrGCMNonceSize = 12
)

// EncryptedWriter wraps an io.Writer with AES-256-GCM encryption. Data is
// encrypted in discrete frames: each frame is prefixed with a random nonce
// followed by the ciphertext (including the GCM authentication tag).
type EncryptedWriter struct {
	w   io.Writer
	gcm cipher.AEAD
}

// NewEncryptedWriter creates an EncryptedWriter from a 32-byte AES key.
func NewEncryptedWriter(w io.Writer, key []byte) (*EncryptedWriter, error) {
	if len(key) != pitrAESKeySize {
		return nil, fmt.Errorf("pitr: encryption key must be %d bytes, got %d", pitrAESKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("pitr: create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("pitr: create gcm: %w", err)
	}
	return &EncryptedWriter{w: w, gcm: gcm}, nil
}

// Write encrypts p and writes the nonce+ciphertext frame to the underlying
// writer.
func (ew *EncryptedWriter) Write(p []byte) (int, error) {
	nonce := make([]byte, pitrGCMNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, fmt.Errorf("pitr: generate nonce: %w", err)
	}

	ciphertext := ew.gcm.Seal(nil, nonce, p, nil)

	// Frame: [nonce | ciphertext]
	frame := append(nonce, ciphertext...)
	n, err := ew.w.Write(frame)
	if err != nil {
		return 0, err
	}
	if n != len(frame) {
		return 0, io.ErrShortWrite
	}
	return len(p), nil
}

// EncryptedReader wraps an io.Reader and decrypts AES-256-GCM frames
// produced by EncryptedWriter.
type EncryptedReader struct {
	r   io.Reader
	gcm cipher.AEAD
}

// NewEncryptedReader creates an EncryptedReader from a 32-byte AES key.
func NewEncryptedReader(r io.Reader, key []byte) (*EncryptedReader, error) {
	if len(key) != pitrAESKeySize {
		return nil, fmt.Errorf("pitr: encryption key must be %d bytes, got %d", pitrAESKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("pitr: create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("pitr: create gcm: %w", err)
	}
	return &EncryptedReader{r: r, gcm: gcm}, nil
}

// Read reads a full encrypted frame from the underlying reader, decrypts it,
// and copies the plaintext into p.
func (er *EncryptedReader) Read(p []byte) (int, error) {
	// Read nonce
	nonce := make([]byte, pitrGCMNonceSize)
	if _, err := io.ReadFull(er.r, nonce); err != nil {
		return 0, err
	}

	// Read remaining ciphertext (len(p) + GCM overhead)
	overhead := er.gcm.Overhead()
	ct := make([]byte, len(p)+overhead)
	n, err := io.ReadFull(er.r, ct)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, err
	}
	ct = ct[:n]

	plaintext, err := er.gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return 0, fmt.Errorf("pitr: decrypt: %w", err)
	}
	copy(p, plaintext)
	return len(plaintext), nil
}

// loadEncryptionKey reads a 32-byte (hex-encoded or raw) key from path.
func loadEncryptionKey(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pitr: read key file: %w", err)
	}
	data = []byte(strings.TrimSpace(string(data)))

	// Try hex-encoded first (64 hex chars = 32 bytes).
	if len(data) == 64 {
		key, err := hex.DecodeString(string(data))
		if err == nil && len(key) == pitrAESKeySize {
			return key, nil
		}
	}
	if len(data) == pitrAESKeySize {
		return data, nil
	}
	return nil, fmt.Errorf("pitr: key file must contain 32 raw bytes or 64 hex characters")
}

// ---------------------------------------------------------------------------
// PITR Checkpoint
// ---------------------------------------------------------------------------

// PITRCheckpoint represents a consistent point-in-time snapshot.
type PITRCheckpoint struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	WALSegment string    `json:"wal_segment"`
	DataHashes []string  `json:"data_hashes"`
	PointCount int64     `json:"point_count"`
	SizeBytes  int64     `json:"size_bytes"`
}

// PITRStatus reports the current state of the PITR manager.
type PITRStatus struct {
	Running              bool      `json:"running"`
	CheckpointCount      int       `json:"checkpoint_count"`
	ArchivedSegments     int       `json:"archived_segments"`
	DedupBlocks          int       `json:"dedup_blocks"`
	DedupBytes           int64     `json:"dedup_bytes"`
	LastCheckpoint       time.Time `json:"last_checkpoint,omitempty"`
	LastArchive          time.Time `json:"last_archive,omitempty"`
	EncryptionEnabled    bool      `json:"encryption_enabled"`
	DeduplicationEnabled bool      `json:"deduplication_enabled"`
}

// PITRRestoreResult describes the outcome of a point-in-time restore.
type PITRRestoreResult struct {
	TargetTime       time.Time     `json:"target_time"`
	CheckpointUsed   string        `json:"checkpoint_used"`
	SegmentsReplayed int           `json:"segments_replayed"`
	PointsRestored   int64         `json:"points_restored"`
	Duration         time.Duration `json:"duration"`
}
