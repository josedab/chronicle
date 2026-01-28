package chronicle

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// WAL is a write-ahead log for crash recovery.
type WAL struct {
	path         string
	file         *os.File
	mu           sync.Mutex
	writer       *bufio.Writer
	syncInterval time.Duration
	closeCh      chan struct{}
	maxSize      int64
	retain       int

	// syncErrors tracks consecutive sync errors for monitoring
	syncErrors   int
	onSyncError  func(error) // optional callback for sync errors
}

// WALOption configures a WAL instance.
type WALOption func(*WAL)

// WithSyncErrorCallback sets a callback for sync errors.
func WithSyncErrorCallback(fn func(error)) WALOption {
	return func(w *WAL) {
		w.onSyncError = fn
	}
}

// NewWAL creates or opens a WAL file.
func NewWAL(path string, syncInterval time.Duration, maxSize int64, retain int, opts ...WALOption) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		path:         path,
		file:         file,
		writer:       bufio.NewWriter(file),
		syncInterval: syncInterval,
		closeCh:      make(chan struct{}),
		maxSize:      maxSize,
		retain:       retain,
	}

	for _, opt := range opts {
		opt(wal)
	}

	go wal.syncLoop()

	return wal, nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	close(w.closeCh)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

// Write appends points to the WAL.
func (w *WAL) Write(points []Point) error {
	if len(points) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotateIfNeeded(); err != nil {
		return err
	}

	payload, err := encodePoints(points)
	if err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(payload))); err != nil {
		return err
	}
	if _, err := w.writer.Write(payload); err != nil {
		return err
	}

	return w.writer.Flush()
}

// ReadAll reads all points from the WAL.
func (w *WAL) ReadAll() ([]Point, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(w.file)
	var out []Point

	for {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if length == 0 {
			continue
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		points, err := decodePoints(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, points...)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return out, nil
}

// Reset truncates the WAL after recovery.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer = bufio.NewWriter(w.file)
	return nil
}

func (w *WAL) rotateIfNeeded() error {
	if w.maxSize <= 0 {
		return nil
	}
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < w.maxSize {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	rotated := w.path + "." + time.Now().Format("20060102T150405")
	if err := os.Rename(w.path, rotated); err != nil {
		return err
	}

	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.writer = bufio.NewWriter(file)

	if w.retain > 0 {
		_ = w.cleanupRotations()
	}
	return nil
}

func (w *WAL) cleanupRotations() error {
	files, err := filepath.Glob(w.path + ".*")
	if err != nil {
		return err
	}
	if len(files) <= w.retain {
		return nil
	}
	sort.Strings(files)
	excess := len(files) - w.retain
	for i := 0; i < excess; i++ {
		_ = os.Remove(files[i])
	}
	return nil
}

func (w *WAL) syncLoop() {
	if w.syncInterval <= 0 {
		return
	}

	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			return
		case <-ticker.C:
			w.mu.Lock()
			var syncErr *WALSyncError
			flushErr := w.writer.Flush()
			fileErr := w.file.Sync()

			if flushErr != nil || fileErr != nil {
				syncErr = &WALSyncError{FlushErr: flushErr, SyncErr: fileErr}
				w.syncErrors++

				// Log the error
				log.Printf("chronicle: WAL sync error (count=%d): %v", w.syncErrors, syncErr)

				// Call error callback if set
				if w.onSyncError != nil {
					w.onSyncError(syncErr)
				}
			} else {
				// Reset error counter on success
				w.syncErrors = 0
			}
			w.mu.Unlock()
		}
	}
}

// Position returns the current WAL position (file size).
func (w *WAL) Position() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush to get accurate position
	_ = w.writer.Flush()

	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// ReadRange reads WAL entries between two positions.
func (w *WAL) ReadRange(start, end int64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if start < 0 || end < start {
		return nil, errors.New("invalid range")
	}

	// Flush pending writes
	_ = w.writer.Flush()

	// Read the range
	size := end - start
	if size == 0 {
		return nil, nil
	}

	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	n, err := io.ReadFull(w.file, data)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	// Seek back to end for writing
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return data[:n], nil
}

// ApplyEntries replays WAL entries from encoded data.
func (w *WAL) ApplyEntries(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Append the data to the WAL
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	return w.writer.Flush()
}
