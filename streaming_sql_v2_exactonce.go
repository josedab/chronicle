package chronicle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// ExactlyOnceProcessor – exactly-once processing semantics
// ---------------------------------------------------------------------------

// TxnState represents the state of a processing transaction.
type TxnState int

const (
	TxnActive TxnState = iota
	TxnCommitted
	TxnAborted
)

// ProcessingTransaction represents a single processing transaction used for
// exactly-once semantics.
type ProcessingTransaction struct {
	ID        uint64    `json:"id"`
	State     TxnState  `json:"state"`
	SeqNum    uint64    `json:"seq_num"`
	EventIDs  []string  `json:"event_ids"`
	OutputIDs []string  `json:"output_ids"`
	Created   time.Time `json:"created"`
	Committed time.Time `json:"committed,omitempty"`
}

// ExactlyOnceCheckpoint stores the state needed to resume processing after a
// failure.
type ExactlyOnceCheckpoint struct {
	QueryID       string            `json:"query_id"`
	SeqNum        uint64            `json:"seq_num"`
	Watermark     int64             `json:"watermark"`
	WindowKeys    []string          `json:"window_keys"`
	ProcessedIDs  map[string]bool   `json:"processed_ids"`
	Timestamp     time.Time         `json:"timestamp"`
}

// ExactlyOnceProcessor provides exactly-once processing guarantees through a
// transaction log, deduplication, and checkpoint/restore.
type ExactlyOnceProcessor struct {
	mu           sync.Mutex
	seqCounter   atomic.Uint64
	activeTxns   map[uint64]*ProcessingTransaction
	txnLog       []*ProcessingTransaction
	processedIDs map[string]bool // deduplication set
	outputIDs    map[string]bool // idempotent output tracking
	maxLogSize   int
}

// NewExactlyOnceProcessor creates an exactly-once processor.
func NewExactlyOnceProcessor(maxLogSize int) *ExactlyOnceProcessor {
	return &ExactlyOnceProcessor{
		activeTxns:   make(map[uint64]*ProcessingTransaction),
		txnLog:       make([]*ProcessingTransaction, 0, 256),
		processedIDs: make(map[string]bool),
		outputIDs:    make(map[string]bool),
		maxLogSize:   maxLogSize,
	}
}

// IsDuplicate returns true if the event ID has already been processed.
func (eop *ExactlyOnceProcessor) IsDuplicate(eventID string) bool {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	return eop.processedIDs[eventID]
}

// Begin starts a new processing transaction, returning its ID.
func (eop *ExactlyOnceProcessor) Begin() uint64 {
	txnID := eop.seqCounter.Add(1)
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn := &ProcessingTransaction{
		ID:      txnID,
		State:   TxnActive,
		SeqNum:  txnID,
		Created: time.Now(),
	}
	eop.activeTxns[txnID] = txn
	return txnID
}

// RecordEvent records that an event was processed within a transaction.
func (eop *ExactlyOnceProcessor) RecordEvent(txnID uint64, eventID string) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if txn.State != TxnActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}
	txn.EventIDs = append(txn.EventIDs, eventID)
	return nil
}

// RecordOutput records an output produced by the transaction to enable
// idempotent replays.
func (eop *ExactlyOnceProcessor) RecordOutput(txnID uint64, outputID string) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	txn.OutputIDs = append(txn.OutputIDs, outputID)
	return nil
}

// Commit commits the transaction, marking all its events as processed and
// outputs as emitted.
func (eop *ExactlyOnceProcessor) Commit(txnID uint64) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if txn.State != TxnActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	txn.State = TxnCommitted
	txn.Committed = time.Now()

	for _, eid := range txn.EventIDs {
		eop.processedIDs[eid] = true
	}
	for _, oid := range txn.OutputIDs {
		eop.outputIDs[oid] = true
	}

	eop.txnLog = append(eop.txnLog, txn)
	delete(eop.activeTxns, txnID)

	// Trim log if it grows too large.
	if len(eop.txnLog) > eop.maxLogSize {
		eop.txnLog = eop.txnLog[len(eop.txnLog)-eop.maxLogSize/2:]
	}
	return nil
}

// Rollback aborts a transaction; none of its events are marked processed.
func (eop *ExactlyOnceProcessor) Rollback(txnID uint64) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	txn.State = TxnAborted
	eop.txnLog = append(eop.txnLog, txn)
	delete(eop.activeTxns, txnID)
	return nil
}

// CreateCheckpoint captures the current state for later recovery.
func (eop *ExactlyOnceProcessor) CreateCheckpoint(queryID string, watermark int64, windowKeys []string) *ExactlyOnceCheckpoint {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	cp := &ExactlyOnceCheckpoint{
		QueryID:      queryID,
		SeqNum:       eop.seqCounter.Load(),
		Watermark:    watermark,
		WindowKeys:   windowKeys,
		ProcessedIDs: make(map[string]bool, len(eop.processedIDs)),
		Timestamp:    time.Now(),
	}
	for k, v := range eop.processedIDs {
		cp.ProcessedIDs[k] = v
	}
	return cp
}

// RestoreCheckpoint restores previously checkpointed state.
func (eop *ExactlyOnceProcessor) RestoreCheckpoint(cp *ExactlyOnceCheckpoint) {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	eop.seqCounter.Store(cp.SeqNum)
	eop.processedIDs = make(map[string]bool, len(cp.ProcessedIDs))
	for k, v := range cp.ProcessedIDs {
		eop.processedIDs[k] = v
	}
}
