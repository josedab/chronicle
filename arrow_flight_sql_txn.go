package chronicle

import (
	"fmt"
	"sync"
	"time"
)

// FlightSQLTransaction represents a Flight SQL transaction.
type FlightSQLTransaction struct {
	ID        string           `json:"id"`
	Status    FlightTxnStatus  `json:"status"`
	Queries   []string         `json:"queries"`
	CreatedAt time.Time        `json:"created_at"`
	Results   [][]ArrowRecordBatch `json:"-"`
}

// FlightTxnStatus represents the status of a Flight SQL transaction.
type FlightTxnStatus string

const (
	FlightTxnActive    FlightTxnStatus = "active"
	FlightTxnCommitted FlightTxnStatus = "committed"
	FlightTxnRolledBack FlightTxnStatus = "rolled_back"
)

// FlightSQLTransactionManager manages Flight SQL transactions.
type FlightSQLTransactionManager struct {
	server *FlightSQLServer
	mu     sync.RWMutex
	txns   map[string]*FlightSQLTransaction
}

// NewFlightSQLTransactionManager creates a new transaction manager.
func NewFlightSQLTransactionManager(server *FlightSQLServer) *FlightSQLTransactionManager {
	return &FlightSQLTransactionManager{
		server: server,
		txns:   make(map[string]*FlightSQLTransaction),
	}
}

// BeginTransaction starts a new transaction.
func (tm *FlightSQLTransactionManager) BeginTransaction() (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txnID := fmt.Sprintf("txn-%d", time.Now().UnixNano())
	tm.txns[txnID] = &FlightSQLTransaction{
		ID:        txnID,
		Status:    FlightTxnActive,
		CreatedAt: time.Now(),
	}
	return txnID, nil
}

// ExecuteInTransaction executes a SQL statement within a transaction.
func (tm *FlightSQLTransactionManager) ExecuteInTransaction(txnID, sql string) ([]ArrowRecordBatch, error) {
	tm.mu.Lock()
	txn, exists := tm.txns[txnID]
	if !exists {
		tm.mu.Unlock()
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}
	if txn.Status != FlightTxnActive {
		tm.mu.Unlock()
		return nil, fmt.Errorf("transaction %s is %s", txnID, txn.Status)
	}
	txn.Queries = append(txn.Queries, sql)
	tm.mu.Unlock()

	batches, err := tm.server.HandleStatementQuery(sql)
	if err != nil {
		return nil, err
	}

	tm.mu.Lock()
	txn.Results = append(txn.Results, batches)
	tm.mu.Unlock()

	return batches, nil
}

// CommitTransaction commits a transaction.
func (tm *FlightSQLTransactionManager) CommitTransaction(txnID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.txns[txnID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}
	if txn.Status != FlightTxnActive {
		return fmt.Errorf("transaction %s is %s", txnID, txn.Status)
	}
	txn.Status = FlightTxnCommitted
	return nil
}

// RollbackTransaction rolls back a transaction.
func (tm *FlightSQLTransactionManager) RollbackTransaction(txnID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.txns[txnID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}
	if txn.Status != FlightTxnActive {
		return fmt.Errorf("transaction %s already %s", txnID, txn.Status)
	}
	txn.Status = FlightTxnRolledBack
	return nil
}

// GetTransaction returns transaction info.
func (tm *FlightSQLTransactionManager) GetTransaction(txnID string) (*FlightSQLTransaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txn, exists := tm.txns[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}
	return txn, nil
}

// --- Flight SQL Statistics ---

// FlightSQLStats provides server statistics for monitoring.
type FlightSQLStats struct {
	TotalConnections   int64         `json:"total_connections"`
	ActiveStreams       int           `json:"active_streams"`
	PreparedStatements int           `json:"prepared_statements"`
	TotalQueries       int64         `json:"total_queries"`
	ActiveTransactions int           `json:"active_transactions"`
	Uptime             time.Duration `json:"uptime"`
}

// Stats returns current server statistics.
func (s *FlightSQLServer) Stats() FlightSQLStats {
	s.sessionMu.RLock()
	psCount := len(s.preparedStatements)
	sessionCount := len(s.sessions)
	s.sessionMu.RUnlock()

	return FlightSQLStats{
		ActiveStreams:       sessionCount,
		PreparedStatements: psCount,
	}
}
