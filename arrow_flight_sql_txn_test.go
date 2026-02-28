package chronicle

import (
	"testing"
)

func TestFlightSQLTransactionManager_Lifecycle(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultFlightSQLConfig()
	server := NewFlightSQLServer(db, config)
	tm := NewFlightSQLTransactionManager(server)

	// Begin transaction
	txnID, err := tm.BeginTransaction()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	txn, err := tm.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if txn.Status != FlightTxnActive {
		t.Fatalf("expected active, got %s", txn.Status)
	}

	// Commit
	if err := tm.CommitTransaction(txnID); err != nil {
		t.Fatalf("commit: %v", err)
	}

	txn, _ = tm.GetTransaction(txnID)
	if txn.Status != FlightTxnCommitted {
		t.Fatalf("expected committed, got %s", txn.Status)
	}

	// Double commit should fail
	if err := tm.CommitTransaction(txnID); err == nil {
		t.Fatal("expected error on double commit")
	}
}

func TestFlightSQLTransactionManager_Rollback(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultFlightSQLConfig()
	server := NewFlightSQLServer(db, config)
	tm := NewFlightSQLTransactionManager(server)

	txnID, err := tm.BeginTransaction()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	if err := tm.RollbackTransaction(txnID); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	txn, _ := tm.GetTransaction(txnID)
	if txn.Status != FlightTxnRolledBack {
		t.Fatalf("expected rolled_back, got %s", txn.Status)
	}
}

func TestFlightSQLServer_GetSqlInfo(t *testing.T) {
	db := setupTestDB(t)

	server := NewFlightSQLServer(db, DefaultFlightSQLConfig())
	info := server.GetSqlInfo()

	if info["flight_sql_server_name"] != "Chronicle Flight SQL" {
		t.Fatalf("unexpected server name: %v", info["flight_sql_server_name"])
	}
}

func TestFlightSQLServer_TxnStats(t *testing.T) {
	db := setupTestDB(t)

	server := NewFlightSQLServer(db, DefaultFlightSQLConfig())
	stats := server.Stats()

	if stats.PreparedStatements != 0 {
		t.Fatalf("expected 0 prepared statements, got %d", stats.PreparedStatements)
	}
}

func TestFlightSQLServer_CreateClosePreparedStatement(t *testing.T) {
	db := setupTestDB(t)

	server := NewFlightSQLServer(db, DefaultFlightSQLConfig())

	stmtID, schema, err := server.CreatePreparedStatement("SELECT * FROM cpu_usage")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if stmtID == "" {
		t.Fatal("expected non-empty statement ID")
	}
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}

	// Close the prepared statement
	if err := server.ClosePreparedStatement(stmtID); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Close again should fail
	if err := server.ClosePreparedStatement(stmtID); err == nil {
		t.Fatal("expected error on double close")
	}
}

func TestFlightSQLServer_GetFlightInfoForStatement(t *testing.T) {
	db := setupTestDB(t)

	server := NewFlightSQLServer(db, DefaultFlightSQLConfig())

	info, err := server.GetFlightInfoStatement("SELECT * FROM test_metric")
	if err != nil {
		t.Fatalf("get flight info: %v", err)
	}
	if len(info.Endpoints) == 0 {
		t.Fatal("expected at least one endpoint")
	}
	if info.TotalRows != -1 {
		t.Fatalf("expected -1 total rows, got %d", info.TotalRows)
	}
}
