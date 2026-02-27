// Package chprotocol implements the ClickHouse native TCP protocol server.
//
// It accepts ClickHouse-compatible TCP connections and translates native
// protocol queries into Chronicle's internal query representation,
// enabling ClickHouse clients to connect directly.
package chprotocol
