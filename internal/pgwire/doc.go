// Package pgwire implements the PostgreSQL wire protocol (v3) server.
//
// It enables PostgreSQL-compatible clients and drivers to connect to
// Chronicle, translating SQL queries into Chronicle's internal query
// representation with session and transaction management.
package pgwire
