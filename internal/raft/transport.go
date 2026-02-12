package raft

import (
	"net/http"
	"time"
)

// NewRaftTransport creates a new Raft transport.
func NewRaftTransport(localAddr string, timeout time.Duration) *RaftTransport {
	return &RaftTransport{
		localAddr: localAddr,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout: timeout,
	}
}
