package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// Start begins accepting connections.
func (s *PGServer) Start() error {
	if s.running.Load() {
		return fmt.Errorf("server already running")
	}

	ln, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("pgwire listen: %w", err)
	}
	s.listener = ln
	s.running.Store(true)

	go s.acceptLoop()
	return nil
}

// Stop shuts down the server.
func (s *PGServer) Stop() error {
	if !s.running.Load() {
		return nil
	}
	s.running.Store(false)
	close(s.shutdown)

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.sessions.Range(func(key, val any) bool {
		if sess, ok := val.(*PGSession); ok {
			sess.cancel()
			_ = sess.conn.Close()
		}
		return true
	})
	return nil
}

func (s *PGServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !s.running.Load() {
				return
			}
			continue
		}

		if s.activeConns.Load() >= int64(s.config.MaxConnections) {
			_ = conn.Close()
			continue
		}

		s.totalConns.Add(1)
		s.activeConns.Add(1)

		sess := newPGSession(s, conn)
		s.sessions.Store(sess.id, sess)

		go s.handleSession(sess)
	}
}

func (s *PGServer) handleSession(sess *PGSession) {
	defer func() {
		_ = sess.conn.Close()
		sess.cancel()
		s.sessions.Delete(sess.id)
		s.activeConns.Add(-1)
	}()

	if err := sess.handleStartup(); err != nil {
		return
	}

	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-s.shutdown:
			return
		default:
		}

		msgType, payload, err := sess.readMessage()
		if err != nil {
			return
		}

		switch msgType {
		case PGMsgQuery:
			s.totalQueries.Add(1)
			if err := sess.handleQuery(payload); err != nil {
				s.queryErrors.Add(1)
			}
		case PGMsgTerminate:
			return
		case PGMsgParse:
			// Extended query: respond with ParseComplete
			sess.writeMessage('1', nil)
			sess.flush()
		case PGMsgBind:
			sess.writeMessage('2', nil)
			sess.flush()
		case PGMsgDescribe:
			sess.writeMessage(PGMsgNoData, nil)
			sess.flush()
		case PGMsgExecute:
			// Simplified: send CommandComplete + ReadyForQuery
			sess.writeCommandComplete("SELECT 0")
			sess.writeReadyForQuery()
			sess.flush()
		case PGMsgSync:
			sess.writeReadyForQuery()
			sess.flush()
		case PGMsgFlush:
			sess.flush()
		default:
			// Unknown message: skip
		}
	}
}

// handleStartup processes the PostgreSQL startup sequence.
func (sess *PGSession) handleStartup() error {
	_ = sess.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	defer func() { _ = sess.conn.SetReadDeadline(time.Time{}) }()

	// Read startup length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(sess.conn, lenBuf); err != nil {
		return err
	}
	msgLen := int(binary.BigEndian.Uint32(lenBuf)) - 4

	if msgLen < 4 || msgLen > 10000 {
		return fmt.Errorf("invalid startup message length: %d", msgLen)
	}

	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(sess.conn, payload); err != nil {
		return err
	}

	// Check for SSL request (80877103 = 1234.5679)
	version := binary.BigEndian.Uint32(payload[:4])
	if version == 80877103 {
		// SSL not supported, send 'N'
		if _, err := sess.conn.Write([]byte{'N'}); err != nil {
			return err
		}
		return sess.handleStartup() // client will retry without SSL
	}

	// Parse startup parameters
	params := parseStartupParams(payload[4:])
	sess.user = params["user"]
	if db, ok := params["database"]; ok && db != "" {
		sess.database = db
	}

	// Authentication
	if sess.server.config.RequireAuth {
		sess.writeAuthCleartextPassword()
		sess.flush()

		msgType, pw, err := sess.readMessage()
		if err != nil {
			return err
		}
		if msgType != PGMsgPassword {
			return fmt.Errorf("expected password message")
		}
		password := strings.TrimRight(string(pw), "\x00")
		if password != sess.server.config.Password {
			sess.writeError("28P01", "password authentication failed for user \""+sess.user+"\"")
			sess.flush()
			return fmt.Errorf("auth failed")
		}
	}

	// Send AuthOK
	sess.writeAuthOK()

	// Send parameter status messages
	sess.writeParamStatus("server_version", sess.server.config.ServerVersion)
	sess.writeParamStatus("server_encoding", "UTF8")
	sess.writeParamStatus("client_encoding", "UTF8")
	sess.writeParamStatus("DateStyle", "ISO, MDY")
	sess.writeParamStatus("TimeZone", "UTC")
	sess.writeParamStatus("is_superuser", "on")
	sess.writeParamStatus("application_name", "")

	// Send BackendKeyData
	sess.writeBackendKeyData(1, 1)

	// Send ReadyForQuery
	sess.writeReadyForQuery()

	return sess.flush()
}

func parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)
	parts := strings.Split(string(data), "\x00")
	for i := 0; i+1 < len(parts); i += 2 {
		if parts[i] == "" {
			break
		}
		params[parts[i]] = parts[i+1]
	}
	return params
}
