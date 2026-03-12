package relays

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
)

// relay errors
var (
	ErrInvalidURL       = errors.New("invalid relay url")
	ErrInvalidID        = errors.New("invalid event id")
	ErrInvalidSignature = errors.New("invalid event signature")

	ErrDisconnected       = errors.New("relay has been disconnected")
	ErrSendFailed         = errors.New("failed to send message, channel is full")
	ErrSubscriptionClosed = errors.New("subscription was closed")

	ErrDuplicateSub    = errors.New("subscription with the same id already exists")
	ErrFullSubChannel  = errors.New("subscription channel is full")
	ErrInvalidSubMatch = errors.New("event does not match the subscription filters")
	ErrClosedSub       = errors.New("subscription closed by the relay")
)

// pool errors
var (
	ErrPoolClosed      = fmt.Errorf("pool is closed")
	ErrNoRelays        = fmt.Errorf("no relays available")
	ErrDuplicateStream = errors.New("stream with the same id already exists")
)

// ConnectErr is returned when a websocket connection attempt fails.
type ConnectErr struct {
	URL        string
	StatusCode int
	Cause      error
}

func (e *ConnectErr) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("failed to connect to %q: %s; status: %d", e.URL, e.Cause.Error(), e.StatusCode)
	}
	return fmt.Sprintf("failed to connect to %q: %v", e.URL, e.Cause.Error())
}

func (e *ConnectErr) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// IsRetriable returns true if the connection error is retriable.
func (e *ConnectErr) IsRetriable() bool {
	if e == nil {
		return true
	}
	if errors.Is(e.Cause, context.DeadlineExceeded) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(e.Cause, &dnsErr) {
		if dnsErr.IsNotFound {
			return false
		}
		if dnsErr.IsTimeout {
			return true
		}
		return false
	}

	var hostErr x509.HostnameError
	if errors.As(e.Cause, &hostErr) {
		return false
	}

	var certInvalidErr x509.CertificateInvalidError
	if errors.As(e.Cause, &certInvalidErr) {
		return false
	}

	var unknownAuthorityErr x509.UnknownAuthorityError
	if errors.As(e.Cause, &unknownAuthorityErr) {
		return false
	}

	var opErr *net.OpError
	if errors.As(e.Cause, &opErr) {
		if opErr.Timeout() {
			return true
		}
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
		if errors.Is(opErr.Err, syscall.EHOSTUNREACH) || errors.Is(opErr.Err, syscall.ENETUNREACH) {
			return true
		}
	}

	var netErr net.Error
	if errors.As(e.Cause, &netErr) && netErr.Timeout() {
		return true
	}

	switch e.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return false
	case http.StatusBadGateway, http.StatusServiceUnavailable, 530:
		return true
	}
	return false
}

// IsRetriableErr returns true if the error is retriable.
func IsRetriableErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrInvalidURL) {
		return false
	}
	if connErr, ok := err.(*ConnectErr); ok {
		return connErr.IsRetriable()
	}
	return false
}
