package relays

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"
)

func TestIsRetriableErr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retriable: true,
		},
		{
			name:      "invalid url",
			err:       fmt.Errorf("wrapped: %w", ErrInvalidURL),
			retriable: false,
		},
		{
			name: "wrapped connect error with invalid url",
			err: fmt.Errorf("wrapped: %w", &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: fmt.Errorf("wrapped: %w", ErrInvalidURL),
			}),
			retriable: false,
		},
		{
			name: "context deadline exceeded",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: context.DeadlineExceeded,
			},
			retriable: true,
		},
		{
			name: "dns timeout",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: &net.DNSError{IsTimeout: true},
			},
			retriable: true,
		},
		{
			name: "dns not found",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: &net.DNSError{IsNotFound: true},
			},
			retriable: false,
		},
		{
			name: "connection refused",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: &net.OpError{Err: syscall.ECONNREFUSED},
			},
			retriable: true,
		},
		{
			name: "host unreachable",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: &net.OpError{Err: syscall.EHOSTUNREACH},
			},
			retriable: true,
		},
		{
			name: "network unreachable",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: &net.OpError{Err: syscall.ENETUNREACH},
			},
			retriable: true,
		},
		{
			name: "hostname mismatch",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: x509.HostnameError{},
			},
			retriable: false,
		},
		{
			name: "invalid certificate",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: x509.CertificateInvalidError{},
			},
			retriable: false,
		},
		{
			name: "unknown authority",
			err: &ConnectErr{
				URL:   "wss://relay.example.com",
				Cause: x509.UnknownAuthorityError{},
			},
			retriable: false,
		},
		{
			name: "http 403",
			err: &ConnectErr{
				URL:        "wss://relay.example.com",
				StatusCode: http.StatusForbidden,
				Cause:      errors.New("bad handshake"),
			},
			retriable: false,
		},
		{
			name: "http 404",
			err: &ConnectErr{
				URL:        "wss://relay.example.com",
				StatusCode: http.StatusNotFound,
				Cause:      errors.New("bad handshake"),
			},
			retriable: false,
		},
		{
			name: "http 502",
			err: &ConnectErr{
				URL:        "wss://relay.example.com",
				StatusCode: http.StatusBadGateway,
				Cause:      errors.New("bad handshake"),
			},
			retriable: true,
		},
		{
			name: "http 530",
			err: &ConnectErr{
				URL:        "wss://relay.example.com",
				StatusCode: 530,
				Cause:      errors.New("bad handshake"),
			},
			retriable: true,
		},
		{
			name:      "non connect error",
			err:       errors.New("some other error"),
			retriable: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := IsRetriableErr(test.err)
			if got != test.retriable {
				t.Fatalf("IsRetriableErr(%v) = %v, want %v", test.err, got, test.retriable)
			}
		})
	}
}
