// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// controlTokenFile is the per-node file holding the shared secret that
// external (non-unix) callers must present on the control API.
const controlTokenFile = "control.token"

const (
	controlTokenBytes = 32
	controlTokenPerm  = 0o600
)

// ensureControlToken returns the shared secret for the TCP control endpoint,
// creating it on first use with 0600 permissions and reusing it on subsequent
// starts. Returned string is the hex-encoded value.
func ensureControlToken(dir string) (string, error) {
	path := filepath.Join(dir, controlTokenFile)
	if raw, err := os.ReadFile(path); err == nil {
		token := strings.TrimSpace(string(raw))
		// Reject silently-corrupted files so a downstream auth failure
		// points at the token, not at the caller.
		if _, decodeErr := hex.DecodeString(token); token != "" && decodeErr == nil {
			return token, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	buf := make([]byte, controlTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	token := hex.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(token+"\n"), controlTokenPerm); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return token, nil
}
