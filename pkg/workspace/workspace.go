package workspace

import (
	"fmt"
	"os"
)

const (
	// TODO(saml) rename to "noise" related name
	CredsDir = "creds"
	PeersDir = "peers"
)

func EnsurePollenDir(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("unable to create pollen dir: %v", err)
	}

	return dir, nil
}
