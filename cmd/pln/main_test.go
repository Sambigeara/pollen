package main

import (
	"os/user"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldEscalateToRoot(t *testing.T) {
	pln := &user.User{Uid: "500"}

	tests := []struct {
		name    string
		goos    string
		uid     int
		dir     string
		plnUser *user.User
		want    bool
	}{
		{"non-linux", "darwin", 1000, "/var/lib/pln", pln, false},
		{"already root", "linux", 0, "/var/lib/pln", pln, false},
		{"home dir", "linux", 1000, "/home/sam/.pln", pln, false},
		{"no pln user", "linux", 1000, "/var/lib/pln", nil, false},
		{"is pln user", "linux", 500, "/var/lib/pln", pln, false},
		{"exact sysdir", "linux", 1000, "/var/lib/pln", pln, true},
		{"subdir", "linux", 1000, "/var/lib/pln/foo", pln, true},
		{"prefix trick", "linux", 1000, "/var/lib/plnfoo", pln, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldEscalateToRoot(tt.goos, tt.uid, tt.dir, tt.plnUser))
		})
	}
}
