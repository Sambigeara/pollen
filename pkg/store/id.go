package store

import (
	// "crypto/rand"
	// "time"

	"github.com/oklog/ulid/v2"
)

func NewID() ulid.ULID {
	// t := time.Now()
	// entropy := ulid.Monotonic(rand.Reader, 0)
	// return ulid.MustNew(ulid.Timestamp(t), entropy)

	return ulid.Make()
}
