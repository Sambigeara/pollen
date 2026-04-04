//go:build linux

package plnfs

// EnableSystemMode sets system mode for the duration of a test and
// returns a restore function.
func EnableSystemMode() func() {
	old := system
	system = true
	return func() { system = old }
}
