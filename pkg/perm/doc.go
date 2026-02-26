// Package perm manages file and directory permissions for the pollen data
// directory on Linux. On non-Linux platforms all operations are no-ops.
//
// On Linux, a system group named "pollen" allows non-root group members
// (e.g. the SSH user during bootstrap) to read public credentials and
// connect to the control socket without requiring sudo.
//
// Expected permission matrix for /var/lib/pollen (Linux):
//
//	Path                       Owner:Group    Mode   Set by
//	─────────────────────────  ─────────────  ────   ───────────────────────
//	/var/lib/pollen/           root:pollen    0770   postinstall.sh
//	keys/                      root:pollen    0770   SetGroupDir
//	keys/ed25519.key           root:root      0600   GenIdentityKey (private)
//	keys/ed25519.pub           root:pollen    0640   SetGroupReadable
//	keys/admin_ed25519.key     root:root      0600   LoadOrCreateAdminKey (private)
//	keys/admin_ed25519.pub     root:pollen    0640   SetGroupReadable
//	keys/cluster.trust.pb      root:pollen    0640   SetGroupReadable
//	keys/membership.cert.pb    root:pollen    0640   SetGroupReadable
//	pollen.sock                root:pollen    0660   SetGroupSocket
//
// If the pollen group does not exist, all SetGroup* functions silently
// return nil — the node still works, but non-root access is unavailable.
package perm
