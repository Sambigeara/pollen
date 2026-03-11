// Package perm manages file and directory permissions for the pln data
// directory on Linux. On non-Linux platforms all operations are no-ops.
//
// On Linux, the daemon runs as the "pln" system user. The postinstall
// script pre-creates directories with pln:pln ownership. When running as
// root (e.g. sudo pln init), atomicWrite chowns files to
// pln:pln automatically. When the pln user doesn't exist (e.g. macOS,
// dev machines), chown is a no-op.
//
// Expected permission matrix for /var/lib/pln (Linux):
//
//	Path                       Owner:Group    Mode   Set by
//	─────────────────────────  ─────────────  ────   ───────────────────────
//	/var/lib/pln/              pln:pln        0770   postinstall.sh
//	keys/                      pln:pln        0770   EnsureDir
//	keys/ed25519.key           pln:pln        0640   SetGroupReadable
//	keys/ed25519.pub           pln:pln        0640   SetGroupReadable
//	keys/admin_ed25519.key     pln:pln        0640   SetGroupReadable
//	keys/admin_ed25519.pub     pln:pln        0640   SetGroupReadable
//	keys/cluster.trust.pb      pln:pln        0640   WriteGroupReadable
//	keys/delegation.cert.pb    pln:pln        0640   WriteGroupReadable
//	config.yaml                pln:pln        0660   WriteGroupWritable
//	state.pb                   pln:pln        0640   WriteGroupReadable
//	.state.lock                pln:pln        0600   SetPrivate
//	pln.sock                   pln:pln        0660   SetGroupSocket
package perm
