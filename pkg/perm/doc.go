// Package perm manages file and directory permissions for the pln data
// directory on Linux. On non-Linux platforms all operations are no-ops.
//
// On Linux, the daemon runs as the "pln" system user. All data-directory
// files are owned by pln:pln. Group membership (e.g. for the SSH user
// during bootstrap) allows reading public credentials and connecting to
// the control socket without requiring sudo.
//
// Expected permission matrix for /var/lib/pln (Linux):
//
//	Path                       Owner:Group    Mode   Set by
//	─────────────────────────  ─────────────  ────   ───────────────────────
//	/var/lib/pln/              pln:pln        0770   postinstall.sh
//	keys/                      pln:pln        0770   SetGroupDir
//	keys/ed25519.key           pln:pln        0600   SetPrivate
//	keys/ed25519.pub           pln:pln        0640   SetGroupReadable
//	keys/admin_ed25519.key     pln:pln        0600   SetPrivate
//	keys/admin_ed25519.pub     pln:pln        0640   SetGroupReadable
//	keys/cluster.trust.pb      pln:pln        0640   SetGroupReadable
//	keys/membership.cert.pb    pln:pln        0640   SetGroupReadable
//	pln.sock                   pln:pln        0660   SetGroupSocket
//
// If the pln user does not exist, all Set* functions silently
// return nil — the node still works, but ownership is unchanged.
package perm
