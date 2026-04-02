// File and directory permissions for the pln data directory.
//
// On Linux, `pln provision` creates the pln system user/group and the
// state directories with setgid (2770) so files created inside them
// automatically inherit the pln group. The daemon runs as pln; CLI
// users in the pln group share access via group permissions. On
// non-Linux platforms the ownership helpers are no-ops.
//
// Expected permission matrix for /var/lib/pln (Linux):
//
//	Path                       Owner:Group    Mode   Set by
//	─────────────────────────  ─────────────  ────   ───────────────────────
//	/var/lib/pln/              pln:pln        2770   Provision / EnsureDir
//	keys/                      pln:pln        2770   EnsureDir
//	keys/ed25519.key           pln:pln        0640   WriteGroupReadable
//	keys/ed25519.pub           pln:pln        0640   WriteGroupReadable
//	keys/admin_ed25519.key     pln:pln        0640   WriteGroupReadable
//	keys/admin_ed25519.pub     pln:pln        0640   WriteGroupReadable
//	keys/root.pub              pln:pln        0640   WriteGroupReadable
//	keys/delegation.cert.pb    pln:pln        0640   WriteGroupReadable
//	config.yaml                pln:pln        0660   WriteGroupWritable
//	state.pb                   pln:pln        0640   WriteGroupReadable (supervisor)
//	pln.sock                   pln:pln        0660   SetGroupSocket
//	cas/                       pln:pln        2770   EnsureDir
//	cas/<shard>/               pln:pln        2770   EnsureDir
//	cas/<shard>/<hash>.wasm    pln:pln        0640   WriteGroupReadable
package plnfs
