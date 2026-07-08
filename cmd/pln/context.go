// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/identity"
)

const (
	defaultContextName = "default"
	contextsFileName   = "contexts.yaml"
	contextsSubdir     = "contexts"
)

type contextEntry struct {
	Dir  string `yaml:"dir"`
	Host string `yaml:"host,omitempty"`
	Wire string `yaml:"wire,omitempty"`
	// Ephemeral port so named local contexts coexist without collision.
	Port int `yaml:"port,omitempty"`
}

func (e contextEntry) isSSHBridge() bool { return e.Host != "" && e.Wire == "" }

// UnmarshalYAML migrates legacy entries that stored a `pln://` URL in
// the `host` field into the new `wire` field, so existing files keep
// loading without operator action.
func (e *contextEntry) UnmarshalYAML(node *yaml.Node) error {
	type raw contextEntry
	var r raw
	if err := node.Decode(&r); err != nil {
		return err
	}
	if r.Wire == "" && strings.HasPrefix(r.Host, plnTargetScheme) {
		r.Wire = r.Host
		r.Host = ""
	}
	*e = contextEntry(r)
	return nil
}

type contextsFile struct {
	Contexts map[string]contextEntry `yaml:"contexts,omitempty"`
	Current  string                  `yaml:"current,omitempty"`
}

func contextsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, plnDir, contextsFileName), nil
}

func contextsRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, plnDir, contextsSubdir), nil
}

func contextDir(name string) (string, error) {
	root, err := contextsRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, name), nil
}

func loadContexts() (*contextsFile, error) {
	path, err := contextsPath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &contextsFile{Contexts: map[string]contextEntry{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var cf contextsFile
	if err := yaml.Unmarshal(raw, &cf); err != nil {
		return nil, fmt.Errorf("parse contexts.yaml: %w", err)
	}
	if cf.Contexts == nil {
		cf.Contexts = map[string]contextEntry{}
	}
	return &cf, nil
}

func saveContexts(cf *contextsFile) error {
	path, err := contextsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil { //nolint:mnd
		return err
	}
	raw, err := yaml.Marshal(cf)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600) //nolint:mnd
}

func resolveContextName() string {
	if v := os.Getenv("PLN_CONTEXT"); v != "" {
		return v
	}
	// Brew's launchd unit must always use the default context, not
	// whichever context the interactive shell last switched to.
	if os.Getenv("XPC_SERVICE_NAME") == "homebrew.mxcl.pln" {
		return defaultContextName
	}
	cf, _ := loadContexts()
	if cf != nil && cf.Current != "" {
		return cf.Current
	}
	return defaultContextName
}

// reportCtxWireWrite calls persistCtxWire and surfaces the outcome:
// the "configured" line on a fresh write, a stderr warning on error,
// silence when the ctx already pinned a wire fallback. action names
// the preceding step ("enrolled", "bootstrapped") for the error path.
func reportCtxWireWrite(out, errOut io.Writer, ctxName, endpoint, action string) {
	switch wrote, err := persistCtxWire(ctxName, endpoint); {
	case err != nil:
		fmt.Fprintf(errOut, "pln: %s but couldn't write wire fallback to ctx: %v\n", action, err)
	case wrote:
		fmt.Fprintf(out, "wire fallback configured: %s%s\n", plnTargetScheme, endpoint)
	}
}

// persistCtxWire writes hostPort to the named ctx's `wire:` field as
// the canonical pln:// URL. It only writes when the field is currently
// empty so an operator's explicit override survives a subsequent pln
// join.
func persistCtxWire(name, hostPort string) (bool, error) {
	cf, err := loadContexts()
	if err != nil {
		return false, err
	}
	entry := cf.Contexts[name]
	if entry.Wire != "" {
		return false, nil
	}
	entry.Wire = plnTargetScheme + hostPort
	cf.Contexts[name] = entry
	if err := saveContexts(cf); err != nil {
		return false, err
	}
	return true, nil
}

// firstBootstrapWireEndpoint returns the first non-empty wire_endpoint
// across the bootstrap peer list, or "" if none advertise a wire
// listener. The list is already ordered most-routable-first by the
// issuer's pickBootstrapPeers.
func firstBootstrapWireEndpoint(peers []*admissionv1.BootstrapPeer) string {
	for _, p := range peers {
		if w := p.GetWireEndpoint(); w != "" {
			return w
		}
	}
	return ""
}

func resolveContextBindings(name, defaultDir string) (contextEntry, error) {
	cf, err := loadContexts()
	if err != nil {
		// The default ctx is the implicit "always works" entry; YAML
		// overrides on it are opt-in. When contexts.yaml is unreadable
		// (the daemon under systemd's ProtectHome=yes can't open
		// $HOME/.pln/contexts.yaml), synthesise the default so `pln up`
		// still launches. Named ctxs must surface the error since the
		// operator explicitly opted into them.
		if name == defaultContextName {
			return contextEntry{Dir: defaultDir}, nil
		}
		return contextEntry{}, err
	}
	entry, ok := cf.Contexts[name]
	if name == defaultContextName {
		if !ok {
			return contextEntry{Dir: defaultDir}, nil
		}
		entry.Dir = defaultDir
		return entry, nil
	}
	if !ok {
		return contextEntry{}, notFoundErr("context %q not found", name)
	}
	return entry, nil
}

func inferTarget(arg string) (dir, host, wire string, err error) {
	if strings.HasPrefix(arg, plnTargetScheme) {
		return "", "", arg, nil
	}
	if strings.Contains(arg, "@") {
		return "", arg, "", nil
	}
	pathLike := strings.HasPrefix(arg, "/") || strings.HasPrefix(arg, "~") ||
		strings.HasPrefix(arg, "./") || strings.HasPrefix(arg, "../")
	if !pathLike {
		if info, statErr := os.Stat(arg); statErr != nil || !info.IsDir() {
			return "", "", "", fmt.Errorf("ambiguous target %q: prefix with ./ for a directory, user@ for an SSH host, or pln:// for a remote endpoint", arg)
		}
	}
	abs, err := filepath.Abs(expandHome(arg))
	if err != nil {
		return "", "", "", err
	}
	return abs, "", "", nil
}

func expandHome(p string) string {
	if !strings.HasPrefix(p, "~") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, strings.TrimPrefix(p, "~"))
}

func newContextCmds() *cobra.Command {
	root := &cobra.Command{
		Use:     "context",
		Aliases: []string{"ctx"},
		Short:   "Manage named admin contexts",
		Long: `Contexts let one admin shell target several clusters or remote
daemons by name (analogous to kubectl contexts). The active context
sets the implicit --dir/--host for every command. The reserved name
"default" always resolves to $PLN_DIR.`,
	}

	addCmd := &cobra.Command{
		Use:   "add <name> <user@host | pln://host:port | /path/to/pln-dir>",
		Short: "Create a new context bound to a wire endpoint, SSH bridge, or local pln directory",
		Long: `Adds a named context. Targets starting with pln:// configure a wire
fallback that can also host a local daemon for the same identity;
targets containing '@' bind to a remote daemon over SSH; absolute or
relative directory paths bind to a local pln dir. --from imports an
existing admin keypair into the ctx's identity dir.`,
		Example: "  pln context add prod root@prod.example.com --from default\n  pln context add cloud pln://edge.pln.sh:7443\n  pln context add dev /tmp/pln-dev",
		Args:    cobra.ExactArgs(2), //nolint:mnd
		RunE:    runContextAdd,
	}
	addCmd.Flags().String("from", "", "Import admin keys from <dir> or 'default' for $PLN_DIR/keys")

	switchCmd := &cobra.Command{
		Use:     "switch <name>",
		Aliases: []string{"use"},
		Short:   "Switch to a named context",
		Long:    "Sets the active context for subsequent commands. Override per-command via $PLN_CONTEXT.",
		Example: "  pln ctx use prod",
		Args:    cobra.ExactArgs(1),
		RunE:    runContextUse,
	}

	lsCmd := &cobra.Command{
		Use:   "ls",
		Short: "List contexts",
		Long:  "Lists all known contexts. The active context is marked with `*` in the CURRENT column.",
		Args:  cobra.NoArgs,
		RunE:  runContextList,
	}

	rmCmd := &cobra.Command{
		Use:   "rm <name>",
		Short: "Remove a context",
		Long:  "Removes a context entry. For contexts whose identity is stored under ~/.pln/contexts/<name>/, the per-context dir is also deleted. Path-bound local contexts leave their pln dir untouched.",
		Args:  cobra.ExactArgs(1),
		RunE:  runContextRemove,
	}

	currentCmd := &cobra.Command{
		Use:   "current",
		Short: "Print the current context name",
		Long:  "Prints the name of the active context. Useful in shell prompts or scripts.",
		Args:  cobra.NoArgs,
		RunE:  runContextCurrent,
	}

	showCmd := &cobra.Command{
		Use:     "show [name]",
		Aliases: []string{"whoami"},
		Short:   "Show a context's identity and cert status",
		Long:    "Prints the target, identity, capabilities, and grant validity windows for a context (the active one if no name is given). Use it to check why a command against the wire endpoint was refused.",
		Example: "  pln context show\n  pln ctx whoami staging",
		Args:    cobra.MaximumNArgs(1),
		RunE:    runContextShow,
	}

	root.AddCommand(addCmd, switchCmd, lsCmd, rmCmd, currentCmd, showCmd)
	return root
}

func runContextAdd(cmd *cobra.Command, args []string) error {
	name, target := args[0], args[1]
	if name == defaultContextName {
		return fmt.Errorf("%q is a reserved context name", defaultContextName)
	}

	dir, host, wire, err := inferTarget(target)
	if err != nil {
		return err
	}

	cf, err := loadContexts()
	if err != nil {
		return err
	}
	if _, exists := cf.Contexts[name]; exists {
		return fmt.Errorf("context %q already exists", name)
	}

	entry := contextEntry{Dir: dir, Host: host, Wire: wire}
	sshBridge := entry.isSSHBridge()

	if host != "" || wire != "" {
		ctxDir, err := provisionContextIdentity(cmd, name, sshBridge)
		if err != nil {
			return err
		}
		entry.Dir = ctxDir
	} else if _, statErr := os.Stat(dir); errors.Is(statErr, os.ErrNotExist) {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s does not exist yet; run `pln init` or `pln up` after switching to this context\n", dir)
	}

	// Every ctx that may host a daemon needs a port reserved. SSH-bridge
	// ctxs never host a daemon locally, so they skip allocation.
	if !sshBridge {
		port, err := pickFreeUDPPort()
		if err != nil {
			return fmt.Errorf("pick free port: %w", err)
		}
		entry.Port = port
	}

	cf.Contexts[name] = entry
	if err := saveContexts(cf); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "added context %q\n", name)
	return nil
}

// pickFreeUDPPort asks the kernel for an unused UDP port by binding to :0
// and immediately releasing it. There's an inherent race between release
// and the daemon binding the same port later, but in practice launchd's
// KeepAlive retries and the cost of the rare collision is one relaunch.
func pickFreeUDPPort() (int, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected local addr type %T", conn.LocalAddr())
	}
	return addr.Port, nil
}

// provisionContextIdentity allocates ~/.pln/contexts/<name>/ for any
// non-local ctx. --from imports an existing admin keypair. With no
// --from, an admin keypair is generated only when needAdminKey is set
// (SSH-bridge ctxs that drive root-level operations on a remote node);
// wire-fallback ctxs leave the dir empty for `pln join` to populate.
func provisionContextIdentity(cmd *cobra.Command, name string, needAdminKey bool) (string, error) {
	ctxDir, err := contextDir(name)
	if err != nil {
		return "", err
	}
	identityDir := identity.IdentityPath(ctxDir)
	if err := os.MkdirAll(identityDir, 0o700); err != nil { //nolint:mnd
		return "", fmt.Errorf("create identity dir: %w", err)
	}
	from, _ := cmd.Flags().GetString("from")
	switch {
	case from == defaultContextName:
		defaultDir, _ := cmd.Flags().GetString("dir")
		if err := copyIdentity(identity.IdentityPath(defaultDir), identityDir); err != nil {
			return "", fmt.Errorf("import from default: %w", err)
		}
	case from != "":
		if err := copyIdentity(from, identityDir); err != nil {
			return "", fmt.Errorf("import from %s: %w", from, err)
		}
	case needAdminKey:
		if _, _, err := identity.EnsureAdminKey(identityDir); err != nil {
			return "", fmt.Errorf("generate admin key: %w", err)
		}
	}
	return ctxDir, nil
}

func runContextUse(cmd *cobra.Command, args []string) error {
	name := args[0]
	cf, err := loadContexts()
	if err != nil {
		return err
	}
	if name != defaultContextName {
		if _, ok := cf.Contexts[name]; !ok {
			return notFoundErr("context %q not found", name)
		}
	}
	cf.Current = name
	if err := saveContexts(cf); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "current context: %s\n", name)
	return nil
}

func runContextList(cmd *cobra.Command, _ []string) error {
	cf, err := loadContexts()
	if err != nil {
		return err
	}

	current := resolveContextName()

	names := []string{defaultContextName}
	for n := range cf.Contexts {
		if n != defaultContextName {
			names = append(names, n)
		}
	}
	slices.Sort(names[1:]) // default always first

	defaultDir, _ := cmd.Flags().GetString("dir")

	t := newStatusTable("NAME", "TARGET", "CURRENT")
	for _, n := range names {
		entry := cf.Contexts[n]
		var target string
		switch {
		case entry.Wire != "":
			target = entry.Wire
		case entry.Host != "":
			target = entry.Host
		case n == defaultContextName:
			target = defaultDir + " (local)"
		default:
			target = entry.Dir
			if p := entry.Port; p > 0 {
				target = fmt.Sprintf("%s :%d", target, p)
			}
		}
		marker := ""
		if n == current {
			marker = "*"
		}
		t.Row(n, target, marker)
	}
	fmt.Fprintln(cmd.OutOrStdout(), t)
	return nil
}

func runContextRemove(cmd *cobra.Command, args []string) error {
	name := args[0]
	if name == defaultContextName {
		return fmt.Errorf("%q is built-in and cannot be removed", defaultContextName)
	}
	cf, err := loadContexts()
	if err != nil {
		return err
	}
	entry, ok := cf.Contexts[name]
	if !ok {
		return notFoundErr("context %q not found", name)
	}

	// Only remove the per-context dir if we own it (remote contexts store
	// keys under ~/.pln/contexts/<name>/). For local contexts the dir is
	// user-managed and we never touch it.
	ctxRoot, _ := contextDir(name)
	if (entry.Host != "" || entry.Wire != "") && entry.Dir == ctxRoot {
		if err := os.RemoveAll(entry.Dir); err != nil {
			return fmt.Errorf("remove identity dir: %w", err)
		}
	}

	// Local named contexts on macOS may have a launchd plist we generated.
	// Tear it down best-effort so the daemon stops and the file isn't orphaned.
	if entry.Host == "" && entry.Wire == "" && runtime.GOOS == osDarwin {
		if plistPath, err := userPlnPlistPath(name); err == nil {
			if _, statErr := os.Stat(plistPath); statErr == nil {
				_ = exec.CommandContext(cmd.Context(), "launchctl", "unload", "-w", plistPath).Run()
				_ = os.Remove(plistPath)
			}
		}
	}

	delete(cf.Contexts, name)
	if cf.Current == name {
		cf.Current = ""
	}
	if err := saveContexts(cf); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "removed context %q\n", name)
	return nil
}

func runContextCurrent(cmd *cobra.Command, _ []string) error {
	fmt.Fprintln(cmd.OutOrStdout(), resolveContextName())
	return nil
}

func copyIdentity(src, dst string) error {
	if err := os.MkdirAll(dst, 0o700); err != nil { //nolint:mnd
		return err
	}
	for _, name := range []string{"admin_ed25519.key", "admin_ed25519.pub", "root.pub"} {
		srcPath := filepath.Join(src, name)
		dstPath := filepath.Join(dst, name)
		if err := copyFile(srcPath, dstPath); err != nil {
			if errors.Is(err, os.ErrNotExist) && name == "root.pub" {
				continue
			}
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600) //nolint:mnd
}
