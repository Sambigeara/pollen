// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/sambigeara/pollen/pkg/auth"
)

const (
	defaultContextName = "default"
	contextsFileName   = "contexts.yaml"
	contextsSubdir     = "contexts"
)

type contextEntry struct {
	Dir  string `yaml:"dir"`
	Host string `yaml:"host,omitempty"`
}

type contextsFile struct {
	Contexts map[string]contextEntry `yaml:"contexts,omitempty"`
	Current  string                  `yaml:"current,omitempty"`
}

// Contexts live under the user's home, never under $PLN_DIR — they're
// operator state, not per-daemon state.
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
	cf, _ := loadContexts()
	if cf != nil && cf.Current != "" {
		return cf.Current
	}
	return defaultContextName
}

// The default context falls back to the caller's $PLN_DIR. Identity for
// any context is always auth.IdentityPath(dir).
func resolveContextBindings(name, defaultDir string) (dir, host string, err error) {
	if name == defaultContextName {
		return defaultDir, "", nil
	}
	cf, loadErr := loadContexts()
	if loadErr != nil {
		return "", "", loadErr
	}
	entry, ok := cf.Contexts[name]
	if !ok {
		return "", "", notFoundErr("context %q not found", name)
	}
	return entry.Dir, entry.Host, nil
}

// Rules: contains '@' → host; starts with /, ~, ./, ../ → dir; resolves to
// an existing directory on disk → dir; otherwise → error. Directory targets
// are always absolutised so stored contexts don't depend on CWD.
func inferTarget(arg string) (dir, host string, err error) {
	if strings.Contains(arg, "@") {
		return "", arg, nil
	}
	pathLike := strings.HasPrefix(arg, "/") || strings.HasPrefix(arg, "~") ||
		strings.HasPrefix(arg, "./") || strings.HasPrefix(arg, "../")
	if !pathLike {
		if info, statErr := os.Stat(arg); statErr != nil || !info.IsDir() {
			return "", "", fmt.Errorf("ambiguous target %q: prefix with ./ for a directory or user@ for an SSH host", arg)
		}
	}
	abs, err := filepath.Abs(expandHome(arg))
	if err != nil {
		return "", "", err
	}
	return abs, "", nil
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
		Use:   "add <name> <user@host | /path/to/pln-dir>",
		Short: "Create a new context bound to a remote host or local pln directory",
		Long: `Adds a named context. Targets containing '@' bind to a remote daemon
over SSH; absolute or relative directory paths bind to a local pln dir.
For remote contexts, --from imports an existing admin keypair so this
shell can act as an admin against the remote cluster.`,
		Example: "  pln context add prod root@prod.example.com --from default\n  pln context add dev /tmp/pln-dev",
		Args:    cobra.ExactArgs(2), //nolint:mnd
		RunE:    runContextAdd,
	}
	addCmd.Flags().String("from", "", "For remote contexts: import admin keys from <dir> or 'default' for $PLN_DIR/keys")

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
		Long:  "Removes a context entry. For remote contexts whose identity is stored under ~/.pln/contexts/<name>/, the per-context dir is also deleted. Local contexts (path-bound) leave their pln dir untouched.",
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

	root.AddCommand(addCmd, switchCmd, lsCmd, rmCmd, currentCmd)
	return root
}

func runContextAdd(cmd *cobra.Command, args []string) error {
	name, target := args[0], args[1]
	if name == defaultContextName {
		return fmt.Errorf("%q is a reserved context name", defaultContextName)
	}

	dir, host, err := inferTarget(target)
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

	if host != "" {
		ctxDir, err := provisionRemoteIdentity(cmd, name)
		if err != nil {
			return err
		}
		dir = ctxDir
	} else if _, statErr := os.Stat(dir); errors.Is(statErr, os.ErrNotExist) {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s does not exist yet; run `pln init` or `pln up` after switching to this context\n", dir)
	}

	cf.Contexts[name] = contextEntry{Dir: dir, Host: host}
	if err := saveContexts(cf); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "added context %q\n", name)
	return nil
}

// Generates a fresh admin keypair under ~/.pln/contexts/<name>/, or copies
// one in when --from is set.
func provisionRemoteIdentity(cmd *cobra.Command, name string) (string, error) {
	ctxDir, err := contextDir(name)
	if err != nil {
		return "", err
	}
	identityDir := auth.IdentityPath(ctxDir)
	if err := os.MkdirAll(identityDir, 0o700); err != nil { //nolint:mnd
		return "", fmt.Errorf("create identity dir: %w", err)
	}
	from, _ := cmd.Flags().GetString("from")
	switch from {
	case "":
		if _, _, err := auth.EnsureAdminKey(identityDir); err != nil {
			return "", fmt.Errorf("generate admin key: %w", err)
		}
	case defaultContextName:
		defaultDir, _ := cmd.Flags().GetString("dir")
		if err := copyIdentity(auth.IdentityPath(defaultDir), identityDir); err != nil {
			return "", fmt.Errorf("import from default: %w", err)
		}
	default:
		if err := copyIdentity(from, identityDir); err != nil {
			return "", fmt.Errorf("import from %s: %w", from, err)
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

	names := make([]string, 0, len(cf.Contexts)+1)
	names = append(names, defaultContextName)
	for n := range cf.Contexts {
		names = append(names, n)
	}
	slices.Sort(names[1:]) // default always first

	defaultDir, _ := cmd.Flags().GetString("dir")

	t := newStatusTable("NAME", "TARGET", "CURRENT")
	for _, n := range names {
		var target string
		switch {
		case n == defaultContextName:
			target = defaultDir + " (local)"
		case cf.Contexts[n].Host != "":
			target = cf.Contexts[n].Host
		default:
			target = cf.Contexts[n].Dir
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
	if entry.Host != "" && entry.Dir == ctxRoot {
		if err := os.RemoveAll(entry.Dir); err != nil {
			return fmt.Errorf("remove identity dir: %w", err)
		}
	}

	// Local named contexts on macOS may have a launchd plist we generated.
	// Tear it down best-effort so the daemon stops and the file isn't orphaned.
	if entry.Host == "" && runtime.GOOS == osDarwin {
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
