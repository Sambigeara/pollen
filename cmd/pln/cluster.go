// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	defaultInviteTTL  = 5 * time.Minute
	httpFetchTimeout  = 30 * time.Second
	relayReadyTimeout = 20 * time.Second
)

var sshBaseArgs = []string{
	"-o", "StrictHostKeyChecking=no",
	"-o", "UserKnownHostsFile=/dev/null",
	"-o", "LogLevel=ERROR",
	"-o", "ControlMaster=auto",
	"-o", "ControlPath=~/.ssh/pln-cm-%r@%h:%p",
	"-o", "ControlPersist=60s",
}

func newIDCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "id",
		Short: "Show local node identity public key",
		Long: `Prints the hex-encoded ed25519 public key of this node, generating one
on first call. Pipe-friendly (no trailing newline). Other admins use
this value as the --subject of an invite token.`,
		Example: "  pln id\n  pln invite --subject \"$(ssh user@host pln id)\"",
		RunE:    withEnv(runID, localOnly()),
	}
}

func newBootstrapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap relays and joiners",
	}

	sshCmd := &cobra.Command{
		Use:   "ssh [name=]<target> [[name=]target...|-]",
		Short: "Bootstrap one or more nodes over SSH",
		Long: `Installs Pollen, enrols the cluster, and starts the daemon on each
SSH target in parallel. Targets must accept SSH as root or with
passwordless sudo. Linux only.

Prefix a target with ` + "`name=`" + ` to label the node, or pipe a list of targets
on stdin via ` + "`-`" + `. Pass --admin to delegate admin authority to the new
relay so the cluster keeps working with the root offline. Pass --no-up
to skip starting the local daemon when the orchestrator also enrols
itself as a cluster member.

The token-authenticated control API is bound on each target at the
standard port so the node is remotely manageable out of the box.`,
		Example: `  pln bootstrap ssh user@host
  pln bootstrap ssh relay-eu=root@10.0.0.5 relay-us=root@10.0.0.6 --admin
  echo "media=alice@10.0.0.5" | pln bootstrap ssh -`,
		Args: cobra.MinimumNArgs(1),
		RunE: withEnv(runBootstrapSSH),
	}
	sshCmd.Flags().Int("relay-port", config.DefaultBootstrapPort, "Relay UDP port to advertise")
	sshCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the relay peer")
	sshCmd.Flags().Bool("admin", false, "Delegate admin authority to the relay")
	sshCmd.Flags().Bool("no-up", false, "Enrol the orchestrator without starting its local daemon")

	cmd.AddCommand(sshCmd)
	return cmd
}

func newClusterCmds() []*cobra.Command {
	purgeCmd := &cobra.Command{
		Use:   "purge",
		Short: "Delete local cluster state",
		Long: `Deletes local cluster credentials (root.pub, membership and
delegation certs, admin keypair), CAS, runtime state, and config from
$PLN_DIR. The node identity (ed25519.{key,pub}) is preserved by
default — pass --include-keys to wipe the keys directory entirely.
Errors if the daemon is running. Prompts unless --yes is given.`,
		Example: "  pln down && pln purge --yes",
		RunE:    withEnv(runPurge, wantsRoot(), localOnly()),
	}
	purgeCmd.Flags().Bool("include-keys", false, "Also delete the node identity keypair")
	purgeCmd.Flags().Bool("yes", false, "Skip interactive confirmation")

	joinCmd := &cobra.Command{
		Use:   "join <token>",
		Short: "Join a cluster using a token",
		Long: `Enrols this node into the cluster the token was minted for. Tokens
come from ` + "`pln invite`" + ` on an admin node. By default, starts the daemon
once enrolment succeeds; use --no-up to defer.`,
		Example: "  pln join \"$(ssh admin pln invite --subject $(pln id))\"",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runJoin, wantsRoot(), localOnly()),
	}
	joinCmd.Flags().Bool("no-up", false, "Enroll credentials without starting the daemon")
	joinCmd.Flags().Bool("public", false, "Hint that this node is publicly reachable; the mesh may use it as a relay (verified at runtime)")

	inviteCmd := &cobra.Command{
		Use:   "invite [subject-pub]",
		Short: "Generate an invite token",
		Long: `Mints a signed invite token. Pass to ` + "`pln join`" + ` on the joining node.
Tokens are time-limited (--ttl) and may bind to a specific subject key
or carry hard access deadlines. Properties are baked into the issued
cert and surfaced to seeds at call time.`,
		Example: "  pln invite --ttl 30m --prop role=worker\n  pln invite --subject $(ssh laptop pln id) --prop role=editor",
		Args:    cobra.RangeArgs(0, 1),
		RunE:    withEnv(runInvite),
	}
	inviteCmd.Flags().String("subject", "", "Optional hex node public key to bind invite")
	inviteCmd.Flags().Duration("ttl", defaultInviteTTL, "Invite token validity duration")
	inviteCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the invited peer")
	inviteCmd.Flags().StringArray("prop", nil, "Cert properties: key=value, JSON, or - for stdin")

	adminCmd := &cobra.Command{Use: "admin", Short: "Manage admin keys (advanced)"}
	adminCmd.AddCommand(&cobra.Command{
		Use:    "keygen",
		Short:  "Generate the local admin key",
		Long:   "Generates an ed25519 admin keypair under $PLN_DIR/keys. Holding the key alone grants no signing authority — install a delegated cert with `pln admin set-cert`.",
		RunE:   withEnv(runAdminKeygen, wantsRoot(), localOnly()),
		Hidden: true,
	})
	adminCmd.AddCommand(&cobra.Command{
		Use:    "set-cert <admin-cert-b64>",
		Short:  "Install a delegated admin certificate",
		Long:   "Installs a delegated admin cert (issued by the root admin) that grants this node signing authority on behalf of the cluster.",
		Args:   cobra.ExactArgs(1),
		RunE:   withEnv(runAdminSetCert, wantsRoot(), localOnly()),
		Hidden: true,
	})

	grantCmd := &cobra.Command{
		Use:   "grant <peer-id>",
		Short: "Grant a certificate to a connected peer",
		Long: `Issues a fresh delegation cert to an already-connected peer. Use
--admin to delegate admin authority (so the cluster stays operable
without the root). Properties are baked into the cert and visible to
seeds and the policy router.`,
		Example: "  pln grant ab12cd34 --prop role=lead --prop team=backend\n  pln grant relay1 --admin",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runGrant),
	}
	grantCmd.Flags().Bool("admin", false, "Issue with full admin capabilities")
	grantCmd.Flags().StringArray("prop", nil, "Cert properties: key=value, JSON, or - for stdin")

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize local root cluster state",
		Long: `Generates root credentials and seeds a single-node cluster on this
host. After ` + "`pln up`" + `, use ` + "`pln invite`" + ` or ` + "`pln bootstrap ssh`" + ` to add
peers. Idempotent if already initialised as the root.

Properties baked in via --prop are written to config.yaml and applied on
every subsequent ` + "`pln up`" + `. Edit config.yaml and restart to update them
later.`,
		Example: "  pln init\n  pln init --prop role=primary --prop region=eu",
		RunE:    withEnv(runInit, wantsRoot(), localOnly()),
	}
	initCmd.Flags().StringArray("prop", nil, "Root node properties: key=value, JSON, or - for stdin")

	return []*cobra.Command{
		initCmd,
		purgeCmd,
		joinCmd,
		inviteCmd,
		adminCmd,
		grantCmd,
		newBootstrapCmd(),
	}
}

func runID(cmd *cobra.Command, _ []string, env *cliEnv) error {
	identityDir := auth.IdentityPath(env.dir)
	pub, err := auth.ReadIdentityPub(identityDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, pub, err = auth.EnsureIdentityKey(identityDir); err != nil {
			return err
		}
	}
	fmt.Fprint(cmd.OutOrStdout(), hex.EncodeToString(pub))
	return nil
}

func runInit(cmd *cobra.Command, _ []string, env *cliEnv) error {
	if nodeSocketActive(filepath.Join(env.dir, socketName)) {
		return errors.New("local node is running; run `pln down` before initializing")
	}

	identityDir := auth.IdentityPath(env.dir)
	_, pub, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}

	existing, err := auth.LoadNodeCredentials(identityDir)
	if err == nil {
		_, adminPub, adminErr := auth.LoadAdminKey(identityDir)
		if adminErr == nil && slices.Equal(adminPub, existing.RootPub()) {
			fmt.Fprintf(cmd.OutOrStdout(), "already initialized as root cluster\nroot_pub: %s\ncluster_id: %x\n", hex.EncodeToString(adminPub), sha256.Sum256(existing.RootPub()))
			return nil
		}
		return errors.New("node is already enrolled in a cluster; run `pln purge` before initializing a new root cluster")
	}

	props, err := parseProperties(cmd)
	if err != nil {
		return err
	}

	// Persist config before issuing the cert: the existing-root short-circuit
	// blocks subsequent re-init, so a config save that races behind a cert
	// write would leave the two diverged with no recovery path.
	if props != nil {
		env.cfg.Properties = props.AsMap()
		if err := config.Save(env.dir, env.cfg); err != nil {
			return fmt.Errorf("persist node properties to config: %w", err)
		}
	}

	creds, err := auth.EnsureLocalRootCredentials(identityDir, pub, props, time.Now(), auth.DefaultDelegationTTL)
	if err != nil {
		return err
	}

	_, adminPub, _ := auth.LoadAdminKey(identityDir)
	fmt.Fprintf(cmd.OutOrStdout(), "initialized root cluster\nroot_pub: %s\ncluster_id: %x\n", hex.EncodeToString(adminPub), sha256.Sum256(creds.RootPub()))
	return nil
}

func runPurge(cmd *cobra.Command, _ []string, env *cliEnv) error {
	includeKeys, _ := cmd.Flags().GetBool("include-keys")
	if nodeSocketActive(filepath.Join(env.dir, socketName)) {
		return errors.New("local node is running; run `pln down` before purging state")
	}

	if confirmed, _ := cmd.Flags().GetBool("yes"); !confirmed {
		fmt.Fprintln(cmd.ErrOrStderr(), "This will delete local cluster, admin, and runtime state.")
		fmt.Fprint(cmd.ErrOrStderr(), "Type \"yes\" to continue: ")
		scanner := bufio.NewScanner(cmd.InOrStdin())
		if !scanner.Scan() || strings.TrimSpace(strings.ToLower(scanner.Text())) != "yes" {
			return errors.New("aborted")
		}
	}

	paths := []string{
		"keys/root.pub", "keys/membership.cert.pb", "keys/delegation.cert.pb",
		"keys/admin_ed25519.key", "keys/admin_ed25519.pub", "config.yaml",
		"state.pb", "state.yaml", "state.yaml.bak", "consumed_invites.json",
		"peers.json", "invites", "cas", "pln.log", socketName,
	}
	if includeKeys {
		paths = append(paths, "keys")
	}

	for _, p := range paths {
		_ = os.RemoveAll(filepath.Join(env.dir, p))
	}

	fmt.Fprintln(cmd.OutOrStdout(), "local state purged")
	return nil
}

func runJoin(cmd *cobra.Command, args []string, env *cliEnv) error {
	identityDir := auth.IdentityPath(env.dir)
	privKey, pubKey, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}

	tkn, err := resolveJoinToken(cmd.Context(), privKey, args[0])
	if err != nil {
		return fmt.Errorf("resolve token: %w", err)
	}

	if _, credErr := auth.EnrollNodeCredentials(identityDir, pubKey, tkn, time.Now()); credErr != nil {
		if errors.Is(credErr, auth.ErrDifferentCluster) {
			_ = runPurge(cmd, nil, env) // Clear old cluster state
			_, credErr = auth.EnrollNodeCredentials(identityDir, pubKey, tkn, time.Now())
		}
		if credErr != nil {
			return fmt.Errorf("enroll credentials: %w", credErr)
		}
	}

	if err := rememberBootstrapPeers(env.dir, tkn.GetClaims().GetBootstrap()); err != nil {
		return fmt.Errorf("persist bootstrap peers: %w", err)
	}

	if public, _ := cmd.Flags().GetBool("public"); public {
		env.cfg.Public = true
		if err := config.Save(env.dir, env.cfg); err != nil {
			return err
		}
	}

	if noUp, _ := cmd.Flags().GetBool("no-up"); noUp {
		fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; run `pln up -d` to start the node")
		return nil
	}

	if nodeSocketActive(filepath.Join(env.dir, socketName)) {
		if err := servicectl("restart", cmd, env); err != nil {
			return fmt.Errorf("joined cluster but failed to restart daemon: %w", err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "joined cluster; daemon restarted")
		return nil
	}

	fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; starting daemon")
	return servicectl("start", cmd, env)
}

func runInvite(cmd *cobra.Command, args []string, env *cliEnv) error {
	var subjectPub ed25519.PublicKey
	subjectFlag, _ := cmd.Flags().GetString("subject")

	if len(args) == 1 {
		subjectFlag = args[0]
	}
	if subjectFlag != "" {
		pk, err := types.PeerKeyFromString(strings.TrimSpace(subjectFlag))
		if err != nil {
			return err
		}
		subjectPub = ed25519.PublicKey(pk.Bytes())
	}

	ttl, _ := cmd.Flags().GetDuration("ttl")
	expireAfter, _ := cmd.Flags().GetDuration("expire-after")

	identityDir := auth.IdentityPath(env.dir)
	nodePriv, _, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}

	signer, err := auth.NewDelegationSigner(identityDir, nodePriv)
	if err != nil {
		return errors.New("this node cannot issue invites; only delegated admins can sign invite tokens")
	}

	bootstrap, err := resolveBootstrapPeers(cmd.Context(), env)
	if err != nil {
		return err
	}

	attrs, err := parseProperties(cmd)
	if err != nil {
		return err
	}

	token, err := signer.IssueInviteToken(subjectPub, bootstrap, time.Now(), ttl, expireAfter, attrs)
	if err != nil {
		return err
	}

	encoded, err := auth.EncodeInviteToken(token)
	if err != nil {
		return err
	}

	fmt.Fprint(cmd.OutOrStdout(), encoded)
	return nil
}

func runAdminKeygen(cmd *cobra.Command, _ []string, env *cliEnv) error {
	_, pub, err := auth.EnsureAdminKey(auth.IdentityPath(env.dir))
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "admin_pub: %s\nnote: this does not grant signing authority\ninstall a delegated admin cert with `pln admin set-cert <admin-cert-b64>`\n", hex.EncodeToString(pub))
	return nil
}

func runAdminSetCert(cmd *cobra.Command, args []string, env *cliEnv) error {
	identityDir := auth.IdentityPath(env.dir)
	_, nodePub, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}
	creds, err := auth.LoadNodeCredentials(identityDir)
	if err != nil {
		return errors.New("node credentials not initialized; run `pln init` or `pln join <token>` first")
	}
	if err := auth.InstallDelegationCert(identityDir, args[0], creds.RootPub(), nodePub, time.Now()); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "delegated admin certificate installed")
	return nil
}

type bootstrapResult struct {
	err     error
	target  string
	peerPub ed25519.PublicKey
	addrs   []string
}

func runBootstrapSSH(cmd *cobra.Command, args []string, env *cliEnv) error {
	specs, err := resolveSSHTargets(cmd, args)
	if err != nil {
		return err
	}

	relayPort, _ := cmd.Flags().GetInt("relay-port")
	if relayPort < minPort || relayPort > maxPort {
		return fmt.Errorf("invalid relay port %d", relayPort)
	}
	expireAfter, _ := cmd.Flags().GetDuration("expire-after")
	delegateAdmin, _ := cmd.Flags().GetBool("admin")

	identityDir := auth.IdentityPath(env.dir)
	nodePriv, localPub, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}
	signer, err := auth.NewDelegationSigner(identityDir, nodePriv)
	if err != nil {
		return errors.New("this node cannot issue tokens; only delegated admins can sign enrollment tokens")
	}

	// Gather every remote's identity before issuing seed tokens: each remote must be told
	// about every other remote as a persistent bootstrap peer, which we can only do once we
	// know all their public keys.
	type discovery struct {
		err      error
		prepared preparedRemote
	}
	discoveries := make([]discovery, len(specs))
	var discWG sync.WaitGroup
	errOut := cmd.ErrOrStderr()
	for i, spec := range specs {
		discWG.Go(func() {
			// Emit a per-target start line so the operator sees immediate
			// progress instead of staring at a silent terminal for the
			// ~30-60s it takes to curl-install + enrol + start each remote.
			fmt.Fprintf(errOut, "bootstrapping %s...\n", spec.target)
			prepared, err := discoverRemote(cmd.Context(), spec, relayPort)
			discoveries[i] = discovery{prepared: prepared, err: err}
		})
	}
	discWG.Wait()

	// Seed pool: local peer cache, any daemon we can reach (local or bridged), and every
	// successfully-discovered remote. Each remote receives this set minus itself, so
	// daemon restarts always have someone to reconnect to. Under ctx (no laptop daemon),
	// the bridged daemon is the only populated source.
	peerPool := make(map[string]*admissionv1.BootstrapPeer)
	localCache, err := peercache.Open(env.dir)
	if err != nil {
		return fmt.Errorf("load peer cache: %w", err)
	}
	for _, entry := range localCache.Snapshot() {
		peerPool[entry.PeerKey.String()] = &admissionv1.BootstrapPeer{
			PeerPub: entry.PeerKey.Bytes(),
			Addrs:   slices.Clone(entry.Addrs),
		}
	}
	if resp, statusErr := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{})); statusErr == nil {
		for _, n := range append([]*controlv1.NodeSummary{resp.Msg.GetSelf()}, resp.Msg.GetNodes()...) {
			if n == nil || n.GetAddr() == "" {
				continue
			}
			key := types.PeerKeyFromBytes(n.GetNode().GetPeerPub()).String()
			if _, ok := peerPool[key]; ok {
				continue
			}
			peerPool[key] = &admissionv1.BootstrapPeer{
				PeerPub: append([]byte(nil), n.GetNode().GetPeerPub()...),
				Addrs:   []string{n.GetAddr()},
			}
		}
	}
	for _, d := range discoveries {
		if d.err != nil {
			continue
		}
		key := types.PeerKeyFromBytes(d.prepared.peerPub).String()
		peerPool[key] = &admissionv1.BootstrapPeer{
			PeerPub: append([]byte(nil), d.prepared.peerPub...),
			Addrs:   slices.Clone(d.prepared.addrs),
		}
	}

	// Buffered to len(discoveries) so immediate-failure sends never block and the
	// drain reads exactly one result per discovery. Results are printed in
	// completion order so fast hosts confirm before slow ones finish.
	results := make(chan bootstrapResult, len(discoveries))
	var enrollWG sync.WaitGroup
	for _, d := range discoveries {
		if d.err != nil {
			results <- bootstrapResult{target: d.prepared.spec.target, err: d.err}
			continue
		}
		prepared := d.prepared
		enrollWG.Go(func() {
			seeded := seedPeersFor(peerPool, prepared.peerPub)
			if err := enrollRemote(cmd.Context(), env, signer, prepared, seeded, expireAfter, delegateAdmin); err != nil {
				results <- bootstrapResult{target: prepared.spec.target, err: err}
				return
			}
			results <- bootstrapResult{
				target:  prepared.spec.target,
				peerPub: prepared.peerPub,
				addrs:   prepared.addrs,
			}
		})
	}

	out := cmd.OutOrStdout()
	var succeeded, failed int
	var failErrs []error
	var bootstrappedPeers []bootstrapResult
	for range discoveries {
		r := <-results
		if r.err != nil {
			failed++
			fmt.Fprintf(out, "  %-40s failed (%v)\n", r.target, r.err)
			failErrs = append(failErrs, fmt.Errorf("%s: %w", r.target, r.err))
			continue
		}
		succeeded++
		fmt.Fprintf(out, "  %-40s ok\n", r.target)
		localCache.Upsert(types.PeerKeyFromBytes(r.peerPub), r.addrs, time.Now())
		bootstrappedPeers = append(bootstrappedPeers, r)
	}
	enrollWG.Wait()
	fmt.Fprintln(out)

	if len(bootstrappedPeers) == 0 {
		return errors.Join(failErrs...)
	}

	if err := localCache.Flush(); err != nil {
		return fmt.Errorf("persist bootstrap peers: %w", err)
	}

	if nodeSocketActive(filepath.Join(env.dir, socketName)) { //nolint:nestif
		for _, r := range bootstrappedPeers {
			if _, err := env.client.ConnectPeer(cmd.Context(), connect.NewRequest(&controlv1.ConnectPeerRequest{
				PeerPub: r.peerPub,
				Addrs:   r.addrs,
			})); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "%s: connect failed: %v\n", r.target, err)
			}
		}
	} else {
		bsPeers := make([]*admissionv1.BootstrapPeer, 0, len(bootstrappedPeers))
		for _, r := range bootstrappedPeers {
			bsPeers = append(bsPeers, &admissionv1.BootstrapPeer{
				PeerPub: append([]byte(nil), r.peerPub...),
				Addrs:   append([]string(nil), r.addrs...),
			})
		}

		joinToken, err := createJoinTokenWithSigner(signer, config.DefaultMembershipTTL, localPub, defaultInviteTTL, expireAfter, bsPeers, nil)
		if err != nil {
			return fmt.Errorf("create local join token: %w", err)
		}

		// runJoin reads --no-up from cmd; with --no-up set it stages the
		// orchestrator's credentials without starting the local daemon.
		if err := runJoin(cmd, []string{joinToken}, env); err != nil {
			return err
		}
	}

	if len(specs) > 1 {
		fmt.Fprintf(out, "%d succeeded, %d failed\n", succeeded, failed)
	}
	if failed > 0 {
		fmt.Fprintf(out, "hint: for failed hosts, use `pln invite` and run `pln join <token>` on the remote host\n")
		return errors.Join(failErrs...)
	}
	return nil
}

// preparedRemote describes a target after pre-enrolment: sudo access confirmed, pln binary
// installed, node identity fetched. spec is always set so it can identify the target in
// error paths; the other fields are only valid once discoverRemote returns nil.
type preparedRemote struct {
	spec    sshTargetSpec
	peerPub ed25519.PublicKey
	addrs   []string
	public  bool
}

// discoverRemote runs pre-enrolment checks and fetches the remote node's public key.
// Safe to call concurrently for different targets.
func discoverRemote(ctx context.Context, spec sshTargetSpec, relayPort int) (preparedRemote, error) {
	fail := func(err error) (preparedRemote, error) {
		return preparedRemote{spec: spec}, err
	}

	inferredAddr, err := inferRelayAddrFromSSHTarget(ctx, spec.target, relayPort)
	if err != nil {
		return fail(err)
	}
	host, _, _ := net.SplitHostPort(inferredAddr)
	ip := net.ParseIP(host)
	public := ip != nil && !ip.IsPrivate() && !ip.IsLoopback()

	if err := requireRemoteLinux(ctx, spec.target); err != nil {
		return fail(err)
	}
	if err := requireRemoteSudo(ctx, spec.target); err != nil {
		return fail(err)
	}
	if err := ensureRemotePollen(ctx, spec.target); err != nil {
		return fail(err)
	}

	idCmd := sshPln(ctx, spec.target, "id")
	var idStdout, idStderr bytes.Buffer
	idCmd.Stdout = &idStdout
	idCmd.Stderr = &idStderr
	if err := idCmd.Run(); err != nil {
		return fail(fmt.Errorf("failed to fetch remote node identity: %w\n%s", err, strings.TrimSpace(idStderr.String())))
	}

	peerKey, err := types.PeerKeyFromString(strings.TrimSpace(idStdout.String()))
	if err != nil {
		return fail(err)
	}

	return preparedRemote{
		spec:    spec,
		peerPub: ed25519.PublicKey(peerKey.Bytes()),
		addrs:   []string{inferredAddr},
		public:  public,
	}, nil
}

// enrollRemote issues a seed token carrying bootstrapPeers, enrols it on the remote and starts
// the daemon. bootstrapPeers must already exclude the remote itself.
// Safe to call concurrently for different targets.
func enrollRemote(ctx context.Context, env *cliEnv, signer *auth.DelegationSigner, remote preparedRemote, bootstrapPeers []*admissionv1.BootstrapPeer, expireAfter time.Duration, delegateAdmin bool) error {
	seedToken, err := createJoinTokenWithSigner(signer, config.DefaultMembershipTTL, remote.peerPub, 1*time.Minute, expireAfter, bootstrapPeers, nil)
	if err != nil {
		return fmt.Errorf("create seed token: %w", err)
	}
	return bootstrapRelayOverSSH(ctx, env, remote.spec.target, seedToken, remote.peerPub, delegateAdmin, remote.spec.nodeName, remote.public)
}

func seedPeersFor(pool map[string]*admissionv1.BootstrapPeer, self ed25519.PublicKey) []*admissionv1.BootstrapPeer {
	selfKey := types.PeerKeyFromBytes(self).String()
	out := make([]*admissionv1.BootstrapPeer, 0, len(pool))
	for k, p := range pool {
		if k == selfKey {
			continue
		}
		out = append(out, &admissionv1.BootstrapPeer{
			PeerPub: append([]byte(nil), p.GetPeerPub()...),
			Addrs:   slices.Clone(p.GetAddrs()),
		})
	}
	return out
}

func bootstrapRelayOverSSH(ctx context.Context, env *cliEnv, sshTarget, seedToken string, relayPub ed25519.PublicKey, delegateAdmin bool, nodeName string, public bool) error {
	joinArgs := []string{"join", "--no-up"}
	if public {
		joinArgs = append(joinArgs, "--public")
	}
	joinArgs = append(joinArgs, seedToken)
	if out, err := sshPln(ctx, sshTarget, joinArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to enroll relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	upArgs := []string{"pln", "up", "-d"}
	if nodeName != "" {
		upArgs = append(upArgs, "--name", nodeName)
	}
	if out, err := sshSudo(ctx, sshTarget, upArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if err := waitForRelayReady(ctx, sshTarget); err != nil {
		return err
	}

	// Bind the token-authenticated control API so the node is remotely
	// manageable. Applied post-start (not via a pre-daemon `pln set`) to
	// survive the config round-trip that `pln join --public` and
	// `pln up -d --name` perform. Paired with the optional admin
	// delegation into a single restart.
	if out, err := sshPln(ctx, sshTarget, "set", "control-addr", config.DefaultControlAddr).CombinedOutput(); err != nil {
		return fmt.Errorf("set control-addr: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if delegateAdmin {
		if err := provisionRelayAdminDelegation(ctx, env, sshTarget, relayPub); err != nil {
			return err
		}
	}
	if out, err := sshSudo(ctx, sshTarget, "pln", "restart").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to restart relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return waitForRelayReady(ctx, sshTarget)
}

func provisionRelayAdminDelegation(ctx context.Context, env *cliEnv, sshTarget string, relayPub ed25519.PublicKey) error {
	identityDir := auth.IdentityPath(env.dir)
	nodePriv, _, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}
	signer, err := auth.NewDelegationSigner(identityDir, nodePriv)
	if err != nil || !signer.IsRoot() {
		return errors.New("only root admin can delegate relay admin certs")
	}

	cert, err := signer.IssueMemberCert(relayPub, auth.FullCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(auth.DefaultDelegationTTL), time.Time{})
	if err != nil {
		return err
	}

	encoded, err := auth.MarshalDelegationCertBase64(cert)
	if err != nil {
		return err
	}

	if out, err := sshPln(ctx, sshTarget, "admin", "set-cert", encoded).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to install relay admin cert: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func runGrant(cmd *cobra.Command, args []string, env *cliEnv) error {
	prefix := strings.ToLower(args[0])
	admin, _ := cmd.Flags().GetBool("admin")

	attrs, err := parseProperties(cmd)
	if err != nil {
		return err
	}

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}

	var matches [][]byte
	for _, n := range statusResp.Msg.GetNodes() {
		if peerIDHasPrefix(n.GetNode().GetPeerPub(), prefix) {
			matches = append(matches, n.GetNode().GetPeerPub())
		}
	}
	if len(matches) == 0 {
		return notFoundErr("no peer matching %q", prefix)
	}
	if len(matches) > 1 {
		return ambiguousErr("ambiguous peer prefix %q matches %d peers", prefix, len(matches))
	}

	peerID := matches[0]
	if _, err := env.client.IssueCert(cmd.Context(), connect.NewRequest(&controlv1.IssueCertRequest{
		PeerPub:    peerID,
		Admin:      admin,
		Attributes: attrs,
	})); err != nil {
		return err
	}

	level := "leaf"
	if admin {
		level = "admin"
	}
	fmt.Fprintf(cmd.OutOrStdout(), "certificate issued (%s) to %s\n", level, hex.EncodeToString(peerID)[:shortHexLen])
	return nil
}

func parseProperties(cmd *cobra.Command) (*structpb.Struct, error) {
	vals, _ := cmd.Flags().GetStringArray("prop")
	if len(vals) == 0 {
		return nil, nil
	}
	merged := make(map[string]any)
	for _, raw := range vals {
		v := strings.TrimSpace(raw)
		switch {
		case v == "-":
			stat, err := os.Stdin.Stat()
			if err != nil || (stat.Mode()&os.ModeCharDevice) != 0 {
				return nil, errors.New("expected JSON on stdin (pipe a file or use heredoc)")
			}
			data, err := io.ReadAll(cmd.InOrStdin())
			if err != nil {
				return nil, fmt.Errorf("reading properties from stdin: %w", err)
			}
			var obj map[string]any
			if err := json.Unmarshal(data, &obj); err != nil {
				return nil, fmt.Errorf("invalid JSON on stdin: %w", err)
			}
			maps.Copy(merged, obj)
		case strings.HasPrefix(v, "{"):
			var obj map[string]any
			if err := json.Unmarshal([]byte(v), &obj); err != nil {
				return nil, fmt.Errorf("invalid JSON in --prop: %w", err)
			}
			maps.Copy(merged, obj)
		default:
			k, val, ok := strings.Cut(v, "=")
			if !ok || k == "" {
				return nil, fmt.Errorf("invalid property: missing '=' in %q", v)
			}
			merged[k] = val
		}
	}
	s, err := structpb.NewStruct(merged)
	if err != nil {
		return nil, fmt.Errorf("building properties: %w", err)
	}
	if err := auth.ValidateAttributes(s); err != nil {
		return nil, err
	}
	return s, nil
}

// --- Utilities ---

// rememberBootstrapPeers primes the local peer cache from a freshly resolved token.
// Only useful before the daemon starts; once running, its own handshakes refresh
// the cache.
func rememberBootstrapPeers(pollenDir string, peers []*admissionv1.BootstrapPeer) error {
	if len(peers) == 0 {
		return nil
	}
	cache, err := peercache.Open(pollenDir)
	if err != nil {
		return fmt.Errorf("load peer cache: %w", err)
	}
	now := time.Now()
	for _, bp := range peers {
		cache.Upsert(types.PeerKeyFromBytes(bp.GetPeerPub()), bp.GetAddrs(), now)
	}
	return cache.Flush()
}

func resolveBootstrapPeers(ctx context.Context, env *cliEnv) ([]*admissionv1.BootstrapPeer, error) {
	cache, err := peercache.Open(env.dir)
	if err != nil {
		return nil, fmt.Errorf("load peer cache: %w", err)
	}
	if entries := cache.Snapshot(); len(entries) > 0 {
		peers := make([]*admissionv1.BootstrapPeer, 0, len(entries))
		for _, entry := range entries {
			peers = append(peers, &admissionv1.BootstrapPeer{
				PeerPub: entry.PeerKey.Bytes(),
				Addrs:   slices.Clone(entry.Addrs),
			})
		}
		return peers, nil
	}

	resp, err := env.client.GetBootstrapInfo(ctx, connect.NewRequest(&controlv1.GetBootstrapInfoRequest{}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			return nil, err
		}
		if socketPermissionDenied(env.dir) {
			return nil, permissionErr("cannot reach daemon — are you in the pln group?\n  fix: sudo usermod -aG pln $(whoami) && newgrp pln")
		}
		return nil, unreachableErr("daemon is not running; run `pln up -d` to start the local node before issuing invites")
	}

	if rec := resp.Msg.GetRecommended(); rec != nil && rec.GetPeer() != nil && len(rec.GetAddrs()) > 0 {
		return []*admissionv1.BootstrapPeer{{
			PeerPub: append([]byte(nil), rec.GetPeer().GetPeerPub()...),
			Addrs:   append([]string(nil), rec.GetAddrs()...),
		}}, nil
	}

	self := resp.Msg.GetSelf()
	if self == nil || self.GetPeer() == nil || len(self.GetAddrs()) == 0 {
		return nil, errors.New("no bootstrap peer addresses available")
	}
	return []*admissionv1.BootstrapPeer{{
		PeerPub: append([]byte(nil), self.GetPeer().GetPeerPub()...),
		Addrs:   append([]string(nil), self.GetAddrs()...),
	}}, nil
}

func resolveJoinToken(ctx context.Context, priv ed25519.PrivateKey, encoded string) (*admissionv1.JoinToken, error) {
	trimmed := strings.TrimSpace(encoded)
	if joinToken, err := auth.DecodeJoinToken(trimmed); err == nil {
		if _, verifyErr := auth.VerifyJoinToken(joinToken, priv.Public().(ed25519.PublicKey), time.Now()); verifyErr == nil { //nolint:forcetypeassert
			return joinToken, nil
		}
	}
	inviteToken, err := auth.DecodeInviteToken(trimmed)
	if err != nil {
		return nil, errors.New("failed to decode token")
	}
	return transport.RedeemInvite(ctx, priv, inviteToken)
}

func createJoinTokenWithSigner(signer *auth.DelegationSigner, defaultMembershipTTL time.Duration, subjectPub ed25519.PublicKey, ttl, expireAfter time.Duration, bootstrap []*admissionv1.BootstrapPeer, attributes *structpb.Struct) (string, error) {
	var accessDeadline time.Time
	if expireAfter > 0 {
		accessDeadline = time.Now().Add(expireAfter)
	}
	token, err := signer.IssueJoinToken(subjectPub, bootstrap, time.Now(), ttl, defaultMembershipTTL, accessDeadline, attributes)
	if err != nil {
		return "", err
	}
	return auth.EncodeJoinToken(token)
}

func ensureRemotePollen(ctx context.Context, sshTarget string) error {
	if sshCmd(ctx, sshTarget, "which pln >/dev/null 2>&1").Run() == nil {
		if out, err := sshSudo(ctx, sshTarget, "pln", "daemon", "install").CombinedOutput(); err != nil {
			return fmt.Errorf("remote daemon install failed: %w\n%s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://raw.githubusercontent.com/%s/main/scripts/install.sh", "sambigeara/pollen"), nil)
	if reqErr != nil {
		return fmt.Errorf("failed to fetch install script: %w", reqErr)
	}
	resp, reqErr := (&http.Client{Timeout: httpFetchTimeout}).Do(req)
	if reqErr != nil {
		return fmt.Errorf("fetch install script: %w", reqErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch install script: unexpected status %s", resp.Status)
	}
	script, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read install script: %w", err)
	}

	installArgs := []string{"bash", "-s", "--"}
	if version != "dev" {
		installArgs = append(installArgs, "--version", version)
	}

	cmd := sshCmd(ctx, sshTarget, installArgs...)
	cmd.Stdin = bytes.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to install pln on remote host: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func waitForRelayReady(ctx context.Context, sshTarget string) error {
	readyCtx, cancel := context.WithTimeout(ctx, relayReadyTimeout)
	defer cancel()

	checkCmd := "for i in $(seq 1 20); do if { test -S /var/lib/pln/pln.sock || [ -S \"$HOME/.pln/pln.sock\" ]; } && pln status --include-offline >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1"
	if out, err := sshSudoShell(readyCtx, sshTarget, checkCmd).CombinedOutput(); err != nil {
		logOut, _ := sshSudoShell(ctx, sshTarget, "journalctl -u pln -n 120 --no-pager 2>/dev/null || tail -n 120 /opt/homebrew/var/log/pln.log 2>/dev/null || tail -n 120 /usr/local/var/log/pln.log 2>/dev/null || true").CombinedOutput()
		return fmt.Errorf("relay failed to become ready\nstatus output: %s\nrelay log:\n%s", strings.TrimSpace(string(out)), strings.TrimSpace(string(logOut)))
	}
	return nil
}

func sshCmd(ctx context.Context, sshTarget string, remoteCmd ...string) *exec.Cmd {
	args := slices.Concat(sshBaseArgs, []string{sshTarget}, remoteCmd)
	return exec.CommandContext(ctx, "ssh", args...)
}

func sshPln(ctx context.Context, sshTarget string, args ...string) *exec.Cmd {
	return sshCmd(ctx, sshTarget, slices.Concat([]string{"sudo", "-n", "-u", "pln", "pln"}, args)...)
}

func sshSudo(ctx context.Context, sshTarget string, args ...string) *exec.Cmd {
	return sshCmd(ctx, sshTarget, slices.Concat([]string{"sudo", "-n"}, args)...)
}

func sshSudoShell(ctx context.Context, sshTarget, shellCmd string) *exec.Cmd {
	return sshCmd(ctx, sshTarget, "sudo -n sh -c '"+shellCmd+"'")
}

func requireRemoteSudo(ctx context.Context, sshTarget string) error {
	script := `sh -c 'if [ "$(id -u)" -eq 0 ]; then exit 0; fi; sudo -n true >/dev/null 2>&1'`
	if err := sshCmd(ctx, sshTarget, script).Run(); err != nil {
		return errors.New("requires SSH as root or passwordless sudo")
	}
	return nil
}

func requireRemoteLinux(ctx context.Context, sshTarget string) error {
	out, err := sshCmd(ctx, sshTarget, "uname -s").Output()
	if err != nil {
		return fmt.Errorf("failed to probe remote OS: %w", err)
	}
	remoteOS := strings.TrimSpace(string(out))
	if remoteOS != "Linux" {
		return fmt.Errorf("remote OS is %q; pln bootstrap ssh targets Linux only", remoteOS)
	}
	return nil
}

func inferRelayAddrFromSSHTarget(ctx context.Context, target string, relayPort int) (string, error) {
	host := strings.TrimSpace(target)
	if host == "" {
		return "", errors.New("ssh target is empty")
	}
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return "", errors.New("ssh target host is empty")
	}
	if net.ParseIP(host) == nil {
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return "", fmt.Errorf("resolve hostname %q: %w", host, err)
		}
		if len(ips) == 0 {
			return "", fmt.Errorf("resolve hostname %q: no addresses returned", host)
		}
		host = ips[0]
		for _, ip := range ips {
			if p := net.ParseIP(ip); p != nil && !p.IsLinkLocalUnicast() && !p.IsLinkLocalMulticast() {
				host = ip
				break
			}
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(relayPort)), nil
}

type sshTargetSpec struct {
	target   string
	nodeName string
}

func resolveSSHTargets(cmd *cobra.Command, args []string) ([]sshTargetSpec, error) {
	var raw []string
	sawStdin := false
	for _, arg := range args {
		if arg == "-" {
			if sawStdin {
				return nil, errors.New("stdin marker '-' specified more than once")
			}
			sawStdin = true
			data, err := io.ReadAll(cmd.InOrStdin())
			if err != nil {
				return nil, fmt.Errorf("reading targets from stdin: %w", err)
			}
			for t := range strings.FieldsSeq(string(data)) {
				if t != "" {
					raw = append(raw, t)
				}
			}
			continue
		}
		raw = append(raw, arg)
	}
	if len(raw) == 0 {
		return nil, errors.New("no SSH targets specified")
	}

	specs := make([]sshTargetSpec, len(raw))
	seen := make(map[string]int)
	for i, entry := range raw {
		// An entry with `=` in the prefix (and no `@` before the `=`) binds an explicit name
		// to the target; otherwise derive a name from the host portion.
		if name, target, ok := strings.Cut(entry, "="); ok && !strings.Contains(name, "@") {
			specs[i] = sshTargetSpec{target: target, nodeName: name}
			continue
		}
		specs[i] = sshTargetSpec{target: entry, nodeName: deriveNodeName(entry, i, seen)}
	}
	return specs, nil
}

func deriveNodeName(target string, index int, seen map[string]int) string {
	host := target
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		host = fmt.Sprintf("node-%d", index+1)
	}
	seen[host]++
	if seen[host] > 1 {
		return fmt.Sprintf("%s-%d", host, seen[host])
	}
	return host
}
