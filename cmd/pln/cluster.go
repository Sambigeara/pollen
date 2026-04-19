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
		RunE:  withEnv(runID, localOnly()),
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
		Args:  cobra.MinimumNArgs(1),
		RunE:  withEnv(runBootstrapSSH, localOnly()),
	}
	sshCmd.Flags().Int("relay-port", config.DefaultBootstrapPort, "Relay UDP port to advertise")
	sshCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the relay peer")
	sshCmd.Flags().Bool("admin", false, "Delegate admin authority to the relay")

	cmd.AddCommand(sshCmd)
	return cmd
}

func newClusterCmds() []*cobra.Command {
	purgeCmd := &cobra.Command{Use: "purge", Short: "Delete local cluster state", RunE: withEnv(runPurge, wantsRoot(), localOnly())}
	purgeCmd.Flags().Bool("all", false, "Also delete local node identity keys")
	purgeCmd.Flags().Bool("yes", false, "Skip interactive confirmation")

	joinCmd := &cobra.Command{Use: "join <token>", Short: "Join a cluster using a token", Args: cobra.ExactArgs(1), RunE: withEnv(runJoin, wantsRoot(), localOnly())}
	joinCmd.Flags().Bool("no-up", false, "Enroll credentials without starting the daemon")
	joinCmd.Flags().Bool("public", false, "Mark this node as publicly accessible (relay)")

	inviteCmd := &cobra.Command{Use: "invite [subject-pub]", Short: "Generate an invite token", Args: cobra.RangeArgs(0, 1), RunE: withEnv(runInvite)}
	inviteCmd.Flags().String("subject", "", "Optional hex node public key to bind invite")
	inviteCmd.Flags().Duration("ttl", defaultInviteTTL, "Invite token validity duration")
	inviteCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the invited peer")
	inviteCmd.Flags().StringArray("attr", nil, "Cert attributes: key=value, JSON, or - for stdin")

	adminCmd := &cobra.Command{Use: "admin", Short: "Manage admin keys"}
	adminCmd.AddCommand(&cobra.Command{Use: "keygen", Short: "Generate the local admin key", RunE: withEnv(runAdminKeygen, wantsRoot(), localOnly()), Hidden: true})
	adminCmd.AddCommand(&cobra.Command{Use: "set-cert <admin-cert-b64>", Short: "Install a delegated admin certificate", Args: cobra.ExactArgs(1), RunE: withEnv(runAdminSetCert, wantsRoot(), localOnly()), Hidden: true})

	grantCmd := &cobra.Command{
		Use:   "grant <peer-id>",
		Short: "Grant a certificate to a connected peer",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runGrant),
	}
	grantCmd.Flags().Bool("admin", false, "Issue with full admin capabilities")
	grantCmd.Flags().StringArray("attr", nil, "Cert attributes: key=value, JSON, or - for stdin")

	return []*cobra.Command{
		{Use: "init", Short: "Initialize local root cluster state", RunE: withEnv(runInit, wantsRoot(), localOnly())},
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

	creds, err := auth.EnsureLocalRootCredentials(identityDir, pub, time.Now(), config.DefaultMembershipTTL, config.DefaultDelegationTTL)
	if err != nil {
		return err
	}

	_, adminPub, _ := auth.LoadAdminKey(identityDir)
	fmt.Fprintf(cmd.OutOrStdout(), "initialized root cluster\nroot_pub: %s\ncluster_id: %x\n", hex.EncodeToString(adminPub), sha256.Sum256(creds.RootPub()))
	return nil
}

func runPurge(cmd *cobra.Command, _ []string, env *cliEnv) error {
	all, _ := cmd.Flags().GetBool("all")
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
		"invites", "cas", socketName,
	}
	if all {
		paths = append(paths, "keys/ed25519.key", "keys/ed25519.pub")
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

	for _, bp := range tkn.GetClaims().GetBootstrap() {
		env.cfg.RememberBootstrapPeer(bp)
	}

	if public, _ := cmd.Flags().GetBool("public"); public {
		env.cfg.Public = true
	}
	if err := config.Save(env.dir, env.cfg); err != nil {
		return err
	}

	if noUp, _ := cmd.Flags().GetBool("no-up"); noUp {
		fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; run `pln up -d` to start the node")
		return nil
	}

	if nodeSocketActive(filepath.Join(env.dir, socketName)) {
		if err := servicectl("restart", cmd); err != nil {
			return fmt.Errorf("joined cluster but failed to restart daemon: %w", err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "joined cluster; daemon restarted")
		return nil
	}

	fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; starting daemon")
	return servicectl("start", cmd)
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

	signer, err := auth.NewDelegationSigner(identityDir, nodePriv, config.DefaultDelegationTTL)
	if err != nil {
		return errors.New("this node cannot issue invites; only delegated admins can sign invite tokens")
	}

	bootstrap, err := resolveBootstrapPeers(cmd.Context(), env)
	if err != nil {
		return err
	}

	attrs, err := parseAttributes(cmd)
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

// --- Bootstrap & Remote Operations ---

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
	signer, err := auth.NewDelegationSigner(identityDir, nodePriv, config.DefaultDelegationTTL)
	if err != nil {
		return errors.New("this node cannot issue tokens; only delegated admins can sign enrollment tokens")
	}

	results := make([]bootstrapResult, len(specs))
	var wg sync.WaitGroup
	for i, spec := range specs {
		wg.Go(func() {
			results[i] = bootstrapRemote(cmd.Context(), signer, env, spec, relayPort, expireAfter, delegateAdmin)
		})
	}
	wg.Wait()

	out := cmd.OutOrStdout()
	var succeeded, failed int
	var failErrs []error
	var bootstrappedPeers []bootstrapResult
	for _, r := range results {
		if r.err != nil {
			failed++
			fmt.Fprintf(out, "  %-40s failed (%v)\n", r.target, r.err)
			failErrs = append(failErrs, fmt.Errorf("%s: %w", r.target, r.err))
			continue
		}
		succeeded++
		fmt.Fprintf(out, "  %-40s ok\n", r.target)
		env.cfg.RememberBootstrapPeer(&admissionv1.BootstrapPeer{
			PeerPub: append([]byte(nil), r.peerPub...),
			Addrs:   append([]string(nil), r.addrs...),
		})
		bootstrappedPeers = append(bootstrappedPeers, r)
	}
	fmt.Fprintln(out)

	if len(bootstrappedPeers) == 0 {
		return errors.Join(failErrs...)
	}

	if err := config.Save(env.dir, env.cfg); err != nil {
		return fmt.Errorf("save bootstrap peer details: %w", err)
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

// bootstrapRemote is safe to call concurrently for different targets.
func bootstrapRemote(ctx context.Context, signer *auth.DelegationSigner, env *cliEnv, spec sshTargetSpec, relayPort int, expireAfter time.Duration, delegateAdmin bool) bootstrapResult {
	target := spec.target
	fail := func(err error) bootstrapResult {
		return bootstrapResult{target: target, err: err}
	}

	inferredAddr, err := inferRelayAddrFromSSHTarget(ctx, target, relayPort)
	if err != nil {
		return fail(err)
	}
	relayAddrs := []string{inferredAddr}

	relayHost, _, _ := net.SplitHostPort(inferredAddr)
	relayIP := net.ParseIP(relayHost)
	public := relayIP != nil && !relayIP.IsPrivate() && !relayIP.IsLoopback()

	if err := requireRemoteSudo(ctx, target); err != nil {
		return fail(err)
	}

	if err := ensureRemotePollen(ctx, target); err != nil {
		return fail(err)
	}

	idCmd := sshPln(ctx, target, "id")
	var idStdout, idStderr bytes.Buffer
	idCmd.Stdout = &idStdout
	idCmd.Stderr = &idStderr
	if err := idCmd.Run(); err != nil {
		return fail(fmt.Errorf("failed to fetch remote node identity: %w\n%s", err, strings.TrimSpace(idStderr.String())))
	}

	relayPeerKey, err := types.PeerKeyFromString(strings.TrimSpace(idStdout.String()))
	if err != nil {
		return fail(err)
	}
	relayPub := ed25519.PublicKey(relayPeerKey.Bytes())

	seedToken, err := createJoinTokenWithSigner(signer, config.DefaultMembershipTTL, relayPub, 1*time.Minute, expireAfter, nil, nil)
	if err != nil {
		return fail(fmt.Errorf("create seed token: %w", err))
	}

	if err := bootstrapRelayOverSSH(ctx, env, target, seedToken, relayPub, delegateAdmin, spec.nodeName, public); err != nil {
		return fail(err)
	}

	return bootstrapResult{
		target:  target,
		peerPub: relayPub,
		addrs:   relayAddrs,
	}
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
	if delegateAdmin {
		if err := provisionRelayAdminDelegation(ctx, env, sshTarget, relayPub); err != nil {
			return err
		}
		if out, err := sshSudo(ctx, sshTarget, "pln", "restart").CombinedOutput(); err != nil {
			return fmt.Errorf("failed to restart relay node: %w\n%s", err, strings.TrimSpace(string(out)))
		}
		return waitForRelayReady(ctx, sshTarget)
	}
	return nil
}

func provisionRelayAdminDelegation(ctx context.Context, env *cliEnv, sshTarget string, relayPub ed25519.PublicKey) error {
	identityDir := auth.IdentityPath(env.dir)
	nodePriv, _, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return err
	}
	signer, err := auth.NewDelegationSigner(identityDir, nodePriv, config.DefaultDelegationTTL)
	if err != nil || !signer.IsRoot() {
		return errors.New("only root admin can delegate relay admin certs")
	}

	cert, err := signer.IssueMemberCert(relayPub, auth.FullCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(config.DefaultDelegationTTL), time.Time{})
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

	attrs, err := parseAttributes(cmd)
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
		return fmt.Errorf("no peer matching %q", prefix)
	}
	if len(matches) > 1 {
		return fmt.Errorf("ambiguous peer prefix %q matches %d peers", prefix, len(matches))
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

func parseAttributes(cmd *cobra.Command) (*structpb.Struct, error) {
	vals, _ := cmd.Flags().GetStringArray("attr")
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
				return nil, fmt.Errorf("reading attributes from stdin: %w", err)
			}
			var obj map[string]any
			if err := json.Unmarshal(data, &obj); err != nil {
				return nil, fmt.Errorf("invalid JSON on stdin: %w", err)
			}
			maps.Copy(merged, obj)
		case strings.HasPrefix(v, "{"):
			var obj map[string]any
			if err := json.Unmarshal([]byte(v), &obj); err != nil {
				return nil, fmt.Errorf("invalid JSON in --attr: %w", err)
			}
			maps.Copy(merged, obj)
		default:
			k, val, ok := strings.Cut(v, "=")
			if !ok || k == "" {
				return nil, fmt.Errorf("invalid attribute: missing '=' in %q", v)
			}
			merged[k] = val
		}
	}
	s, err := structpb.NewStruct(merged)
	if err != nil {
		return nil, fmt.Errorf("building attributes: %w", err)
	}
	if err := auth.ValidateAttributes(s); err != nil {
		return nil, err
	}
	return s, nil
}

// --- Utilities ---

func resolveBootstrapPeers(ctx context.Context, env *cliEnv) ([]*admissionv1.BootstrapPeer, error) {
	if len(env.cfg.BootstrapPeers) > 0 {
		peers := make([]*admissionv1.BootstrapPeer, 0, len(env.cfg.BootstrapPeers))
		for peerHex, addrs := range env.cfg.BootstrapPeers {
			pk, _ := types.PeerKeyFromString(peerHex)
			peers = append(peers, &admissionv1.BootstrapPeer{
				PeerPub: pk.Bytes(),
				Addrs:   addrs,
			})
		}
		return peers, nil
	}

	resp, err := env.client.GetBootstrapInfo(ctx, connect.NewRequest(&controlv1.GetBootstrapInfoRequest{}))
	if err != nil {
		return nil, err
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
		if out, err := sshSudo(ctx, sshTarget, "pln", "provision").CombinedOutput(); err != nil {
			return fmt.Errorf("remote provision failed: %w\n%s", err, strings.TrimSpace(string(out)))
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

	checkCmd := "for i in $(seq 1 20); do if { test -S /var/lib/pln/pln.sock || [ -S \"$HOME/.pln/pln.sock\" ]; } && pln status --all >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1"
	if out, err := sshSudoShell(readyCtx, sshTarget, checkCmd).CombinedOutput(); err != nil {
		logOut, _ := sshSudoShell(ctx, sshTarget, "journalctl -u pln -n 120 --no-pager 2>/dev/null || tail -n 120 /opt/homebrew/var/log/pln.log 2>/dev/null || tail -n 120 /usr/local/var/log/pln.log 2>/dev/null || true").CombinedOutput()
		return fmt.Errorf("relay failed to become ready\nstatus output: %s\nrelay log:\n%s", strings.TrimSpace(string(out)), strings.TrimSpace(string(logOut)))
	}
	return nil
}

// sshCmd builds an ssh(1) invocation with the standard base flags and the given
// remote command tokens appended verbatim.
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
