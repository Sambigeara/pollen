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

func newIDCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "id",
		Short: "Show local node identity public key",
		RunE:  withEnv(false, runID),
	}
}

func newBootstrapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap relays and joiners",
	}

	sshCmd := &cobra.Command{
		Use:   "ssh <host>",
		Short: "Bootstrap a relay over SSH",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(false, runBootstrapSSH),
	}
	sshCmd.Flags().Int("relay-port", config.DefaultBootstrapPort, "Relay UDP port to advertise")
	sshCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the relay peer")
	sshCmd.Flags().Bool("admin", false, "Delegate admin authority to the relay")

	cmd.AddCommand(sshCmd)
	return cmd
}

func newClusterCmds() []*cobra.Command {
	purgeCmd := &cobra.Command{Use: "purge", Short: "Delete local cluster state", RunE: withEnv(true, runPurge)}
	purgeCmd.Flags().Bool("all", false, "Also delete local node identity keys")
	purgeCmd.Flags().Bool("yes", false, "Skip interactive confirmation")

	joinCmd := &cobra.Command{Use: "join <token>", Short: "Join a cluster using a token", Args: cobra.ExactArgs(1), RunE: withEnv(true, runJoin)}
	joinCmd.Flags().Bool("no-up", false, "Enroll credentials without starting the daemon")
	joinCmd.Flags().Bool("public", false, "Mark this node as publicly accessible (relay)")

	inviteCmd := &cobra.Command{Use: "invite [subject-pub]", Short: "Generate an invite token", Args: cobra.RangeArgs(0, 1), RunE: withEnv(false, runInvite)}
	inviteCmd.Flags().String("subject", "", "Optional hex node public key to bind invite")
	inviteCmd.Flags().Duration("ttl", defaultInviteTTL, "Invite token validity duration")
	inviteCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the invited peer")
	inviteCmd.Flags().StringArray("attr", nil, "Cert attributes: key=value, JSON, or - for stdin")

	adminCmd := &cobra.Command{Use: "admin", Short: "Manage admin keys"}
	adminCmd.AddCommand(&cobra.Command{Use: "keygen", Short: "Generate the local admin key", RunE: withEnv(true, runAdminKeygen), Hidden: true})
	adminCmd.AddCommand(&cobra.Command{Use: "set-cert <admin-cert-b64>", Short: "Install a delegated admin certificate", Args: cobra.ExactArgs(1), RunE: withEnv(true, runAdminSetCert), Hidden: true})

	certCmd := &cobra.Command{Use: "cert", Short: "Certificate management"}
	certIssueCmd := &cobra.Command{
		Use:   "issue <peer-id>",
		Short: "Issue a new certificate to a peer",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(false, runCertIssue),
	}
	certIssueCmd.Flags().Bool("admin", false, "Issue with full admin capabilities")
	certIssueCmd.Flags().StringArray("attr", nil, "Cert attributes: key=value, JSON, or - for stdin")
	certCmd.AddCommand(certIssueCmd)

	return []*cobra.Command{
		{Use: "init", Short: "Initialize local root cluster state", RunE: withEnv(true, runInit)},
		purgeCmd,
		joinCmd,
		inviteCmd,
		adminCmd,
		certCmd,
		newBootstrapCmd(),
	}
}

func runID(cmd *cobra.Command, _ []string, env *cliEnv) error {
	pub, err := auth.ReadIdentityPub(env.dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, pub, err = auth.EnsureIdentityKey(env.dir); err != nil {
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

	_, pub, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}

	existing, err := auth.LoadNodeCredentials(env.dir)
	if err == nil {
		_, adminPub, adminErr := auth.LoadAdminKey(env.dir)
		if adminErr == nil && slices.Equal(adminPub, existing.RootPub()) {
			fmt.Fprintf(cmd.OutOrStdout(), "already initialized as root cluster\nroot_pub: %s\ncluster_id: %x\n", hex.EncodeToString(adminPub), sha256.Sum256(existing.RootPub()))
			return nil
		}
		return errors.New("node is already enrolled in a cluster; run `pln purge` before initializing a new root cluster")
	}

	creds, err := auth.EnsureLocalRootCredentials(env.dir, pub, time.Now(), config.DefaultMembershipTTL, config.DefaultDelegationTTL)
	if err != nil {
		return err
	}

	_, adminPub, _ := auth.LoadAdminKey(env.dir)
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
	privKey, pubKey, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}

	tkn, err := resolveJoinToken(cmd.Context(), privKey, args[0])
	if err != nil {
		return fmt.Errorf("resolve token: %w", err)
	}

	if _, credErr := auth.EnrollNodeCredentials(env.dir, pubKey, tkn, time.Now()); credErr != nil {
		if errors.Is(credErr, auth.ErrDifferentCluster) {
			_ = runPurge(cmd, nil, env) // Clear old cluster state
			_, credErr = auth.EnrollNodeCredentials(env.dir, pubKey, tkn, time.Now())
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

	nodePriv, _, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}

	signer, err := auth.NewDelegationSigner(env.dir, nodePriv, config.DefaultDelegationTTL)
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
	_, pub, err := auth.EnsureAdminKey(env.dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "admin_pub: %s\nnote: this does not grant signing authority\ninstall a delegated admin cert with `pln admin set-cert <admin-cert-b64>`\n", hex.EncodeToString(pub))
	return nil
}

func runAdminSetCert(cmd *cobra.Command, args []string, env *cliEnv) error {
	_, nodePub, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}
	creds, err := auth.LoadNodeCredentials(env.dir)
	if err != nil {
		return errors.New("node credentials not initialized; run `pln init` or `pln join <token>` first")
	}
	if err := auth.InstallDelegationCert(env.dir, args[0], creds.RootPub(), nodePub, time.Now()); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "delegated admin certificate installed")
	return nil
}

// --- Bootstrap & Remote Operations ---

func runBootstrapSSH(cmd *cobra.Command, args []string, env *cliEnv) error {
	host := args[0]
	relayPort, _ := cmd.Flags().GetInt("relay-port")
	if relayPort < minPort || relayPort > maxPort {
		return fmt.Errorf("invalid relay port %d", relayPort)
	}

	inferredAddr, err := inferRelayAddrFromSSHTarget(host, relayPort)
	if err != nil {
		return err
	}
	relayAddrs := []string{inferredAddr}

	fmt.Fprintln(cmd.OutOrStdout(), "ensuring pln is installed on remote host...")
	if err := ensureRemotePollen(cmd.Context(), host); err != nil {
		return err
	}

	idCmd := sshPln(cmd.Context(), host, "id")
	var idStdout, idStderr bytes.Buffer
	idCmd.Stdout = &idStdout
	idCmd.Stderr = &idStderr
	if err := idCmd.Run(); err != nil {
		return fmt.Errorf("failed to fetch remote node identity: %w\n%s", err, strings.TrimSpace(idStderr.String()))
	}

	relayPeerKey, err := types.PeerKeyFromString(strings.TrimSpace(idStdout.String()))
	if err != nil {
		return err
	}
	relayPub := ed25519.PublicKey(relayPeerKey.Bytes())
	expireAfter, _ := cmd.Flags().GetDuration("expire-after")

	delegateAdmin, _ := cmd.Flags().GetBool("admin")

	if err := bootstrapAccept(cmd, env, relayPub, relayAddrs, host, expireAfter, delegateAdmin); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "relay bootstrapped: %s\n", host)
	fmt.Fprintln(cmd.OutOrStdout(), "hint: reconnect SSH to use `pln` without sudo (new group membership requires a fresh login)")
	return nil
}

func bootstrapAccept(cmd *cobra.Command, env *cliEnv, relayPub ed25519.PublicKey, relayAddrs []string, sshTarget string, expireAfter time.Duration, delegateAdmin bool) error {
	nodePriv, _, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}
	signer, err := auth.NewDelegationSigner(env.dir, nodePriv, config.DefaultDelegationTTL)
	if err != nil {
		return errors.New("this node cannot issue tokens; only delegated admins can sign enrollment tokens")
	}

	seedToken, err := createJoinTokenWithSigner(signer, config.DefaultMembershipTTL, relayPub, 1*time.Minute, expireAfter, nil, nil)
	if err != nil {
		return fmt.Errorf("create relay seed token: %w", err)
	}

	if err := bootstrapRelayOverSSH(cmd, env, sshTarget, seedToken, relayPub, delegateAdmin); err != nil {
		return err
	}

	env.cfg.RememberBootstrapPeer(&admissionv1.BootstrapPeer{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	})
	if err := config.Save(env.dir, env.cfg); err != nil {
		return fmt.Errorf("save relay bootstrap details: %w", err)
	}

	if nodeSocketActive(filepath.Join(env.dir, socketName)) {
		if _, err := env.client.ConnectPeer(cmd.Context(), connect.NewRequest(&controlv1.ConnectPeerRequest{
			PeerPub: relayPub,
			Addrs:   relayAddrs,
		})); err != nil {
			return fmt.Errorf("connect running node to relay: %w", err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "relay is ready; connected to running node")
		return nil
	}

	_, localPub, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}

	joinToken, err := createJoinTokenWithSigner(signer, config.DefaultMembershipTTL, localPub, defaultInviteTTL, expireAfter, []*admissionv1.BootstrapPeer{{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	}}, nil)
	if err != nil {
		return fmt.Errorf("create local join token: %w", err)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "relay is ready; starting local node")
	if err := runJoin(cmd, []string{joinToken}, env); err != nil {
		return err
	}
	return servicectl("start", cmd)
}

func bootstrapRelayOverSSH(cmd *cobra.Command, env *cliEnv, sshTarget, seedToken string, relayPub ed25519.PublicKey, delegateAdmin bool) error {
	ctx := cmd.Context()
	if out, err := sshPln(ctx, sshTarget, "join", "--no-up", "--public", seedToken).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to enroll relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if out, err := sshRoot(ctx, sshTarget, "sudo pln up -d").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if err := waitForRelayReady(ctx, sshTarget); err != nil {
		return err
	}
	if delegateAdmin {
		if err := provisionRelayAdminDelegation(ctx, env, sshTarget, relayPub); err != nil {
			return err
		}
		if out, err := sshRoot(ctx, sshTarget, "sudo pln restart").CombinedOutput(); err != nil {
			return fmt.Errorf("failed to restart relay node: %w\n%s", err, strings.TrimSpace(string(out)))
		}
		return waitForRelayReady(ctx, sshTarget)
	}
	return nil
}

func provisionRelayAdminDelegation(ctx context.Context, env *cliEnv, sshTarget string, relayPub ed25519.PublicKey) error {
	nodePriv, _, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return err
	}
	signer, err := auth.NewDelegationSigner(env.dir, nodePriv, config.DefaultDelegationTTL)
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

func runCertIssue(cmd *cobra.Command, args []string, env *cliEnv) error {
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
		return nil, fmt.Errorf("failed to decode token")
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
	if exec.CommandContext(ctx, "ssh", sshTarget, "which pln >/dev/null 2>&1").Run() == nil {
		if out, err := sshRoot(ctx, sshTarget, "pln provision").CombinedOutput(); err != nil {
			return fmt.Errorf("remote provision failed: %w\n%s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://raw.githubusercontent.com/%s/main/scripts/install.sh", "sambigeara/pollen"), nil)
	if reqErr != nil {
		return fmt.Errorf("failed to fetch install script: %w", reqErr)
	}
	resp, reqErr := (&http.Client{Timeout: httpFetchTimeout}).Do(req)
	if reqErr != nil || resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch install script")
	}
	defer resp.Body.Close()
	script, _ := io.ReadAll(resp.Body)

	args := []string{sshTarget, "bash", "-s", "--"}
	if version != "dev" {
		args = append(args, "--version", version)
	}

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = bytes.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to install pln on remote host: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func waitForRelayReady(ctx context.Context, sshTarget string) error {
	readyCtx, cancel := context.WithTimeout(ctx, relayReadyTimeout)
	defer cancel()

	checkCmd := "for i in $(seq 1 20); do if { sudo test -S /var/lib/pln/pln.sock || [ -S \"$HOME/.pln/pln.sock\" ]; } && sudo pln status --all >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1"
	if out, err := sshRoot(readyCtx, sshTarget, checkCmd).CombinedOutput(); err != nil {
		logOut, _ := sshRoot(ctx, sshTarget, "journalctl -u pln -n 120 --no-pager 2>/dev/null || tail -n 120 /opt/homebrew/var/log/pln.log 2>/dev/null || tail -n 120 /usr/local/var/log/pln.log 2>/dev/null || true").CombinedOutput()
		return fmt.Errorf("relay failed to become ready\nstatus output: %s\nrelay log:\n%s", strings.TrimSpace(string(out)), strings.TrimSpace(string(logOut)))
	}
	return nil
}

func sshPln(ctx context.Context, sshTarget string, args ...string) *exec.Cmd {
	sshArgs := append([]string{sshTarget, "sudo", "-u", "pln", "pln"}, args...)
	return exec.CommandContext(ctx, "ssh", sshArgs...)
}

func sshRoot(ctx context.Context, sshTarget, shellCmd string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", sshTarget, shellCmd)
}

func inferRelayAddrFromSSHTarget(target string, relayPort int) (string, error) {
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
	return net.JoinHostPort(host, strconv.Itoa(relayPort)), nil
}
