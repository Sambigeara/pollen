package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/types"
)

func newBootstrapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap relays and joiners",
	}

	sshCmd := &cobra.Command{
		Use:   "ssh <host>",
		Short: "Bootstrap a relay over SSH",
		Args:  cobra.ExactArgs(1),
		Run:   runBootstrapSSH,
	}
	sshCmd.Flags().Int("relay-port", config.DefaultBootstrapPort, "Relay UDP port to advertise")
	sshCmd.Flags().Duration("expire-after", 0, "Hard access expiry for the relay peer (e.g. 24h)")

	cmd.AddCommand(sshCmd)
	return cmd
}

func runBootstrapSSH(cmd *cobra.Command, args []string) {
	host := args[0]

	relayPort, err := cmd.Flags().GetInt("relay-port")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if relayPort < minPort || relayPort > maxPort {
		fmt.Fprintf(cmd.ErrOrStderr(), "invalid relay port %d\n", relayPort)
		os.Exit(1)
	}

	inferredAddr, err := inferRelayAddrFromSSHTarget(host, relayPort)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	relayAddrs := []string{inferredAddr}

	ctx := cmd.Context()

	fmt.Fprintln(cmd.OutOrStdout(), "ensuring pln is installed on remote host...")
	if err := ensureRemotePollen(ctx, host); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	idCmd := sshPln(ctx, host, "id")
	var idStdout, idStderr bytes.Buffer
	idCmd.Stdout = &idStdout
	idCmd.Stderr = &idStderr
	if err := idCmd.Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to fetch remote node identity: %v\n%s\n", err, strings.TrimSpace(idStderr.String()))
		os.Exit(1)
	}

	relayPeerKey, err := types.PeerKeyFromString(strings.TrimSpace(idStdout.String()))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	relayPub := ed25519.PublicKey(relayPeerKey.Bytes())

	expireAfter, err := getExpireAfterFlag(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if err := bootstrapAccept(cmd, relayPub, relayAddrs, host, expireAfter); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "relay bootstrapped: %s\n", host)
	fmt.Fprintln(cmd.OutOrStdout(), "hint: reconnect SSH to use `pln` without sudo (new group membership requires a fresh login)")
}

func bootstrapAccept(cmd *cobra.Command, relayPub ed25519.PublicKey, relayAddrs []string, sshTarget string, expireAfter time.Duration) error {
	issuerCtx, err := loadTokenIssuerContext(cmd)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("this node cannot issue join tokens; only delegated admins can sign enrollment tokens")
		}
		return err
	}

	seedToken, err := createJoinTokenWithSigner(issuerCtx.signer, issuerCtx.cfg.CertTTLs.MembershipTTL(), relayPub, defaultBootstrapJoinTokenTTL, expireAfter, nil)
	if err != nil {
		return fmt.Errorf("create relay seed token: %w", err)
	}

	if err := bootstrapRelayOverSSH(cmd, sshTarget, seedToken, relayPub); err != nil {
		return err
	}

	if err := saveBootstrapPeer(issuerCtx.pollenDir, &admissionv1.BootstrapPeer{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	}); err != nil {
		return fmt.Errorf("save relay bootstrap details: %w", err)
	}

	sockPath := filepath.Join(issuerCtx.pollenDir, socketName)
	if active, _ := nodeSocketActive(sockPath); active {
		client := newControlClient(cmd)
		ctx := cmd.Context()
		if _, err := client.ConnectPeer(ctx, connect.NewRequest(&controlv1.ConnectPeerRequest{
			PeerId: relayPub,
			Addrs:  relayAddrs,
		})); err != nil {
			return fmt.Errorf("connect running node to relay: %w", err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "relay is ready; connected to running node")
		return nil
	}

	_, localPub, err := node.GenIdentityKey(issuerCtx.pollenDir)
	if err != nil {
		return err
	}

	joinToken, err := createJoinTokenWithSigner(issuerCtx.signer, issuerCtx.cfg.CertTTLs.MembershipTTL(), localPub, defaultJoinTokenTTL, expireAfter, []*admissionv1.BootstrapPeer{{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	}})
	if err != nil {
		return fmt.Errorf("create local join token: %w", err)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "relay is ready; starting local node")
	if err := enrollToken(cmd.Context(), issuerCtx.pollenDir, joinToken); err != nil {
		return err
	}
	return servicectl("start", cmd)
}

// sshPln runs a pln subcommand on the remote host as the pln system user.
// All data-plane operations (key generation, enrollment, admin cert management)
// must go through this so files are created with correct ownership.
func sshPln(ctx context.Context, sshTarget string, args ...string) *exec.Cmd {
	sshArgs := make([]string, 0, 5+len(args)) //nolint:mnd
	sshArgs = append(sshArgs, sshTarget, "sudo", "-u", "pln", "pln")
	sshArgs = append(sshArgs, args...)
	return exec.CommandContext(ctx, "ssh", sshArgs...)
}

// sshRoot runs a command on the remote host as root via sudo.
// Use for service management (up, down).
func sshRoot(ctx context.Context, sshTarget, shellCmd string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", sshTarget, shellCmd)
}

func bootstrapRelayOverSSH(cmd *cobra.Command, sshTarget, seedToken string, relayPub ed25519.PublicKey) error {
	ctx := cmd.Context()

	// Enroll the relay into the cluster (as pln user for correct file ownership),
	// then start the daemon (as root for systemctl).
	if out, err := sshPln(ctx, sshTarget, "join", "--no-up", "--public", seedToken).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to enroll relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if out, err := sshRoot(ctx, sshTarget, "sudo pln up -d").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	if err := waitForRelayReady(cmd, sshTarget); err != nil {
		return err
	}
	if err := provisionRelayAdminDelegation(cmd, sshTarget, relayPub); err != nil {
		return err
	}
	if out, err := sshRoot(ctx, sshTarget, "sudo pln restart").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to restart relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return waitForRelayReady(cmd, sshTarget)
}

func createJoinTokenWithSigner(signer *auth.DelegationSigner, defaultMembershipTTL time.Duration, subjectPub ed25519.PublicKey, ttl, expireAfter time.Duration, bootstrap []*admissionv1.BootstrapPeer) (string, error) {
	if ttl <= 0 {
		return "", errors.New("token ttl must be positive")
	}

	var accessDeadline time.Time
	if expireAfter > 0 {
		accessDeadline = time.Now().Add(expireAfter)
	}

	token, err := auth.IssueJoinTokenWithIssuer(
		signer.Priv,
		signer.Trust,
		signer.Issuer,
		subjectPub,
		bootstrap,
		time.Now(),
		ttl,
		defaultMembershipTTL,
		accessDeadline,
	)
	if err != nil {
		return "", err
	}

	return auth.EncodeJoinToken(token)
}

func ensureRemotePollen(ctx context.Context, sshTarget string) error {
	script, err := fetchInstallScript()
	if err != nil {
		return err
	}

	args := []string{sshTarget, "bash", "-s", "--"}
	if version != "dev" {
		args = append(args, "--version", version)
	}

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = bytes.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to install pln on remote host: %w\n%s", err, strings.TrimSpace(string(out)))
	}

	return nil
}

func provisionRelayAdminDelegation(cmd *cobra.Command, sshTarget string, relayPub ed25519.PublicKey) error {
	ctx := cmd.Context()

	issuerCtx, err := loadTokenIssuerContext(cmd)
	if err != nil {
		return err
	}
	if !slices.Equal(issuerCtx.signer.Trust.GetRootPub(), issuerCtx.signer.Issuer.GetClaims().GetSubjectPub()) {
		return errors.New("only root admin can delegate relay admin certs")
	}

	cert, err := auth.IssueDelegationCert(
		issuerCtx.signer.Priv,
		nil,
		issuerCtx.signer.Trust.GetClusterId(),
		relayPub,
		auth.FullCapabilities(),
		time.Now().Add(-time.Minute),
		time.Now().Add(issuerCtx.cfg.CertTTLs.DelegationTTL()),
		time.Time{},
	)
	if err != nil {
		return err
	}

	encoded, err := auth.MarshalDelegationCertBase64(cert)
	if err != nil {
		return err
	}

	setCertOut, err := sshPln(ctx, sshTarget, "admin", "set-cert", encoded).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to install relay admin cert: %w\n%s", err, strings.TrimSpace(string(setCertOut)))
	}

	return nil
}

func waitForRelayReady(cmd *cobra.Command, sshTarget string) error {
	ctx := cmd.Context()

	readyCtx, cancel := context.WithTimeout(ctx, bootstrapStatusWait)
	defer cancel()

	checkCmd := "for i in $(seq 1 20); do if { sudo test -S /var/lib/pln/pln.sock || [ -S \"$HOME/.pln/pln.sock\" ]; } && sudo pln status --all >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1"
	out, err := sshRoot(readyCtx, sshTarget, checkCmd).CombinedOutput()
	if err == nil {
		return nil
	}

	logCmd := "journalctl -u pln -n 120 --no-pager 2>/dev/null || tail -n 120 /opt/homebrew/var/log/pln.log 2>/dev/null || tail -n 120 /usr/local/var/log/pln.log 2>/dev/null || true"
	logOut, _ := sshRoot(ctx, sshTarget, logCmd).CombinedOutput()
	return fmt.Errorf("relay failed to become ready\nstatus output: %s\nrelay log:\n%s", strings.TrimSpace(string(out)), strings.TrimSpace(string(logOut)))
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
