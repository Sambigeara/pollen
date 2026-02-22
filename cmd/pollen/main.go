package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenDir  = ".pollen"
	socketName = "pollen.sock"

	controlClientTimeout         = 10 * time.Second
	passiveGossipInterval        = 10 * time.Second
	defaultJoinTokenTTL          = 5 * time.Minute
	defaultBootstrapJoinTokenTTL = 1 * time.Minute
	bootstrapStatusWait          = 20 * time.Second
	minPort                      = 1
	maxPort                      = 65535
	defaultRepo                  = "sambigeara/pollen"
	scriptFetchTimeout           = 30 * time.Second
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted")

	rootCmd.AddCommand(
		newVersionCmd(),
		newInitCmd(),
		newPurgeCmd(),
		newAdminCmd(),
		newIDCmd(),
		newBootstrapCmd(),
		newUpCmd(),
		newDownCmd(),
		newInviteCmd(),
		newStatusCmd(),
		newServeCmd(),
		newUnserveCmd(),
		newConnectCmd(),
		newDaemonCmd(),
		newUpgradeCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("failed to execute command: %q", err)
	}
}

func defaultRootDir() string {
	base, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("unable to retrieve user config dir: %v", err)
	}
	return filepath.Join(base, pollenDir)
}

func newAdminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Manage admin keys",
	}

	cmd.AddCommand(&cobra.Command{
		Use:    "keygen",
		Short:  "Generate the local admin key",
		Run:    runAdminKeygen,
		Hidden: true,
	})

	cmd.AddCommand(&cobra.Command{
		Use:    "set-cert <admin-cert-b64>",
		Short:  "Install a delegated admin certificate",
		Args:   cobra.ExactArgs(1),
		Run:    runAdminSetCert,
		Hidden: true,
	})

	return cmd
}

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show Pollen version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			short, err := cmd.Flags().GetBool("short")
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), err)
				return
			}

			if short {
				fmt.Fprintln(cmd.OutOrStdout(), version)
				return
			}

			fmt.Fprintf(cmd.OutOrStdout(), "version: %s\ncommit: %s\ndate: %s\n", version, commit, date)
		},
	}

	cmd.Flags().Bool("short", false, "Print version only")
	return cmd
}

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize local root cluster state",
		Run:   runInit,
	}
}

func newPurgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge",
		Short: "Delete local cluster state",
		Run:   runPurge,
	}
	cmd.Flags().Bool("all", false, "Also delete local node identity keys")
	cmd.Flags().Bool("yes", false, "Skip interactive confirmation")
	return cmd
}

func newIDCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "id",
		Short: "Show local node identity public key",
		Run:   runID,
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
		Run:   runBootstrapSSH,
	}
	sshCmd.Flags().Int("relay-port", config.DefaultBootstrapPort, "Relay UDP port to advertise")

	cmd.AddCommand(sshCmd)
	return cmd
}

func newUpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start a Pollen node",
		Run:   runUp,
	}
	cmd.Flags().BoolP("daemon", "d", false, "Start as a background service")
	cmd.Flags().Int("port", config.DefaultBootstrapPort, "Listening port")
	cmd.Flags().IPSlice("ips", []net.IP{}, "Advertised IPs")
	cmd.Flags().String("join", "", "Optional join or invite token for one-shot enrollment")
	return cmd
}

func runUp(cmd *cobra.Command, args []string) {
	daemon, _ := cmd.Flags().GetBool("daemon")

	if daemon {
		// -d is incompatible with foreground-only flags.
		for _, name := range []string{"port", "ips", "join"} {
			if cmd.Flags().Changed(name) {
				fmt.Fprintf(cmd.ErrOrStderr(), "-d/--daemon cannot be combined with --%s; configure the service via `pollen daemon install`\n", name)
				return
			}
		}
		runUpDaemon(cmd)
		return
	}

	// Foreground mode: warn if a daemon instance already owns the socket.
	pollenDir, err := pollenPath(cmd)
	if err == nil {
		sockPath := filepath.Join(pollenDir, socketName)
		if active, _ := nodeSocketActive(sockPath); active {
			if daemonServiceInstalled() {
				fmt.Fprintln(cmd.ErrOrStderr(), "a daemon instance is already running; use `pollen down` to stop it or `pollen up -d` to manage it")
				return
			}
		}
	}

	runNode(cmd, args)
}

func newDownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "down",
		Short: "Gracefully stop the local running node",
		Run:   runDown,
	}
}

func newInviteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "invite [subject-pub]",
		Short: "Generate an invite token (open or subject-bound)",
		Args:  cobra.RangeArgs(0, 1), //nolint:mnd
		Run:   runInvite,
	}
	cmd.Flags().String("subject", "", "Optional hex node public key to bind invite")
	cmd.Flags().Duration("ttl", defaultJoinTokenTTL, "Invite token validity duration")
	cmd.Flags().StringArray("bootstrap", nil, "Bootstrap peer as <peer-pub-hex>@<host:port> (repeatable)")
	return cmd
}

func newServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve [port] [name]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		Run:   runServe,
	}
	return cmd
}

func newUnserveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unserve <port|name>",
		Short: "Stop exposing a local service",
		Args:  cobra.ExactArgs(1),
		Run:   runUnserve,
	}
}

func newConnectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect <service> [provider] [local-port]",
		Short: "Tunnel a local port to a service",
		Args:  cobra.RangeArgs(1, 3), //nolint:mnd
		Run:   runConnect,
	}
	return cmd
}

func pollenPath(cmd *cobra.Command) (string, error) {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		return "", err
	}
	return workspace.EnsurePollenDir(dir)
}

func runNode(cmd *cobra.Command, args []string) {
	logging.Init()
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	logger.Infow("starting pollen...", "version", version)

	port, _ := cmd.Flags().GetInt("port")
	joinToken, _ := cmd.Flags().GetString("join")
	ips, _ := cmd.Flags().GetIPSlice("ips")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		logger.Fatal(err)
	}

	var addrs []string
	if len(ips) > 0 {
		addrs = make([]string, len(ips))
		for i, ip := range ips {
			addrs[i] = ip.String()
		}
	}

	privKey, pubKey, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		logger.Fatal("failed to load signing keys: ", err)
	}

	var tkn *admissionv1.JoinToken
	if joinToken != "" {
		tkn, err = resolveJoinToken(ctx, privKey, joinToken)
		if err != nil {
			logger.Fatal("failed to resolve join token: ", err)
		}
	}

	creds, credErr := auth.LoadOrEnrollNodeCredentials(pollenDir, pubKey, tkn, time.Now())
	if credErr != nil {
		switch {
		case errors.Is(credErr, auth.ErrCredentialsNotFound) && joinToken == "":
			logger.Info("node is not initialized; auto-initializing root cluster")
			creds, credErr = auth.EnsureLocalRootCredentials(pollenDir, pubKey, time.Now())
			if credErr != nil {
				logger.Fatal("auto-init failed: ", credErr)
			}
		case errors.Is(credErr, auth.ErrCredentialsNotFound):
			logger.Fatal("node is not initialized; run `pollen init` or `pollen up --join <token>`")
		case errors.Is(credErr, auth.ErrDifferentCluster):
			logger.Fatal("node is already enrolled in a different cluster; run `pollen purge` before joining a new cluster")
		default:
			logger.Fatal(credErr)
		}
	}

	creds.InviteSigner, err = auth.LoadAdminSigner(pollenDir, time.Now())
	if err != nil {
		logger.Infow("invite redemption disabled", "err", err)
	}

	stateStore, err := store.Load(pollenDir, pubKey)
	if err != nil {
		logger.Fatal("failed to load state: ", err)
	}

	conf := &node.Config{
		Port:             port,
		GossipInterval:   passiveGossipInterval,
		PeerTickInterval: time.Second,
		AdvertisedIPs:    addrs,
	}

	n, err := node.New(conf, privKey, creds, stateStore, peer.NewStore())
	if err != nil {
		logger.Fatal(err)
	}

	nodeSrv := node.NewNodeService(n, stopFunc)

	logger.Info("successfully started node")

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(ctx context.Context) error {
		grpcSrv := server.NewGRPCServer()
		return grpcSrv.Start(ctx, nodeSrv, filepath.Join(pollenDir, socketName))
	})

	p.Go(func(ctx context.Context) error {
		return n.Start(ctx, tkn)
	})

	if err := p.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		logger.Fatal(err)
	}
}

func runInit(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	running, err := nodeSocketActive(filepath.Join(pollenDir, socketName))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	if running {
		fmt.Fprintln(cmd.ErrOrStderr(), "local node is running; run `pollen down` before initializing")
		return
	}

	_, pub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	existing, err := auth.LoadExistingNodeCredentials(pollenDir, pub, time.Now())
	if err == nil {
		_, adminPub, adminErr := auth.LoadAdminKey(pollenDir)
		if adminErr != nil && !errors.Is(adminErr, os.ErrNotExist) {
			fmt.Fprintln(cmd.ErrOrStderr(), adminErr)
			return
		}

		if adminErr == nil && slices.Equal(adminPub, existing.Trust.GetGenesisPub()) {
			fmt.Fprintf(cmd.OutOrStdout(), "already initialized as root cluster\nroot_pub: %s\ncluster_id: %s\n",
				hex.EncodeToString(adminPub),
				hex.EncodeToString(existing.Trust.GetClusterId()),
			)
			return
		}

		fmt.Fprintln(cmd.ErrOrStderr(), "node is already enrolled in a cluster; run `pollen purge` before initializing a new root cluster")
		return
	}
	if !errors.Is(err, auth.ErrCredentialsNotFound) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		fmt.Fprintln(cmd.ErrOrStderr(), "run `pollen purge` to reset local state")
		return
	}

	creds, err := auth.EnsureLocalRootCredentials(pollenDir, pub, time.Now())
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_, adminPub, err := auth.LoadAdminKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "initialized root cluster\nroot_pub: %s\ncluster_id: %s\n",
		hex.EncodeToString(adminPub),
		hex.EncodeToString(creds.Trust.GetClusterId()),
	)
}

func runPurge(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	all, err := cmd.Flags().GetBool("all")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	confirmed, err := cmd.Flags().GetBool("yes")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	running, err := nodeSocketActive(filepath.Join(pollenDir, socketName))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	if running {
		fmt.Fprintln(cmd.ErrOrStderr(), "local node is running; run `pollen down` before purging state")
		return
	}

	if !confirmed && !confirmPurge(cmd, all) {
		return
	}

	paths := []string{
		filepath.Join(pollenDir, "config.yaml"),
		filepath.Join(pollenDir, "state.yaml"),
		filepath.Join(pollenDir, ".state.lock"),
		filepath.Join(pollenDir, socketName),
		filepath.Join(pollenDir, "consumed_invites.json"),
		filepath.Join(pollenDir, "invites"),
		filepath.Join(pollenDir, "keys", "cluster.trust.pb"),
		filepath.Join(pollenDir, "keys", "membership.cert.pb"),
		filepath.Join(pollenDir, "keys", "admin.cert.pb"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.key"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.pub"),
	}
	if all {
		paths = append(paths,
			filepath.Join(pollenDir, "keys", "ed25519.key"),
			filepath.Join(pollenDir, "keys", "ed25519.pub"),
		)
	}

	for _, path := range paths {
		if removeErr := os.RemoveAll(path); removeErr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "remove %s: %v\n", path, removeErr)
			return
		}
	}

	if err := removeDirIfEmpty(filepath.Join(pollenDir, "keys")); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "local state purged")
	if all {
		fmt.Fprintln(cmd.OutOrStdout(), "identity keys removed")
	}
}

func resolveJoinToken(ctx context.Context, priv ed25519.PrivateKey, encoded string) (*admissionv1.JoinToken, error) {
	trimmed := strings.TrimSpace(encoded)

	joinToken, joinErr := auth.DecodeJoinToken(trimmed)
	if joinErr == nil {
		if _, err := auth.VerifyJoinToken(joinToken, priv.Public().(ed25519.PublicKey), time.Now()); err == nil { //nolint:forcetypeassert
			return joinToken, nil
		}
	}

	inviteToken, inviteErr := auth.DecodeInviteToken(trimmed)
	if inviteErr != nil {
		if joinErr != nil {
			return nil, fmt.Errorf("failed to decode join token (%w) or invite token (%w)", joinErr, inviteErr)
		}
		return nil, fmt.Errorf("failed to decode invite token (%w)", inviteErr)
	}

	redeemed, err := mesh.RedeemInvite(ctx, priv, inviteToken)
	if err != nil {
		return nil, fmt.Errorf("failed to redeem invite token: %w", err)
	}

	return redeemed, nil
}

func confirmPurge(cmd *cobra.Command, all bool) bool {
	fmt.Fprintln(cmd.ErrOrStderr(), "This will delete local cluster, admin, and runtime state.")
	if all {
		fmt.Fprintln(cmd.ErrOrStderr(), "It will also delete local node identity keys.")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "Node identity keys will be kept.")
	}
	fmt.Fprint(cmd.ErrOrStderr(), "Type \"yes\" to continue: ")

	scanner := bufio.NewScanner(cmd.InOrStdin())
	if !scanner.Scan() {
		if scanErr := scanner.Err(); scanErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), scanErr)
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), "aborted")
		}
		return false
	}

	if strings.TrimSpace(strings.ToLower(scanner.Text())) != "yes" {
		fmt.Fprintln(cmd.ErrOrStderr(), "aborted")
		return false
	}
	return true
}

func nodeSocketActive(sockPath string) (bool, error) {
	if _, err := os.Stat(sockPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(context.Background(), "unix", sockPath)
	if err != nil {
		return false, nil //nolint:nilerr // dial failure means socket is stale, not an error
	}
	_ = conn.Close()
	return true, nil
}

func removeDirIfEmpty(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(entries) > 0 {
		return nil
	}
	return os.Remove(path)
}

func runDown(cmd *cobra.Command, _ []string) {
	client := newControlClient(cmd)
	if _, err := client.Shutdown(context.Background(), connect.NewRequest(&controlv1.ShutdownRequest{})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "node stopped")
}

func runAdminKeygen(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_, pub, err := auth.LoadAdminKey(pollenDir)
	created := false
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, pub, err = auth.LoadOrCreateAdminKey(pollenDir)
			created = true
		}
	}
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	state := "admin key already present"
	if created {
		state = "generated local admin key"
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%s\nadmin_pub: %s\n", state, hex.EncodeToString(pub))
	fmt.Fprintln(cmd.OutOrStdout(), "note: this does not grant signing authority")
	fmt.Fprintln(cmd.OutOrStdout(), "install a delegated admin cert with `pollen admin set-cert <admin-cert-b64>`")
}

func runAdminSetCert(cmd *cobra.Command, args []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_, adminPub, err := auth.LoadAdminKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_, nodePub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	creds, err := auth.LoadExistingNodeCredentials(pollenDir, nodePub, time.Now())
	if err != nil {
		if errors.Is(err, auth.ErrCredentialsNotFound) {
			fmt.Fprintln(cmd.ErrOrStderr(), "node credentials not initialized; run `pollen init` or `pollen up --join <token>` first")
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
		}
		return
	}

	cert, err := auth.UnmarshalAdminCertBase64(strings.TrimSpace(args[0]))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if err := auth.VerifyAdminCert(cert, creds.Trust, time.Now()); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	if !bytes.Equal(cert.GetClaims().GetAdminPub(), adminPub) {
		fmt.Fprintln(cmd.ErrOrStderr(), "admin cert subject mismatch")
		return
	}

	if err := auth.SaveAdminCert(pollenDir, cert); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "delegated admin certificate installed")
}

func runID(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_, pub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprint(cmd.OutOrStdout(), hex.EncodeToString(pub))
}

func runInvite(cmd *cobra.Command, args []string) {
	subjectFlag, err := cmd.Flags().GetString("subject")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	subjectPub, err := resolveInviteSubject(subjectFlag, args)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	ttl, err := cmd.Flags().GetDuration("ttl")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	bootstrapSpecs, err := cmd.Flags().GetStringArray("bootstrap")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	bootstraps, err := resolveBootstrapPeers(cmd, bootstrapSpecs)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	encoded, err := createInviteToken(cmd, subjectPub, ttl, bootstraps)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}

func runBootstrapSSH(cmd *cobra.Command, args []string) {
	host := args[0]

	relayPort, err := cmd.Flags().GetInt("relay-port")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if relayPort < minPort || relayPort > maxPort {
		fmt.Fprintf(cmd.ErrOrStderr(), "invalid relay port %d\n", relayPort)
		return
	}

	inferredAddr, err := inferRelayAddrFromSSHTarget(host, relayPort)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	relayAddrs := []string{inferredAddr}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ensuring pollen is installed on remote host...")
	if err := ensureRemotePollen(ctx, host); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	out, err := exec.CommandContext(ctx, "ssh", host, "pollen", "id").CombinedOutput()
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to fetch remote node identity: %v\n%s\n", err, strings.TrimSpace(string(out)))
		return
	}

	relayPeerKey, err := types.PeerKeyFromString(strings.TrimSpace(string(out)))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	relayPub := ed25519.PublicKey(relayPeerKey.Bytes())

	if err := bootstrapAccept(cmd, relayPub, relayAddrs, host); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "relay bootstrapped: %s\n", host)
}

func bootstrapAccept(cmd *cobra.Command, relayPub ed25519.PublicKey, relayAddrs []string, sshTarget string) error {
	seedToken, err := createJoinToken(cmd, relayPub, defaultBootstrapJoinTokenTTL, nil)
	if err != nil {
		return fmt.Errorf("create relay seed token: %w", err)
	}

	if err := bootstrapRelayOverSSH(cmd, sshTarget, seedToken); err != nil {
		return err
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	if err := saveBootstrapPeer(pollenDir, &admissionv1.BootstrapPeer{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	}); err != nil {
		return fmt.Errorf("save relay bootstrap details: %w", err)
	}

	_, localPub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		return err
	}

	joinToken, err := createJoinToken(cmd, localPub, defaultJoinTokenTTL, []*admissionv1.BootstrapPeer{{
		PeerPub: append([]byte(nil), relayPub...),
		Addrs:   append([]string(nil), relayAddrs...),
	}})
	if err != nil {
		return fmt.Errorf("create local join token: %w", err)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "relay is ready; starting local node")
	return execLocalUpWithJoin(cmd, joinToken)
}

func bootstrapRelayOverSSH(cmd *cobra.Command, sshTarget, seedToken string) error {
	if err := startRelayOverSSH(cmd, sshTarget, seedToken); err != nil {
		return err
	}
	if err := waitForRelayReady(cmd, sshTarget); err != nil {
		return err
	}
	if err := provisionRelayAdminDelegation(cmd, sshTarget); err != nil {
		return err
	}
	if err := restartRelayOverSSH(cmd, sshTarget); err != nil {
		return err
	}
	return waitForRelayReady(cmd, sshTarget)
}

func createJoinToken(cmd *cobra.Command, subjectPub ed25519.PublicKey, ttl time.Duration, bootstrap []*admissionv1.BootstrapPeer) (string, error) {
	if ttl <= 0 {
		return "", errors.New("token ttl must be positive")
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return "", err
	}

	signer, err := auth.LoadAdminSigner(pollenDir, time.Now())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("this node cannot issue join tokens; only delegated admins can sign enrollment tokens")
		}
		return "", err
	}

	token, err := auth.IssueJoinTokenWithIssuer(
		signer.Priv,
		signer.Trust,
		signer.Issuer,
		subjectPub,
		bootstrap,
		time.Now(),
		ttl,
	)
	if err != nil {
		return "", err
	}

	encoded, err := auth.EncodeJoinToken(token)
	if err != nil {
		return "", err
	}

	return encoded, nil
}

func createInviteToken(cmd *cobra.Command, subjectPub ed25519.PublicKey, ttl time.Duration, bootstrap []*admissionv1.BootstrapPeer) (string, error) {
	if ttl <= 0 {
		return "", errors.New("token ttl must be positive")
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return "", err
	}

	signer, err := auth.LoadAdminSigner(pollenDir, time.Now())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("this node cannot issue invites; only delegated admins can sign invite tokens")
		}
		return "", err
	}

	token, err := auth.IssueInviteTokenWithSigner(signer, subjectPub, bootstrap, time.Now(), ttl)
	if err != nil {
		return "", err
	}

	encoded, err := auth.EncodeInviteToken(token)
	if err != nil {
		return "", err
	}

	return encoded, nil
}

func resolveBootstrapPeers(cmd *cobra.Command, specs []string) ([]*admissionv1.BootstrapPeer, error) {
	if len(specs) == 0 {
		pollenDir, err := pollenPath(cmd)
		if err != nil {
			return nil, err
		}

		registered, err := loadBootstrapPeers(pollenDir)
		if err != nil {
			return nil, err
		}
		if len(registered) > 0 {
			return registered, nil
		}

		bootstrap, err := localBootstrapPeer(cmd)
		if err != nil {
			return nil, err
		}
		return []*admissionv1.BootstrapPeer{bootstrap}, nil
	}

	return parseBootstrapSpecs(specs)
}

func saveBootstrapPeer(pollenDir string, peer *admissionv1.BootstrapPeer) error {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return err
	}
	if err := cfg.RememberBootstrapPeer(peer); err != nil {
		return err
	}

	return config.Save(pollenDir, cfg)
}

func loadBootstrapPeers(pollenDir string) ([]*admissionv1.BootstrapPeer, error) {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return nil, err
	}

	return cfg.BootstrapProtoPeers()
}

func resolveInviteSubject(subjectFlag string, args []string) (ed25519.PublicKey, error) {
	hasArg := len(args) == 1
	hasFlag := strings.TrimSpace(subjectFlag) != ""

	if hasArg && hasFlag {
		return nil, errors.New("provide subject as positional argument or --subject, not both")
	}

	subject := ""
	if hasArg {
		subject = strings.TrimSpace(args[0])
	}

	if subject == "" && hasFlag {
		subject = strings.TrimSpace(subjectFlag)
	}

	if subject == "" {
		return nil, nil
	}

	pk, err := types.PeerKeyFromString(subject)
	if err != nil {
		return nil, err
	}

	return ed25519.PublicKey(pk.Bytes()), nil
}

func parseBootstrapSpecs(specs []string) ([]*admissionv1.BootstrapPeer, error) {
	byPeer := make(map[string]*admissionv1.BootstrapPeer)
	order := make([]string, 0)

	for _, spec := range specs {
		parsed, err := parseBootstrapSpec(spec)
		if err != nil {
			return nil, err
		}

		key := hex.EncodeToString(parsed.pub)
		entry, ok := byPeer[key]
		if !ok {
			entry = &admissionv1.BootstrapPeer{PeerPub: append([]byte(nil), parsed.pub...)}
			byPeer[key] = entry
			order = append(order, key)
		}

		if !slices.Contains(entry.Addrs, parsed.addr) {
			entry.Addrs = append(entry.Addrs, parsed.addr)
		}
	}

	out := make([]*admissionv1.BootstrapPeer, 0, len(order))
	for _, key := range order {
		out = append(out, byPeer[key])
	}

	return out, nil
}

func parseBootstrapSpec(spec string) (bootstrapInfo, error) {
	spec = strings.TrimSpace(spec)
	parts := strings.SplitN(spec, "@", 2) //nolint:mnd
	if len(parts) != 2 {                  //nolint:mnd
		return bootstrapInfo{}, errors.New("invalid bootstrap format, expected <peer-pub-hex>@<host:port>")
	}

	pk, err := types.PeerKeyFromString(parts[0])
	if err != nil {
		return bootstrapInfo{}, fmt.Errorf("parse bootstrap peer key: %w", err)
	}
	pub := ed25519.PublicKey(pk.Bytes())

	addr, err := config.NormalizeRelayAddr(parts[1])
	if err != nil {
		return bootstrapInfo{}, fmt.Errorf("parse bootstrap address: %w", err)
	}

	return bootstrapInfo{pub: pub, addr: addr}, nil
}

type bootstrapInfo struct {
	addr string
	pub  ed25519.PublicKey
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
		return fmt.Errorf("failed to install pollen on remote host: %w\n%s", err, strings.TrimSpace(string(out)))
	}

	return nil
}

func startRelayOverSSH(cmd *cobra.Command, sshTarget, token string) error {
	remoteStart := fmt.Sprintf("nohup pollen up --join %q >/tmp/pollen.log 2>&1 < /dev/null &", token)
	startOut, err := exec.CommandContext(cmd.Context(), "ssh", sshTarget, remoteStart).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start relay node: %w\n%s", err, strings.TrimSpace(string(startOut)))
	}

	return nil
}

func execLocalUpWithJoin(cmd *cobra.Command, joinToken string) error {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve local executable: %w", err)
	}

	args := []string{exe, "--dir", pollenDir, "up", "--join", joinToken}
	if err := syscall.Exec(exe, args, os.Environ()); err != nil {
		return fmt.Errorf("exec local node startup: %w", err)
	}

	return nil
}

func restartRelayOverSSH(cmd *cobra.Command, sshTarget string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	restartCmd := "pollen down >/dev/null 2>&1 || true; pollen up -d"
	out, err := exec.CommandContext(ctx, "ssh", sshTarget, restartCmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to restart relay node: %w\n%s", err, strings.TrimSpace(string(out)))
	}

	return nil
}

func provisionRelayAdminDelegation(cmd *cobra.Command, sshTarget string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	initOut, err := exec.CommandContext(ctx, "ssh", sshTarget, "pollen", "admin", "keygen").CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to initialize relay admin key: %w\n%s", err, strings.TrimSpace(string(initOut)))
	}

	relayAdminPub, err := parseAdminPubFromInitOutput(string(initOut))
	if err != nil {
		return err
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	signer, err := auth.LoadAdminSigner(pollenDir, time.Now())
	if err != nil {
		return err
	}
	if !slices.Equal(signer.Trust.GetGenesisPub(), signer.Issuer.GetClaims().GetAdminPub()) {
		return errors.New("only genesis admin can delegate relay admin certs")
	}

	cert, err := auth.IssueAdminCert(
		signer.Priv,
		signer.Trust.GetClusterId(),
		relayAdminPub,
		time.Now().Add(-time.Minute),
		time.Now().Add(10*365*24*time.Hour),
	)
	if err != nil {
		return err
	}

	encoded, err := auth.MarshalAdminCertBase64(cert)
	if err != nil {
		return err
	}

	setCertOut, err := exec.CommandContext(ctx, "ssh", sshTarget, "pollen", "admin", "set-cert", encoded).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to install relay admin cert: %w\n%s", err, strings.TrimSpace(string(setCertOut)))
	}

	return nil
}

func waitForRelayReady(cmd *cobra.Command, sshTarget string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	readyCtx, cancel := context.WithTimeout(ctx, bootstrapStatusWait)
	defer cancel()

	checkCmd := "for i in $(seq 1 20); do if [ -S \"$HOME/.pollen/pollen.sock\" ] && pollen status --all >/dev/null 2>&1; then exit 0; fi; sleep 1; done; exit 1"
	out, err := exec.CommandContext(readyCtx, "ssh", sshTarget, checkCmd).CombinedOutput()
	if err == nil {
		return nil
	}

	logCmd := "tail -n 120 ~/Library/Logs/pollen.log 2>/dev/null || journalctl --user -u pollen -n 120 --no-pager 2>/dev/null || tail -n 120 /tmp/pollen.log 2>/dev/null || true"
	logOut, _ := exec.CommandContext(ctx, "ssh", sshTarget, logCmd).CombinedOutput()
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

func parseAdminPubFromInitOutput(out string) (ed25519.PublicKey, error) {
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "admin_pub:") {
			continue
		}
		pk, err := types.PeerKeyFromString(strings.TrimSpace(strings.TrimPrefix(line, "admin_pub:")))
		if err != nil {
			return nil, err
		}
		return ed25519.PublicKey(pk.Bytes()), nil
	}

	return nil, errors.New("failed to parse relay admin public key")
}

func localBootstrapPeer(cmd *cobra.Command) (*admissionv1.BootstrapPeer, error) {
	client := newControlClient(cmd)
	resp, err := client.GetBootstrapInfo(context.Background(), connect.NewRequest(&controlv1.GetBootstrapInfoRequest{}))
	if err != nil {
		return nil, err
	}

	self := resp.Msg.GetSelf()
	if self == nil || self.GetPeer() == nil {
		return nil, errors.New("missing bootstrap peer information")
	}
	if len(self.GetAddrs()) == 0 {
		return nil, errors.New("bootstrap peer has no advertised addresses")
	}

	return &admissionv1.BootstrapPeer{
		PeerPub: append([]byte(nil), self.GetPeer().GetPeerId()...),
		Addrs:   append([]string(nil), self.GetAddrs()...),
	}, nil
}

func runServe(cmd *cobra.Command, args []string) {
	portStr := args[0]
	name := ""
	if len(args) > 1 {
		name = args[1]
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
	}

	client := newControlClient(cmd)
	var namePtr *string
	if name != "" {
		namePtr = &name
	}
	if _, err = client.RegisterService(context.Background(), connect.NewRequest(&controlv1.RegisterServiceRequest{
		Port: uint32(port),
		Name: namePtr,
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Registered service %s on port: %s\n", name, portStr)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Registered service on port: %s\n", portStr)
}

func runUnserve(cmd *cobra.Command, args []string) {
	arg := args[0]
	port := uint32(0)
	name := ""
	if isPortArg(arg) {
		p, _ := strconv.Atoi(arg)
		port = uint32(p)
	} else {
		name = arg
	}

	var namePtr *string
	if name != "" {
		namePtr = &name
	}

	client := newControlClient(cmd)
	if _, err := client.UnregisterService(context.Background(), connect.NewRequest(&controlv1.UnregisterServiceRequest{
		Port: port,
		Name: namePtr,
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service %s\n", name)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service on port: %d\n", port)
}

func runConnect(cmd *cobra.Command, args []string) {
	serviceArg := args[0]
	providerArg := ""
	localPortArg := ""
	if len(args) >= 2 { //nolint:mnd
		if len(args) == 2 && isPortArg(args[1]) { //nolint:mnd
			localPortArg = args[1]
		} else {
			providerArg = args[1]
		}
	}
	if len(args) == 3 { //nolint:mnd
		localPortArg = args[2] //nolint:mnd
	}

	localPort := uint32(0)
	if localPortArg != "" {
		p, err := strconv.Atoi(localPortArg)
		if err != nil || p <= 0 || p > 65535 {
			fmt.Fprintln(cmd.ErrOrStderr(), "invalid local port")
			return
		}
		localPort = uint32(p)
	}

	client := newControlClient(cmd)
	statusResp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	svc, err := resolveService(statusResp.Msg, serviceArg, providerArg)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	connectResp, err := client.ConnectService(context.Background(), connect.NewRequest(&controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerId: svc.GetProvider().GetPeerId()},
		RemotePort: svc.GetPort(),
		LocalPort:  localPort,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	local := connectResp.Msg.GetLocalPort()
	provider := formatPeerID(svc.GetProvider().GetPeerId(), false)
	fmt.Fprintf(cmd.OutOrStdout(), "forwarding localhost:%d -> %s (%s:%d)\n", local, svc.GetName(), provider, svc.GetPort())
}

func resolveService(st *controlv1.GetStatusResponse, serviceArg, providerArg string) (*controlv1.ServiceSummary, error) {
	if st == nil {
		return nil, errors.New("no status available")
	}

	reachableProviders := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		selfID := peerKeyString(st.GetSelf().GetNode().GetPeerId())
		reachableProviders[selfID] = true
	}
	for _, n := range st.Nodes {
		if isReachableStatus(n.GetStatus()) {
			reachableProviders[peerKeyString(n.GetNode().GetPeerId())] = true
		}
	}

	portFilter := uint32(0)
	if p, err := strconv.Atoi(serviceArg); err == nil && p > 0 && p <= 65535 {
		portFilter = uint32(p)
	}

	matches := make([]*controlv1.ServiceSummary, 0, len(st.Services))
	for _, svc := range st.Services {
		if portFilter > 0 {
			if svc.GetPort() != portFilter && svc.GetName() != serviceArg {
				continue
			}
		} else if svc.GetName() != serviceArg {
			continue
		}

		providerID := peerKeyString(svc.GetProvider().GetPeerId())
		if !reachableProviders[providerID] {
			continue
		}
		if providerArg != "" && !peerIDHasPrefix(svc.GetProvider().GetPeerId(), providerArg) {
			continue
		}
		matches = append(matches, svc)
	}

	if len(matches) == 0 {
		if providerArg != "" {
			return nil, fmt.Errorf("no reachable provider for %q on %q", serviceArg, providerArg)
		}
		return nil, fmt.Errorf("no reachable service match for %q", serviceArg)
	}
	if len(matches) > 1 {
		var b strings.Builder
		fmt.Fprintf(&b, "service %q has multiple providers; use: pollen connect %s <provider>\n", serviceArg, serviceArg)
		for _, svc := range matches {
			provider := formatPeerID(svc.GetProvider().GetPeerId(), false)
			fmt.Fprintf(&b, "- %s (%s:%d)\n", svc.GetName(), provider, svc.GetPort())
		}
		return nil, errors.New(strings.TrimSpace(b.String()))
	}

	return matches[0], nil
}

func peerKeyString(peerID []byte) string {
	if len(peerID) == 0 {
		return ""
	}
	key := types.PeerKeyFromBytes(peerID)
	return (&key).String()
}

func formatPeerID(peerID []byte, wide bool) string {
	full := peerKeyString(peerID)
	if full == "" {
		return "-"
	}
	if wide || len(full) <= 8 {
		return full
	}
	return full[:8]
}

func formatStatus(s controlv1.NodeStatus) string {
	switch s {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return "online"
	case controlv1.NodeStatus_NODE_STATUS_RELAY:
		return "relay"
	case controlv1.NodeStatus_NODE_STATUS_OFFLINE:
		return "offline"
	default:
		return "offline"
	}
}

func isReachableStatus(s controlv1.NodeStatus) bool {
	return s == controlv1.NodeStatus_NODE_STATUS_ONLINE || s == controlv1.NodeStatus_NODE_STATUS_RELAY
}

func peerIDHasPrefix(peerID []byte, prefix string) bool {
	if prefix == "" {
		return true
	}
	full := peerKeyString(peerID)
	return strings.HasPrefix(full, strings.ToLower(prefix))
}

func isPortArg(s string) bool {
	p, err := strconv.Atoi(s)
	return err == nil && p > 0 && p <= 65535
}

func newControlClient(cmd *cobra.Command) controlv1connect.ControlServiceClient {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	socket := filepath.Join(pollenDir, socketName)

	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(context.Background(), "unix", socket)
		},
	}

	httpClient := &http.Client{
		Timeout:   controlClientTimeout,
		Transport: tr,
	}

	return controlv1connect.NewControlServiceClient(
		httpClient,
		"http://unix",
		connect.WithGRPC(),
	)
}

func newUpgradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade Pollen to the latest (or specified) version",
		Args:  cobra.NoArgs,
		Run:   runUpgrade,
	}
	cmd.Flags().String("version", "", "Install a specific release (e.g. v0.3.0)")
	cmd.Flags().Bool("allow-breaking", false, "Allow semver-breaking upgrades")
	return cmd
}

func fetchInstallScript() ([]byte, error) {
	repo := os.Getenv("POLLEN_REPO")
	if repo == "" {
		repo = defaultRepo
	}
	scriptURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/main/scripts/install.sh", repo)

	resp, err := (&http.Client{Timeout: scriptFetchTimeout}).Get(scriptURL) //nolint:noctx
	if err != nil {
		return nil, fmt.Errorf("failed to fetch install script: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch install script: HTTP %d", resp.StatusCode)
	}

	script, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read install script: %w", err)
	}

	return script, nil
}

func runUpgrade(cmd *cobra.Command, _ []string) {
	script, err := fetchInstallScript()
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	args := []string{"-s", "--"}

	if v, _ := cmd.Flags().GetString("version"); v != "" {
		args = append(args, "--version", v)
	}
	if ab, _ := cmd.Flags().GetBool("allow-breaking"); ab {
		args = append(args, "--allow-breaking")
	}

	bash := exec.Command("bash", args...)
	bash.Stdin = bytes.NewReader(script)
	bash.Stdout = cmd.OutOrStdout()
	bash.Stderr = cmd.ErrOrStderr()

	if err := bash.Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "upgrade failed: %v\n", err)
	}
}
