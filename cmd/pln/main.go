package main

import (
	"context"
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
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	plnDir     = ".pln"
	socketName = "pln.sock"

	controlClientTimeout         = 10 * time.Second
	callWorkloadTimeout          = 60 * time.Second
	passiveGossipInterval        = 10 * time.Second
	defaultJoinTokenTTL          = 5 * time.Minute
	defaultBootstrapJoinTokenTTL = 1 * time.Minute
	bootstrapStatusWait          = 20 * time.Second
	minPort                      = 1
	maxPort                      = 65535
	defaultRepo                  = "sambigeara/pollen"
	scriptFetchTimeout           = 30 * time.Second
	defaultMaxConnectionAge      = 24 * time.Hour
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func loadConfigOrDefault(pollenDir string) *config.Config {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		cfg = &config.Config{}
	}
	return cfg
}

func getExpireAfterFlag(cmd *cobra.Command) (time.Duration, error) {
	d, err := cmd.Flags().GetDuration("expire-after")
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, errors.New("expire-after must be >= 0")
	}
	return d, nil
}

func main() {
	rootCmd := &cobra.Command{Use: "pln", Short: "Peer-to-peer mesh networking"}
	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted (env: PLN_DIR)")

	rootCmd.AddCommand(
		newVersionCmd(),
		newInitCmd(),
		newPurgeCmd(),
		newAdminCmd(),
		newIDCmd(),
		newBootstrapCmd(),
		newUpCmd(),
		newDownCmd(),
		newRestartCmd(),
		newUpgradeCmd(),
		newJoinCmd(),
		newInviteCmd(),
		newStatusCmd(),
		newServeCmd(),
		newUnserveCmd(),
		newConnectCmd(),
		newDisconnectCmd(),
		newDenyCmd(),
		newLogsCmd(),
		newSeedCmd(),
		newUnseedCmd(),
		newCallCmd(),
		newProvisionCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func defaultRootDir() string {
	if d := os.Getenv("PLN_DIR"); d != "" {
		return d
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = os.Getenv("HOME")
	}
	// Under sudo, os.UserHomeDir returns /root. Resolve SUDO_USER to
	// find the invoking user's home so the fallback path doesn't land
	// in /root/.pln.
	if os.Getuid() == 0 {
		if name := os.Getenv("SUDO_USER"); name != "" {
			if u, err := user.Lookup(name); err == nil {
				homeDir = u.HomeDir
			}
		}
	}
	home := filepath.Join(homeDir, plnDir)
	if runtime.GOOS != osLinux {
		return home
	}
	const sysDir = "/var/lib/pln"
	sysState := hasState(sysDir)
	homeState := hasState(home)
	switch {
	case sysState && homeState:
		fmt.Fprintf(os.Stderr,
			"warning: pollen state found in both %s and %s; using %s\n"+
				"  the state in %s will be ignored\n"+
				"  to change this, set PLN_DIR=%s\n",
			sysDir, home, sysDir, home, home)
		return sysDir
	case sysState:
		return sysDir
	case homeState:
		return home
	default:
		// Neither has state. Prefer sysDir when it exists (deb
		// package installed) so credentials land where the system
		// service reads from. Falls through to ~/.pln on systems
		// without the package.
		if fi, err := os.Stat(sysDir); err == nil && fi.IsDir() {
			return sysDir
		}
		if _, err := os.Stat(homeDir); errors.Is(err, os.ErrNotExist) {
			return sysDir
		}
		return home
	}
}

func hasState(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "keys", "ed25519.pub"))
	return err == nil
}

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show Pollen version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			short, _ := cmd.Flags().GetBool("short")
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

func newIDCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "id",
		Short: "Show local node identity public key",
		Run:   runID,
	}
}

func runID(cmd *cobra.Command, _ []string) {
	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	pub, err := auth.ReadIdentityPub(pollenDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
		_, pub, err = auth.EnsureIdentityKey(pollenDir)
		if err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
	}

	fmt.Fprint(cmd.OutOrStdout(), hex.EncodeToString(pub))
}

func newUpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start a Pollen node (foreground by default, -d for background service)",
		Run:   runUp,
	}
	cmd.Flags().Int("port", config.DefaultBootstrapPort, "Listening port")
	cmd.Flags().IPSlice("ips", []net.IP{}, "Advertised IPs")
	cmd.Flags().Bool("public", false, "Mark this node as publicly accessible (relay)")
	cmd.Flags().BoolP("detach", "d", false, "Run as a background service")
	cmd.Flags().Bool("metrics", false, "Log metrics and trace output at debug level")
	return cmd
}

func runUp(cmd *cobra.Command, _ []string) {
	detach, _ := cmd.Flags().GetBool("detach")

	if public, _ := cmd.Flags().GetBool("public"); public {
		pollenDir, _ := cmd.Flags().GetString("dir")
		if err := applyPublicFlag(pollenDir); err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
	}

	if detach {
		if err := servicectl("start", cmd); err != nil {
			os.Exit(1)
		}
		return
	}

	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err == nil {
		sockPath := filepath.Join(pollenDir, socketName)
		if active, _ := nodeSocketActive(sockPath); active {
			fmt.Fprintln(cmd.ErrOrStderr(),
				"a node is already running; use `pln down` to stop the background service")
			return
		}
	}

	runNode(cmd)
}

func runNode(cmd *cobra.Command) {
	zapLogger, err := logging.New()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	zap.ReplaceGlobals(zapLogger.Named("pln"))
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	logger.Infow("starting pln...", "version", version)

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err != nil {
		logger.Fatal(err)
	}

	privKey, pubKey, err := auth.EnsureIdentityKey(pollenDir)
	if err != nil {
		logger.Fatal("failed to load signing keys: ", err)
	}

	cfg, err := config.Load(pollenDir)
	if err != nil {
		logger.Fatalw("failed to load config", "err", err)
	}

	port, _ := cmd.Flags().GetInt("port")

	var addrs []string
	if cmd.Flags().Changed("ips") {
		ips, _ := cmd.Flags().GetIPSlice("ips")
		addrs = make([]string, 0, len(ips))
		for _, ip := range ips {
			addrs = append(addrs, ip.String())
		}
	}

	creds, credErr := auth.LoadNodeCredentials(pollenDir)
	if credErr != nil {
		if !errors.Is(credErr, auth.ErrCredentialsNotFound) {
			logger.Fatal(credErr)
		}
		logger.Info("node is not initialized; auto-initializing root cluster")
		creds, credErr = auth.EnsureLocalRootCredentials(pollenDir, pubKey, time.Now(), config.DefaultMembershipTTL, config.DefaultDelegationTTL)
		if credErr != nil {
			logger.Fatal("auto-init failed: ", credErr)
		}
	} else if auth.IsCertExpired(creds.Cert(), time.Now()) {
		logger.Warnw("delegation certificate has expired — starting in degraded mode, will attempt renewal",
			"expired_at", auth.CertExpiresAt(creds.Cert()))
	}

	signer, err := auth.NewDelegationSigner(pollenDir, privKey, config.DefaultDelegationTTL)
	if err != nil {
		logger.Infow("invite redemption disabled", "err", err)
	} else {
		creds.SetDelegationKey(signer)
	}

	var runtimeState *statev1.RuntimeState
	if raw, readErr := os.ReadFile(filepath.Join(pollenDir, "state.pb")); readErr == nil && len(raw) > 0 {
		runtimeState = loadRuntimeState(raw)
	}

	var inviteConsumer auth.InviteConsumer
	if creds.DelegationKey() != nil {
		var entries []*statev1.ConsumedInvite
		if runtimeState != nil {
			entries = runtimeState.GetConsumedInvites()
		}
		inviteConsumer = auth.NewInviteConsumer(entries)
	}

	bootstrapPeers := make([]supervisor.BootstrapTarget, 0, len(cfg.BootstrapPeers))
	for peerHex, addrs := range cfg.BootstrapPeers {
		pk, _ := types.PeerKeyFromString(peerHex)
		bootstrapPeers = append(bootstrapPeers, supervisor.BootstrapTarget{
			PeerKey: pk,
			Addrs:   addrs,
		})
	}

	metricsEnabled, _ := cmd.Flags().GetBool("metrics")

	var initialConns []supervisor.ConnectionEntry
	for _, conn := range cfg.Connections {
		peerKey, parseErr := types.PeerKeyFromString(conn.Peer)
		if parseErr != nil {
			logger.Warnw("skipping invalid connection peer in config", "peer", conn.Peer, "err", parseErr)
			continue
		}
		initialConns = append(initialConns, supervisor.ConnectionEntry{
			PeerKey:    peerKey,
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		})
	}

	initialServices := make([]supervisor.ServiceEntry, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		initialServices = append(initialServices, supervisor.ServiceEntry{
			Name: svc.Name,
			Port: svc.Port,
		})
	}

	opts := supervisor.Options{
		SigningKey:         privKey,
		PollenDir:          pollenDir,
		SocketPath:         filepath.Join(pollenDir, socketName),
		RuntimeState:       runtimeState,
		ShutdownFunc:       stopFunc,
		ListenPort:         port,
		GossipInterval:     passiveGossipInterval,
		PeerTickInterval:   time.Second,
		AdvertisedIPs:      addrs,
		BootstrapPeers:     bootstrapPeers,
		InitialConnections: initialConns,
		InitialServices:    initialServices,
		MaxConnectionAge:   defaultMaxConnectionAge,
		BootstrapPublic:    cfg.Public,
		MetricsEnabled:     metricsEnabled,
	}

	n, err := supervisor.New(opts, creds, inviteConsumer)
	if err != nil {
		logger.Fatal(err)
	}

	logger.Info("successfully started node")

	if err := n.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		logger.Fatal(err)
	}
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

type tokenIssuerContext struct {
	signer    *auth.DelegationSigner
	pollenDir string
}

func loadTokenIssuerContext(pollenDir string) (*tokenIssuerContext, error) {
	nodePriv, _, err := auth.EnsureIdentityKey(pollenDir)
	if err != nil {
		return nil, err
	}

	signer, err := auth.NewDelegationSigner(pollenDir, nodePriv, config.DefaultDelegationTTL)
	if err != nil {
		return nil, err
	}

	return &tokenIssuerContext{
		pollenDir: pollenDir,
		signer:    signer,
	}, nil
}

// shouldEscalateToRoot reports whether the current process should re-exec
// via sudo. Returns true when: running on Linux, not already root, targeting
// a system install directory, the pln system user exists, and the current
// user is not the pln user itself (bootstrap's `sudo -u pln` path).
func shouldEscalateToRoot(goos string, uid int, dir string, plnUser *user.User) bool {
	if goos != osLinux || uid == 0 {
		return false
	}
	if dir != "/var/lib/pln" && !strings.HasPrefix(dir, "/var/lib/pln/") {
		return false
	}
	if plnUser == nil {
		return false
	}
	if fmt.Sprint(uid) == plnUser.Uid {
		return false
	}
	return true
}

// reexecAsRoot re-execs the current process via sudo when a non-root user
// targets a system install directory. All offline commands (init, join, purge,
// admin) call this as their first action. The existing root code paths handle
// file ownership (chown pln:pln) and group membership (ensureSudoUserInPlnGroup).
func reexecAsRoot(cmd *cobra.Command) {
	dir, _ := cmd.Flags().GetString("dir")
	plnUser, _ := user.Lookup("pln")
	if !shouldEscalateToRoot(runtime.GOOS, os.Getuid(), dir, plnUser) {
		return
	}
	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "cannot resolve binary path: %v\n", err)
		os.Exit(1)
	}
	sudoPath, err := exec.LookPath("sudo")
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "sudo is required for system installs: %v\n", err)
		os.Exit(1)
	}
	args := append([]string{"sudo", binary}, os.Args[1:]...)
	if err := syscall.Exec(sudoPath, args, os.Environ()); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to escalate to root: %v\n", err)
		os.Exit(1)
	}
}

func setConfigPublic(pollenDir string, public bool) error {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return err
	}
	cfg.Public = public
	return config.Save(pollenDir, cfg)
}

func applyPublicFlag(pollenDir string) error {
	if _, _, err := auth.LoadAdminKey(pollenDir); err != nil {
		return fmt.Errorf("--public requires admin keys; run `pln init` to create a root cluster, or join with an admin token")
	}
	return setConfigPublic(pollenDir, true)
}

func saveBootstrapPeer(pollenDir string, peer *admissionv1.BootstrapPeer) error {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return err
	}
	cfg.RememberBootstrapPeer(peer)
	return config.Save(pollenDir, cfg)
}

func clusterStatePaths(pollenDir string) []string {
	return []string{
		filepath.Join(pollenDir, "keys", "root.pub"),
		filepath.Join(pollenDir, "keys", "membership.cert.pb"),
		filepath.Join(pollenDir, "keys", "delegation.cert.pb"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.key"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.pub"),
		filepath.Join(pollenDir, "config.yaml"),
		filepath.Join(pollenDir, "state.pb"),
		filepath.Join(pollenDir, "state.yaml"),     // TODO(saml) these can go after a release or two
		filepath.Join(pollenDir, "state.yaml.bak"), // TODO(saml) these can go after a release or two
		filepath.Join(pollenDir, "consumed_invites.json"),
		filepath.Join(pollenDir, "invites"),
		filepath.Join(pollenDir, "cas"),
	}
}

func fetchInstallScript() ([]byte, error) {
	scriptURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/main/scripts/install.sh", defaultRepo)
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

func newDenyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "deny <peer-id>",
		Short: "Deny a peer's membership",
		Args:  cobra.ExactArgs(1),
		Run:   runDeny,
	}
}

func runDeny(cmd *cobra.Command, args []string) {
	prefix := strings.ToLower(args[0])
	pollenDir, _ := cmd.Flags().GetString("dir")

	client := newControlClient(pollenDir)
	statusResp, err := client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	// Resolve the peer ID prefix to a full peer ID.
	var matches [][]byte
	for _, n := range statusResp.Msg.GetNodes() {
		if peerIDHasPrefix(n.GetNode().GetPeerPub(), prefix) {
			matches = append(matches, n.GetNode().GetPeerPub())
		}
	}

	if len(matches) == 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "no peer matching %q\n", prefix)
		os.Exit(1)
	}
	if len(matches) > 1 {
		fmt.Fprintf(cmd.ErrOrStderr(), "ambiguous peer prefix %q matches %d peers\n", prefix, len(matches))
		os.Exit(1)
	}

	peerID := matches[0]
	_, err = client.DenyPeer(cmd.Context(), connect.NewRequest(&controlv1.DenyPeerRequest{
		PeerPub: peerID,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	cfg := loadConfigOrDefault(pollenDir)
	cfg.ForgetBootstrapPeer(peerID)
	if saveErr := config.Save(pollenDir, cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist denial to config: %v\n", saveErr)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "denied peer %s\n", formatPeerID(peerID, false))
}

func newControlClient(pollenDir string) controlv1connect.ControlServiceClient {
	return newControlClientWithTimeout(pollenDir, controlClientTimeout)
}

func newControlClientWithTimeout(pollenDir string, timeout time.Duration) controlv1connect.ControlServiceClient {
	socket := filepath.Join(pollenDir, socketName)

	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(context.Background(), "unix", socket)
		},
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}

	return controlv1connect.NewControlServiceClient(
		httpClient,
		"http://unix",
		connect.WithGRPC(),
	)
}

// loadRuntimeState deserializes a RuntimeState from raw bytes, with backward
// compatibility for state.pb files that contain raw GossipEventBatch bytes.
func loadRuntimeState(raw []byte) *statev1.RuntimeState {
	var rs statev1.RuntimeState
	if err := rs.UnmarshalVT(raw); err == nil && (len(rs.GetGossipState()) > 0 || len(rs.GetPeers()) > 0 || len(rs.GetConsumedInvites()) > 0) {
		return &rs
	}
	return &statev1.RuntimeState{GossipState: raw}
}
