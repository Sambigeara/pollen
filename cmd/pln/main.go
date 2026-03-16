package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
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
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/workspace"
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
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	pub, err := node.ReadIdentityPub(pollenDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
		_, pub, err = node.GenIdentityKey(pollenDir)
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
		if err := applyPublicFlag(cmd); err != nil {
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

	pollenDir, err := pollenPath(cmd)
	if err == nil {
		sockPath := filepath.Join(pollenDir, socketName)
		if active, _ := nodeSocketActive(sockPath); active {
			fmt.Fprintln(cmd.ErrOrStderr(),
				"a node is already running; use `pln down` to stop the background service")
			return
		}
	}

	runNode(cmd)
}

func pollenPath(cmd *cobra.Command) (string, error) {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		return "", err
	}
	return workspace.EnsurePollenDir(dir)
}

func runNode(cmd *cobra.Command) {
	logging.Init()
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	logger.Infow("starting pln...", "version", version)

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		logger.Fatal(err)
	}

	privKey, pubKey, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		logger.Fatal("failed to load signing keys: ", err)
	}

	cfg := loadConfigOrDefault(pollenDir)
	certTTLs := cfg.CertTTLs

	port, _ := cmd.Flags().GetInt("port")
	if !cmd.Flags().Changed("port") && cfg.Port != 0 {
		port = int(cfg.Port)
	}

	var addrs []string
	if cmd.Flags().Changed("ips") {
		ips, _ := cmd.Flags().GetIPSlice("ips")
		addrs = make([]string, 0, len(ips))
		for _, ip := range ips {
			addrs = append(addrs, ip.String())
		}
	} else if len(cfg.AdvertiseIPs) > 0 {
		addrs = cfg.AdvertiseIPs
	}

	creds, credErr := auth.LoadOrEnrollNodeCredentials(pollenDir, pubKey, nil, time.Now())
	if credErr != nil {
		switch {
		case errors.Is(credErr, auth.ErrCredentialsNotFound):
			logger.Info("node is not initialized; auto-initializing root cluster")
			creds, credErr = auth.EnsureLocalRootCredentials(pollenDir, pubKey, time.Now(), certTTLs.MembershipTTL(), certTTLs.DelegationTTL())
			if credErr != nil {
				logger.Fatal("auto-init failed: ", credErr)
			}
		case errors.Is(credErr, auth.ErrCertExpired):
			logger.Warnw("delegation certificate has expired — starting in degraded mode, will attempt renewal",
				"expired_at", auth.CertExpiresAt(creds.Cert))
		case errors.Is(credErr, auth.ErrDifferentCluster):
			logger.Fatal("node is already enrolled in a different cluster; run `pln purge` before joining a new cluster")
		default:
			logger.Fatal(credErr)
		}
	}

	creds.DelegationKey, err = auth.LoadDelegationSigner(pollenDir, privKey, time.Now(), certTTLs.DelegationTTL())
	if err != nil {
		logger.Infow("invite redemption disabled", "err", err)
	}

	stateStore, err := store.Load(pollenDir, pubKey)
	if err != nil {
		logger.Fatal("failed to load state: ", err)
	}

	if creds.DelegationKey != nil {
		creds.DelegationKey.Consumed = stateStore
	}

	for _, svc := range cfg.Services {
		stateStore.UpsertLocalService(svc.Port, svc.Name)
	}

	for _, conn := range cfg.Connections {
		peerKey, parseErr := types.PeerKeyFromString(conn.Peer)
		if parseErr != nil {
			logger.Warnw("skipping invalid connection peer in config", "peer", conn.Peer, "err", parseErr)
			continue
		}
		stateStore.AddDesiredConnection(peerKey, conn.RemotePort, conn.LocalPort)
	}

	var bootstrapPeers []node.BootstrapPeer
	protoPeers, loadErr := cfg.BootstrapProtoPeers()
	if loadErr != nil {
		logger.Warnw("failed to parse bootstrap peers from config", "err", loadErr)
	} else {
		for _, bp := range protoPeers {
			pk := types.PeerKeyFromBytes(bp.GetPeerPub())
			if stateStore.IsDenied(bp.GetPeerPub()) {
				logger.Warnw("bootstrap peer is denied; skipping (remove from config.yaml)", "peer", pk.String())
				continue
			}
			bootstrapPeers = append(bootstrapPeers, node.BootstrapPeer{
				PeerKey: pk,
				Addrs:   bp.GetAddrs(),
			})
		}
	}

	metricsEnabled, _ := cmd.Flags().GetBool("metrics")

	conf := &node.Config{
		Port:             port,
		GossipInterval:   passiveGossipInterval,
		PeerTickInterval: time.Second,
		AdvertisedIPs:    addrs,
		BootstrapPeers:   bootstrapPeers,
		TLSIdentityTTL:   certTTLs.TLSIdentityTTL(),
		MembershipTTL:    certTTLs.MembershipTTL(),
		ReconnectWindow:  certTTLs.ReconnectWindowDuration(),
		MaxConnectionAge: defaultMaxConnectionAge,
		BootstrapPublic:  cfg.Public,
		MetricsEnabled:   metricsEnabled,
	}

	n, err := node.New(conf, privKey, creds, stateStore, peer.NewStore(), pollenDir)
	if err != nil {
		logger.Fatal(err)
	}

	nodeSrv := node.NewNodeService(n, stopFunc, creds)

	logger.Info("successfully started node")

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(ctx context.Context) error {
		grpcSrv := server.NewGRPCServer()
		return grpcSrv.Start(ctx, nodeSrv, filepath.Join(pollenDir, socketName))
	})

	p.Go(func(ctx context.Context) error {
		return n.Start(ctx)
	})

	if err := p.Wait(); err != nil {
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
	cfg       *config.Config
	signer    *auth.DelegationSigner
	pollenDir string
}

func loadTokenIssuerContext(cmd *cobra.Command) (*tokenIssuerContext, error) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return nil, err
	}

	cfg := loadConfigOrDefault(pollenDir)

	nodePriv, _, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		return nil, err
	}

	signer, err := auth.LoadDelegationSigner(pollenDir, nodePriv, time.Now(), cfg.CertTTLs.DelegationTTL())
	if err != nil {
		return nil, err
	}

	return &tokenIssuerContext{
		pollenDir: pollenDir,
		cfg:       cfg,
		signer:    signer,
	}, nil
}

func ensureSudoUserInPlnGroup(cmd *cobra.Command) {
	if runtime.GOOS != osLinux || os.Getuid() != 0 {
		return
	}
	sudoUser := os.Getenv("SUDO_USER")
	if sudoUser == "" || userInGroup(cmd.Context(), sudoUser, "pln") {
		return
	}
	if err := exec.CommandContext(cmd.Context(), "usermod", "-aG", "pln", sudoUser).Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: could not add %s to pln group: %v\n", sudoUser, err)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "added %s to the pln group -- log out and back in for CLI access without sudo\n", sudoUser)
	}
}

func userInGroup(ctx context.Context, user, group string) bool {
	out, err := exec.CommandContext(ctx, "id", "-nG", user).Output()
	if err != nil {
		return false
	}
	return slices.Contains(strings.Fields(string(out)), group)
}

func setConfigPublic(pollenDir string, public bool) error {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return err
	}
	cfg.Public = public
	return config.Save(pollenDir, cfg)
}

func applyPublicFlag(cmd *cobra.Command) error {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}
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
	if err := cfg.RememberBootstrapPeer(peer); err != nil {
		return err
	}

	return config.Save(pollenDir, cfg)
}

func clusterStatePaths(pollenDir string) []string {
	return []string{
		filepath.Join(pollenDir, "keys", "cluster.trust.pb"),
		filepath.Join(pollenDir, "keys", "membership.cert.pb"),
		filepath.Join(pollenDir, "keys", "delegation.cert.pb"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.key"),
		filepath.Join(pollenDir, "keys", "admin_ed25519.pub"),
		filepath.Join(pollenDir, "config.yaml"),
		filepath.Join(pollenDir, "state.pb"),
		filepath.Join(pollenDir, "state.yaml"),     // TODO(saml) these can go after a release or two
		filepath.Join(pollenDir, "state.yaml.bak"), // TODO(saml) these can go after a release or two
		filepath.Join(pollenDir, ".state.lock"),
		filepath.Join(pollenDir, "consumed_invites.json"),
		filepath.Join(pollenDir, "invites"),
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

	client := newControlClient(cmd)
	statusResp, err := client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	// Resolve the peer ID prefix to a full peer ID.
	var matches [][]byte
	for _, n := range statusResp.Msg.GetNodes() {
		if peerIDHasPrefix(n.GetNode().GetPeerId(), prefix) {
			matches = append(matches, n.GetNode().GetPeerId())
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
		PeerId: peerID,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to prepare pln dir: %v\n", err)
		os.Exit(1)
	}
	cfg := loadConfigOrDefault(pollenDir)
	cfg.ForgetBootstrapPeer(peerID)
	if saveErr := config.Save(pollenDir, cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist denial to config: %v\n", saveErr)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "denied peer %s\n", formatPeerID(peerID, false))
}

func newControlClient(cmd *cobra.Command) controlv1connect.ControlServiceClient {
	return newControlClientWithTimeout(cmd, controlClientTimeout)
}

func newControlClientWithTimeout(cmd *cobra.Command, timeout time.Duration) controlv1connect.ControlServiceClient {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to prepare pln dir: %v\n", err)
		os.Exit(1)
	}

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
