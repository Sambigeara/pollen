package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/flynn/noise"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenDir      = ".pollen"
	socketName     = "pollen.sock"
	defaultUDPPort = "51820"
)

func main() {
	base, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("unable to retrieve user config dir: %v", err)
	}
	defaultRootDir := filepath.Join(base, pollenDir)

	rootCmd := &cobra.Command{Use: "pollen"}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("port", defaultUDPPort, "Listening port")
	nodeCmd.Flags().String("join", "", "Invite token to join remote peer")
	nodeCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	inviteCmd := &cobra.Command{
		Use:   "invite",
		Short: "Generate an invite token for a peer",
		Run:   runInvite,
	}
	inviteCmd.Flags().IP("ip", []byte{}, "IP address advertised to peers")
	inviteCmd.Flags().String("port", defaultUDPPort, "Port advertised to peers")
	inviteCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	rootCmd.AddCommand(nodeCmd, inviteCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
}

func runNode(cmd *cobra.Command, args []string) {
	logging.Init()
	defer zap.L().Sync()

	zap.L().Info("starting pollen...", zap.String("version", "0.1.0")) // TODO(saml) retrieve version
	log := zap.S().Named("pollen")

	portStr, _ := cmd.Flags().GetString("port")
	joinToken, _ := cmd.Flags().GetString("join")
	dir, _ := cmd.Flags().GetString("dir")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	var tkn *peerv1.Invite
	if joinToken != "" {
		tkn, err = node.DecodeToken(joinToken)
		if err != nil {
			log.Fatal(err)
		}
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("port '%s' is invalid: %v", portStr, err)
	}

	n, err := node.New(port, pollenDir)
	if err != nil {
		log.Fatal(err)
	}

	nodeSrv := node.NewNodeService(n)

	cmd.Print("Successfully started node\n")

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(ctx context.Context) error {
		grpcSrv := server.NewGRPCServer()
		// TODO(saml) this needs to be self healing, in case another local node which originally owned
		// the grpc server port disappears and this one needs to claim it.
		return grpcSrv.Start(ctx, nodeSrv, filepath.Join(pollenDir, socketName))
	})

	p.Go(func(ctx context.Context) error {
		return n.Start(ctx, tkn)
	})

	if err := p.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		log.Fatal(err)
	}
}

func runInvite(cmd *cobra.Command, args []string) {
	ip, _ := cmd.Flags().GetIP("ip")
	port, _ := cmd.Flags().GetString("port")
	dir, _ := cmd.Flags().GetString("dir")

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	credsDir := filepath.Join(pollenDir, workspace.CredsDir)
	noiseKey, err := mesh.GenStaticKey(cs, credsDir)
	if err != nil {
		log.Fatalf("failed to load noise key: %v", err)
	}

	if ip == nil {
		var err error
		ip, err = mesh.DiscoverIP()
		if err != nil {
			log.Fatalf("failed to infer public IP")
		}
	}

	token, err := node.NewInvite(ip, port, noiseKey.Public)
	if err != nil {
		log.Fatalf("failed to generate invite: %v", err)
	}

	encoded, err := node.EncodeToken(token)
	if err != nil {
		log.Fatalf("failed to encode invite: %v", err)
	}

	invitesStore, err := invites.Load(filepath.Join(pollenDir, workspace.PeersDir))
	if err != nil {
		log.Fatalf("failed to load peers store: %v", err)
	}
	defer invitesStore.Save()

	invitesStore.AddInvite(token)

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}
