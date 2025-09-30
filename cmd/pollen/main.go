package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/flynn/noise"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/peers"
	"github.com/sambigeara/pollen/pkg/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spf13/cobra"
)

const (
	pollenDir  = ".pollen"
	socketName = "pollen.sock"
)

func main() {
	base, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("unable to retrieve user config dir: %v", err)
	}
	defaultRootDir := filepath.Join(base, pollenDir)

	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("root", defaultRootDir, "Pollen root directory where local state is persisted")

	seedCmd := &cobra.Command{
		Use:   "seed [wasm-file]",
		Short: "Deploy a function to the network",
		Run:   runSeed,
	}

	runCmd := &cobra.Command{
		Use:   "run [function-name]",
		Short: "Execute a function",
		Run:   runRun,
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all functions",
		Run:   runList,
	}

	inviteCmd := &cobra.Command{
		Use:   "invite",
		Short: "Generate an invite token for a peer",
		Run:   runInvite,
	}
	inviteCmd.Flags().String("addr", "", "Peer address for the invite")
	inviteCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	rootCmd.AddCommand(seedCmd, runCmd, listCmd, inviteCmd)
	rootCmd.Execute()
}

func runSeed(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Println("Error: wasm file argument required")
		cmd.Usage()
		return
	}

	wasmFile := args[0]

	sockPath, err := resolveSocketPath(cmd)
	if err != nil {
		log.Fatalf("failed to resolve socket: %v", err)
	}

	client, cleanup, err := connectToControlService(sockPath)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	_, err = client.Seed(ctx, &controlv1.SeedRequest{
		WasmPath: wasmFile,
	})
	if err != nil {
		log.Fatalf("failed to seed function: %v", err)
	}

	cmd.Printf("Successfully seeded function from %s\n", wasmFile)
}

func runRun(cmd *cobra.Command, args []string) {

	if len(args) == 0 {
		cmd.Println("Error: function name required")
		cmd.Usage()
		return
	}

	fn := args[0]

	sockPath, err := resolveSocketPath(cmd)
	if err != nil {
		log.Fatalf("failed to resolve socket: %v", err)
	}

	client, cleanup, err := connectToControlService(sockPath)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	resp, err := client.Run(ctx, &controlv1.RunRequest{
		Name: fn,
	})
	if err != nil {
		log.Fatalf("failed to run function: %v", err)
	}

	cmd.Printf("%s\n", resp.Result)
}

func runList(cmd *cobra.Command, args []string) {
	sockPath, err := resolveSocketPath(cmd)
	if err != nil {
		log.Fatalf("failed to resolve socket: %v", err)
	}

	client, cleanup, err := connectToControlService(sockPath)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	resp, err := client.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
	if err != nil {
		log.Fatalf("failed to list functions: %v", err)
	}

	cmd.Printf("%s\n", strings.Join(resp.Functions, "\n"))
}

func connectToControlService(sockPath string) (controlv1.ControlServiceClient, func() error, error) {
	conn, err := grpc.NewClient("unix:"+sockPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	client := controlv1.NewControlServiceClient(conn)
	return client, conn.Close, nil
}

func resolveSocketPath(cmd *cobra.Command) (string, error) {
	root, _ := cmd.Flags().GetString("root")
	pollenDir, err := workspace.EnsurePollenDir(root)
	if err != nil {
		return "", err
	}

	return filepath.Join(pollenDir, socketName), nil
}

func runInvite(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("addr")
	dir, _ := cmd.Flags().GetString("dir")

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	credsDir := filepath.Join(pollenDir, workspace.CredsDir)
	noiseKey, err := node.GenLocalStaticKey(cs, credsDir)
	if err != nil {
		log.Fatalf("failed to load noise key: %v", err)
	}

	token, err := node.NewInvite(addr, noiseKey.Public)
	if err != nil {
		log.Fatalf("failed to generate invite: %v", err)
	}

	encoded, err := node.EncodeToken(token)
	if err != nil {
		log.Fatalf("failed to encode invite: %v", err)
	}

	peersStore, err := peers.Load(filepath.Join(pollenDir, workspace.PeersDir))
	if err != nil {
		log.Fatalf("failed to load peers store: %v", err)
	}
	defer peersStore.Save()

	peersStore.AddInvite(token)

	cmd.Printf("%s\n", encoded)
}
