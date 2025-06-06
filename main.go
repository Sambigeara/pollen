package main

import (
	"strings"
	"time"

	"github.com/sambigeara/pollen/pkg/node"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("listen", ":8080", "Listen address")
	nodeCmd.Flags().String("peers", "", "Initial peers")

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

	rootCmd.AddCommand(nodeCmd, seedCmd, runCmd)
	rootCmd.Execute()
}

func runNode(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("listen")
	peers, _ := cmd.Flags().GetString("peers")

	node := node.New(addr)

	if peers != "" {
		for _, peer := range strings.Split(peers, ",") {
			node.AddPeer(peer)
		}
	}

	go node.StartGossip()

	go func() {
		time.Sleep(2 * time.Second)
		node.AddFunction("hello-world2", "abc123")
	}()

	node.Listen()
}

func runSeed(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Println("Error: wasm file argument required")
		cmd.Usage()
		return
	}

	wasmFile := args[0]
	_ = wasmFile
	// node.AddFunction("hello-world2", wasmFile)
}

func runRun(cmd *cobra.Command, args []string) {}
