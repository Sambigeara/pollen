// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/cas"
)

func newStaticCmds() []*cobra.Command {
	seed := &cobra.Command{
		Use:   "seed <name> <dir>",
		Short: "Publish a static site",
		Long:  "Walks <dir>, stores each file in the local blob store, builds a manifest blob keyed by relative path, and publishes the site under <name>. Other nodes replicate the manifest plus referenced blobs and claim the site.",
		Args:  cobra.ExactArgs(2), //nolint:mnd
		RunE:  withEnv(runStaticSeed),
	}
	seed.Flags().Uint32("min-replicas", 1, "minimum number of claiming replicas")

	unseed := &cobra.Command{
		Use:   "unseed <name>",
		Short: "Unpublish a static site",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runStaticUnseed),
	}

	ls := &cobra.Command{
		Use:   "ls",
		Short: "List published static sites",
		Args:  cobra.NoArgs,
		RunE:  withEnv(runStaticLs),
	}

	root := &cobra.Command{Use: "static", Short: "Work with static sites"}
	root.AddCommand(seed, unseed, ls)
	return []*cobra.Command{root}
}

func runStaticSeed(cmd *cobra.Command, args []string, env *cliEnv) error {
	name := args[0]
	dir := args[1]

	store, err := cas.New(env.dir)
	if err != nil {
		return fmt.Errorf("open blob store: %w", err)
	}

	var paths []*statev1.StaticPath
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		hash, putErr := store.Put(f)
		if putErr != nil {
			return fmt.Errorf("put %s: %w", path, putErr)
		}
		digest, _ := hex.DecodeString(hash)
		rel, _ := filepath.Rel(dir, path)
		rel = "/" + strings.TrimPrefix(filepath.ToSlash(rel), "./")
		paths = append(paths, &statev1.StaticPath{Path: rel, Digest: digest})
		if _, err := env.client.AnnounceBlob(cmd.Context(), connect.NewRequest(&controlv1.AnnounceBlobRequest{Hash: hash})); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: could not announce %s: %v\n", hash, err) //nolint:gosec
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no files found in %s", dir)
	}

	manifest := &statev1.StaticManifest{Paths: paths}
	buf, err := manifest.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	manifestHash, err := store.Put(bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("put manifest: %w", err)
	}
	manifestDigest, _ := hex.DecodeString(manifestHash)

	if _, err := env.client.AnnounceBlob(cmd.Context(), connect.NewRequest(&controlv1.AnnounceBlobRequest{Hash: manifestHash})); err != nil {
		return fmt.Errorf("announce manifest: %w", err)
	}

	minReplicas, _ := cmd.Flags().GetUint32("min-replicas")
	if _, err := env.client.SeedStatic(cmd.Context(), connect.NewRequest(&controlv1.SeedStaticRequest{
		Name:           name,
		ManifestDigest: manifestDigest,
		MinReplicas:    minReplicas,
	})); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s (%d files, manifest %s)\n", name, len(paths), manifestHash[:shortHexLen]) //nolint:gosec
	return nil
}

func runStaticUnseed(cmd *cobra.Command, args []string, env *cliEnv) error {
	if _, err := env.client.UnseedStatic(cmd.Context(), connect.NewRequest(&controlv1.UnseedStaticRequest{Name: args[0]})); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "unseeded %s\n", args[0]) //nolint:gosec
	return nil
}

func runStaticLs(cmd *cobra.Command, _ []string, env *cliEnv) error {
	resp, err := env.client.ListStatic(cmd.Context(), connect.NewRequest(&controlv1.ListStaticRequest{}))
	if err != nil {
		return err
	}
	for _, site := range resp.Msg.GetSites() {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%d replicas\t%d claimed\n", //nolint:gosec
			site.GetName(),
			hex.EncodeToString(site.GetManifestDigest())[:shortHexLen],
			site.GetMinReplicas(),
			len(site.GetClaimants()),
		)
	}
	return nil
}
