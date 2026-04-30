// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"connectrpc.com/connect"
	units "github.com/docker/go-units"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

var hashPattern = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

const (
	streamChunkBytes       = 1 << 20 // stays below the default 4 MiB gRPC per-message cap
	defaultWorkloadMinReps = 2
)

type seedKind int

const (
	kindUnknown seedKind = iota
	kindWorkload
	kindStatic
	kindBlob
)

func (k seedKind) String() string {
	switch k {
	case kindWorkload:
		return "workload"
	case kindStatic:
		return "static site"
	case kindBlob:
		return "blob"
	default:
		return "unknown"
	}
}

func newSeedCmds() []*cobra.Command {
	seedCmd := &cobra.Command{
		Use:   "seed <source> [name]",
		Short: "Publish content to the mesh (workload, static site, or blob)",
		Long: `Publish content to the mesh. The kind is autodetected from <source>:

  *.wasm file → WASM workload
  directory   → static site
  any other   → blob (or stdin when <source> is '-')

Without [name], the workload/site is named after the file/directory and
the blob remains anonymous (addressed only by hash).`,
		Args: cobra.RangeArgs(1, 2), //nolint:mnd
		RunE: withEnv(runSeed),
	}
	seedCmd.Flags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
		if name == "mem" {
			return "memory"
		}
		return pflag.NormalizedName(name)
	})
	seedCmd.Flags().Uint32("min-replicas", 0, "minimum replicas (workload only; default 2)")
	seedCmd.Flags().Bool("everywhere", false, "place workload on every node (workload only)")
	seedCmd.Flags().String("memory", "", "memory limit, e.g. 64MiB, 128M (alias --mem; workload only; default 8MiB)")
	seedCmd.Flags().Duration("timeout", 0, "per-invocation timeout (workload only, e.g. 500ms, 5s)")
	seedCmd.Example = `  pln seed ./hello.wasm                     # workload, named "hello"
  pln seed ./hello.wasm greeter --everywhere # workload on every node
  pln seed ./public my-site                 # static site
  pln seed ./big-file.bin payload           # named blob
  cat data | pln seed -                     # anonymous blob from stdin`

	unseedCmd := &cobra.Command{
		Use:   "unseed <name-or-hash>",
		Short: "Stop publishing content (workload, static site, or blob)",
		Long: `Resolves the name or hash against currently-published workloads, static
sites, and blobs, and removes the matching one. Errors if multiple kinds
share the same name.`,
		Example: "  pln unseed hello\n  pln unseed ab12cd34   # by hash prefix",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runUnseed),
	}

	fetchCmd := &cobra.Command{
		Use:   "fetch <name-or-hash>",
		Short: "Pull a blob into the local content store",
		Long: `Forces this node to download a blob from a peer that already advertises
it. Useful for pre-warming caches or pinning content locally. Without
--from, any peer holding the blob is acceptable.`,
		Example: "  pln fetch payload\n  pln fetch ab12cd34 --from <peer-hex>",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runFetch),
	}
	fetchCmd.Flags().String("from", "", "force-fetch from this peer (hex); by default any peer advertising the blob is acceptable")

	return []*cobra.Command{seedCmd, unseedCmd, fetchCmd}
}

func detectSeedKind(source string) (seedKind, error) {
	if source == "-" {
		return kindBlob, nil
	}
	info, err := os.Stat(source)
	if err != nil {
		return kindUnknown, fmt.Errorf("open %s: %w", source, err)
	}
	if info.IsDir() {
		return kindStatic, nil
	}
	if strings.HasSuffix(strings.ToLower(source), ".wasm") {
		return kindWorkload, nil
	}
	return kindBlob, nil
}

func validateSeedFlags(cmd *cobra.Command, kind seedKind) error {
	workloadOnly := []string{"everywhere", "memory", "timeout", "min-replicas"}
	if kind != kindWorkload {
		for _, f := range workloadOnly {
			if cmd.Flags().Changed(f) {
				return fmt.Errorf("--%s only applies to .wasm workloads", f)
			}
		}
	}
	return nil
}

func runSeed(cmd *cobra.Command, args []string, env *cliEnv) error {
	source := args[0]
	var name string
	if len(args) > 1 {
		name = args[1]
	}

	kind, err := detectSeedKind(source)
	if err != nil {
		return err
	}
	if err := validateSeedFlags(cmd, kind); err != nil {
		return err
	}

	switch kind {
	case kindWorkload:
		return seedWorkload(cmd, env, source, name)
	case kindStatic:
		return seedStatic(cmd, env, source, name)
	case kindBlob:
		return seedBlob(cmd, env, source, name)
	case kindUnknown:
		return fmt.Errorf("could not detect kind for %q", source)
	}
	return nil
}

func seedWorkload(cmd *cobra.Command, env *cliEnv, source, name string) error {
	f, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", source, err)
	}
	defer f.Close()

	if name == "" {
		name = strings.TrimSuffix(filepath.Base(source), ".wasm")
	}

	minReplicas := uint32(defaultWorkloadMinReps)
	if cmd.Flags().Changed("min-replicas") {
		minReplicas, _ = cmd.Flags().GetUint32("min-replicas")
	}
	everywhere, _ := cmd.Flags().GetBool("everywhere")
	memoryStr, _ := cmd.Flags().GetString("memory")
	memoryBytes, err := parseMemoryBytes(memoryStr)
	if err != nil {
		return fmt.Errorf("invalid --memory: %w", err)
	}
	timeout, _ := cmd.Flags().GetDuration("timeout")

	var spread float32
	if everywhere {
		spread = 1.0
	}

	stream := env.client.SeedWorkload(cmd.Context())
	if err := stream.Send(&controlv1.SeedWorkloadRequest{
		Payload: &controlv1.SeedWorkloadRequest_Header{
			Header: &controlv1.SeedWorkloadHeader{
				Name:        name,
				MinReplicas: minReplicas,
				Spread:      spread,
				MemoryBytes: memoryBytes,
				TimeoutMs:   uint32(timeout.Milliseconds()),
			},
		},
	}); err != nil {
		return err
	}

	buf := make([]byte, streamChunkBytes)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&controlv1.SeedWorkloadRequest{
				Payload: &controlv1.SeedWorkloadRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read %s: %w", source, readErr)
		}
	}

	resp, err := stream.CloseAndReceive()
	if err != nil {
		return err
	}

	hash := resp.Msg.GetHash()
	if len(hash) > shortHexLen {
		hash = hash[:shortHexLen]
	}
	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s (%s)\n", resp.Msg.GetName(), hash)
	return nil
}

func seedStatic(cmd *cobra.Command, env *cliEnv, dir, name string) error {
	if name == "" {
		name = filepath.Base(filepath.Clean(dir))
	}

	var paths []*statev1.StaticPath
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		f, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		defer f.Close()
		hash, uploadErr := uploadBlob(cmd, env, &controlv1.UploadBlobHeader{}, f)
		if uploadErr != nil {
			return fmt.Errorf("upload %s: %w", path, uploadErr)
		}
		digest, _ := hex.DecodeString(hash)
		rel, _ := filepath.Rel(dir, path)
		rel = "/" + strings.TrimPrefix(filepath.ToSlash(rel), "./")
		paths = append(paths, &statev1.StaticPath{Path: rel, Digest: digest})
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
	manifestHash, err := uploadBlob(cmd, env, &controlv1.UploadBlobHeader{}, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("upload manifest: %w", err)
	}
	manifestDigest, _ := hex.DecodeString(manifestHash)

	if _, err := env.client.SeedStatic(cmd.Context(), connect.NewRequest(&controlv1.SeedStaticRequest{
		Name:           name,
		ManifestDigest: manifestDigest,
	})); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s (%d files, manifest %s)\n", name, len(paths), manifestHash[:shortHexLen])
	return nil
}

func seedBlob(cmd *cobra.Command, env *cliEnv, source, name string) error {
	var r io.Reader
	if source == "-" {
		r = cmd.InOrStdin()
	} else {
		f, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("open %s: %w", source, err)
		}
		defer f.Close()
		r = f
	}

	header := &controlv1.UploadBlobHeader{}
	if name != "" {
		header.Name = &name
	}

	hash, err := uploadBlob(cmd, env, header, r)
	if err != nil {
		return err
	}
	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "seeded %s (%s)\n", name, hash[:shortHexLen]) //nolint:gosec
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n", hash) //nolint:gosec
	}
	return nil
}

func uploadBlob(cmd *cobra.Command, env *cliEnv, header *controlv1.UploadBlobHeader, r io.Reader) (string, error) {
	stream := env.client.UploadBlob(cmd.Context())
	if err := stream.Send(&controlv1.UploadBlobRequest{
		Payload: &controlv1.UploadBlobRequest_Header{Header: header},
	}); err != nil {
		return "", err
	}
	buf := make([]byte, streamChunkBytes)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&controlv1.UploadBlobRequest{
				Payload: &controlv1.UploadBlobRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return "", err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", fmt.Errorf("read blob: %w", readErr)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		return "", err
	}
	return resp.Msg.GetHash(), nil
}

// runUnseed dispatches to the right RPC by inspecting the daemon's
// status. Workload, static, and blob each have their own resolver; if
// the arg matches in more than one kind we error rather than guess.
func runUnseed(cmd *cobra.Command, args []string, env *cliEnv) error {
	arg := args[0]

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}
	st := statusResp.Msg

	wl := matchWorkloadArg(st.GetWorkloads(), arg)
	site := matchStaticArg(st.GetSites(), arg)
	blobHash, blobErr := matchBlobArg(st.GetBlobs(), arg)
	hasBlob := blobErr == nil
	// Distinguish "no match" from "ambiguous" — only the latter should
	// short-circuit; "no match" lets the other kinds still satisfy.
	var blobAmbiguous error
	if blobErr != nil && !strings.HasPrefix(blobErr.Error(), "no blob") {
		blobAmbiguous = blobErr
	}

	matches := 0
	var kinds []string
	if wl != nil {
		matches++
		kinds = append(kinds, "workload "+wl.GetName())
	}
	if site != nil {
		matches++
		kinds = append(kinds, "static site "+site.GetName())
	}
	if hasBlob {
		matches++
		kinds = append(kinds, "blob "+blobHash[:shortHexLen])
	}

	if matches == 0 {
		if blobAmbiguous != nil {
			return blobAmbiguous
		}
		return notFoundErr("no workload, static site, or blob matching %q", arg)
	}
	if matches > 1 {
		return ambiguousErr("multiple matches for %q (%s) — remove one first or use a more specific identifier", arg, strings.Join(kinds, ", "))
	}

	switch {
	case wl != nil:
		if _, err := env.client.UnseedWorkload(cmd.Context(), connect.NewRequest(&controlv1.UnseedWorkloadRequest{Hash: arg})); err != nil {
			return err
		}
	case site != nil:
		if _, err := env.client.UnseedStatic(cmd.Context(), connect.NewRequest(&controlv1.UnseedStaticRequest{Name: site.GetName()})); err != nil {
			return err
		}
	case hasBlob:
		if _, err := env.client.RemoveBlob(cmd.Context(), connect.NewRequest(&controlv1.RemoveBlobRequest{Hash: blobHash})); err != nil {
			return err
		}
	}

	fmt.Fprintf(cmd.OutOrStdout(), "unseeded %s\n", arg)
	return nil
}

func runFetch(cmd *cobra.Command, args []string, env *cliEnv) error {
	hash, err := resolveBlob(cmd, env, args[0])
	if err != nil {
		return err
	}

	req := &controlv1.FetchBlobRequest{Hash: hash}
	if from, _ := cmd.Flags().GetString("from"); from != "" {
		peerPub, err := hex.DecodeString(from)
		if err != nil {
			return fmt.Errorf("invalid --from peer key: %w", err)
		}
		req.PeerPub = peerPub
	}
	if _, err := env.client.FetchBlob(cmd.Context(), connect.NewRequest(req)); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "fetched %s\n", hash)
	return nil
}

func resolveBlob(cmd *cobra.Command, env *cliEnv, arg string) (string, error) {
	if hashPattern.MatchString(arg) {
		return strings.ToLower(arg), nil
	}
	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return "", err
	}
	return matchBlobArg(statusResp.Msg.GetBlobs(), arg)
}

// matchWorkloadArg returns the unique workload matching arg by name, full
// hash, or hash prefix. Returns nil for no match. Ambiguity is rare for
// workloads because names are unique within the spec namespace; if the
// hash prefix is ambiguous we conservatively return nil and let the
// daemon's UnseedWorkload do its own resolution.
func matchWorkloadArg(workloads []*controlv1.WorkloadSummary, arg string) *controlv1.WorkloadSummary {
	for _, w := range workloads {
		if w.GetName() == arg || w.GetHash() == arg {
			return w
		}
	}
	lower := strings.ToLower(arg)
	if !isHexPrefix(lower) {
		return nil
	}
	var found *controlv1.WorkloadSummary
	for _, w := range workloads {
		if strings.HasPrefix(w.GetHash(), lower) {
			if found != nil {
				return nil
			}
			found = w
		}
	}
	return found
}

func matchStaticArg(sites []*controlv1.StaticSummary, arg string) *controlv1.StaticSummary {
	for _, s := range sites {
		if s.GetName() == arg {
			return s
		}
	}
	return nil
}

func matchBlobArg(blobs []*controlv1.BlobSummary, arg string) (string, error) {
	var nameMatches []*controlv1.BlobSummary
	for _, b := range blobs {
		if b.GetName() == arg {
			nameMatches = append(nameMatches, b)
		}
	}
	if len(nameMatches) == 1 {
		return nameMatches[0].GetHash(), nil
	}
	if len(nameMatches) > 1 {
		return "", blobCollisionError(arg, nameMatches)
	}

	lower := strings.ToLower(arg)
	var hashes []string
	for _, b := range blobs {
		if strings.HasPrefix(b.GetHash(), lower) {
			hashes = append(hashes, b.GetHash())
		}
	}
	if len(hashes) == 1 {
		return hashes[0], nil
	}
	if len(hashes) > 1 {
		return "", hashPrefixCollisionError(arg, hashes)
	}

	if blob, err := resolveBySuffix(arg, blobs, func(b *controlv1.BlobSummary) suffixCandidate {
		return suffixCandidate{name: b.GetName(), peerKey: peerKeyString(b.GetPublisher().GetPeerPub()), include: b.GetName() != ""}
	}, blobCollisionError); !errors.Is(err, errNoSuffixMatch) {
		if err != nil {
			return "", err
		}
		return blob.GetHash(), nil
	}

	return "", notFoundErr("no blob matching %q — run \"pln status blobs\" to see available blobs", arg)
}

func hashPrefixCollisionError(prefix string, hashes []string) error {
	prefixes := minUniquePrefixes(hashes)
	var b strings.Builder
	fmt.Fprintf(&b, "prefix %q matches multiple blobs — pick one:\n", prefix)
	for i, hash := range hashes {
		short := hash
		if len(short) > shortHexLen {
			short = short[:shortHexLen]
		}
		fmt.Fprintf(&b, "  %s    (%s)\n", prefixes[i], short)
	}
	return wrapExit(exitAmbiguous, errors.New(strings.TrimSpace(b.String())))
}

func blobCollisionError(name string, matches []*controlv1.BlobSummary) error {
	ids := make([]string, len(matches))
	for i, b := range matches {
		ids[i] = peerKeyString(b.GetPublisher().GetPeerPub())
	}
	prefixes := minUniquePrefixes(ids)

	var b strings.Builder
	fmt.Fprintf(&b, "multiple blobs match %q — pick one:\n", name)
	for i, blob := range matches {
		hash := blob.GetHash()
		if len(hash) > shortHexLen {
			hash = hash[:shortHexLen]
		}
		fmt.Fprintf(&b, "  pln fetch %s-%s    (%s, published by %s)\n", name, prefixes[i], hash, formatPeerID(blob.GetPublisher().GetPeerPub(), false))
	}
	return wrapExit(exitAmbiguous, errors.New(strings.TrimSpace(b.String())))
}

func isHexPrefix(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

// parseMemoryBytes parses a human-readable memory size ("64MiB", "128M",
// "1GiB"). An empty string returns 0, which tells the server to use its
// default.
func parseMemoryBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	n, err := units.RAMInBytes(s)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, errors.New("size must be positive")
	}
	return uint64(n), nil
}
