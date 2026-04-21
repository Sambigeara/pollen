// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

var hashPattern = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

func newBlobCmds() []*cobra.Command {
	putCmd := &cobra.Command{
		Use:   "put <file> [name]",
		Short: "Upload a blob to the daemon and print its hash",
		Long:  "Reads <file> (or stdin when <file> is '-') and uploads it to the connected daemon, which stores it in its content-addressed blob store and announces availability to the cluster. Prints the SHA-256 hex digest. If [name] is given, the blob is published under that name and other peers can fetch it via `pln blob fetch <name>`. Re-publishing the same content under a new name renames it.",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		RunE:  withEnv(runBlobPut),
	}

	fetchCmd := &cobra.Command{
		Use:   "fetch <hash-or-name>",
		Short: "Pull a blob into the daemon's content store",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runBlobFetch),
	}
	fetchCmd.Flags().String("from", "", "force-fetch from this peer (hex); by default any peer advertising the blob is acceptable")

	rmCmd := &cobra.Command{
		Use:   "rm <hash-or-name>",
		Short: "Remove a blob from the daemon's store and drop its name",
		Long:  "Evicts the blob's bytes from the daemon's content store, un-announces availability, and drops any name this peer published for it. Other peers keep their own copies until they evict locally.",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runBlobRm),
	}

	blobCmd := &cobra.Command{Use: "blob", Short: "Work with content-addressed blobs"}
	blobCmd.AddCommand(putCmd, fetchCmd, rmCmd)
	return []*cobra.Command{blobCmd}
}

func runBlobPut(cmd *cobra.Command, args []string, env *cliEnv) error {
	var r io.Reader
	if args[0] == "-" {
		r = cmd.InOrStdin()
	} else {
		f, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("open %s: %w", args[0], err)
		}
		defer f.Close()
		r = f
	}

	header := &controlv1.UploadBlobHeader{}
	if len(args) > 1 {
		name := args[1]
		header.Name = &name
	}
	hash, err := uploadBlob(cmd, env, header, r)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", hash) //nolint:gosec
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

func runBlobFetch(cmd *cobra.Command, args []string, env *cliEnv) error {
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

	fmt.Fprintf(cmd.OutOrStdout(), "fetched %s\n", hash) //nolint:gosec
	return nil
}

func runBlobRm(cmd *cobra.Command, args []string, env *cliEnv) error {
	hash, err := resolveBlob(cmd, env, args[0])
	if err != nil {
		return err
	}
	if _, err := env.client.RemoveBlob(cmd.Context(), connect.NewRequest(&controlv1.RemoveBlobRequest{Hash: hash})); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", args[0])
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

	return "", fmt.Errorf("no blob matching %q — run \"pln status blobs\" to see available blobs", arg)
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
	return errors.New(strings.TrimSpace(b.String()))
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
		fmt.Fprintf(&b, "  pln blob fetch %s-%s    (%s, published by %s)\n", name, prefixes[i], hash, formatPeerID(blob.GetPublisher().GetPeerPub(), false))
	}
	return errors.New(strings.TrimSpace(b.String()))
}
