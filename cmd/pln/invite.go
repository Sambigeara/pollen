package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

func newInviteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "invite [subject-pub]",
		Short: "Generate an invite token (open or subject-bound)",
		Args:  cobra.RangeArgs(0, 1), //nolint:mnd
		Run:   runInvite,
	}
	cmd.Flags().String("subject", "", "Optional hex node public key to bind invite")
	cmd.Flags().Duration("ttl", defaultJoinTokenTTL, "Invite token validity duration")
	cmd.Flags().Duration("expire-after", 0, "Hard access expiry for the invited peer (e.g. 24h)")
	return cmd
}

func runInvite(cmd *cobra.Command, args []string) {
	subjectFlag, err := cmd.Flags().GetString("subject")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	subjectPub, err := resolveInviteSubject(subjectFlag, args)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	ttl, err := cmd.Flags().GetDuration("ttl")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	expireAfter, err := getExpireAfterFlag(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	pollenDir, _ := cmd.Flags().GetString("dir")
	bootstraps, err := resolveBootstrapPeers(cmd.Context(), pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	encoded, err := createInviteToken(pollenDir, subjectPub, ttl, expireAfter, bootstraps)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}

func createInviteToken(pollenDir string, subjectPub ed25519.PublicKey, ttl, expireAfter time.Duration, bootstrap []*admissionv1.BootstrapPeer) (string, error) {
	if ttl <= 0 {
		return "", errors.New("token ttl must be positive")
	}

	issuerCtx, err := loadTokenIssuerContext(pollenDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("this node cannot issue invites; only delegated admins can sign invite tokens")
		}
		return "", err
	}

	token, err := issuerCtx.signer.IssueInviteToken(subjectPub, bootstrap, time.Now(), ttl, expireAfter)
	if err != nil {
		return "", err
	}

	return auth.EncodeInviteToken(token)
}

func resolveBootstrapPeers(ctx context.Context, pollenDir string) ([]*admissionv1.BootstrapPeer, error) {
	cfg, err := config.Load(pollenDir)
	if err != nil {
		return nil, err
	}
	if len(cfg.BootstrapPeers) > 0 {
		peers := make([]*admissionv1.BootstrapPeer, 0, len(cfg.BootstrapPeers))
		for peerHex, addrs := range cfg.BootstrapPeers {
			pk, _ := types.PeerKeyFromString(peerHex)
			peers = append(peers, &admissionv1.BootstrapPeer{
				PeerPub: pk.Bytes(),
				Addrs:   addrs,
			})
		}
		return peers, nil
	}

	bootstrap, err := localBootstrapPeer(ctx, pollenDir)
	if err != nil {
		return nil, err
	}
	return []*admissionv1.BootstrapPeer{bootstrap}, nil
}

func localBootstrapPeer(ctx context.Context, pollenDir string) (*admissionv1.BootstrapPeer, error) {
	client := newControlClient(pollenDir)
	resp, err := client.GetBootstrapInfo(ctx, connect.NewRequest(&controlv1.GetBootstrapInfoRequest{}))
	if err != nil {
		return nil, err
	}

	if rec := resp.Msg.GetRecommended(); rec != nil && rec.GetPeer() != nil && len(rec.GetAddrs()) > 0 {
		return &admissionv1.BootstrapPeer{
			PeerPub: append([]byte(nil), rec.GetPeer().GetPeerPub()...),
			Addrs:   append([]string(nil), rec.GetAddrs()...),
		}, nil
	}

	self := resp.Msg.GetSelf()
	if self == nil || self.GetPeer() == nil {
		return nil, errors.New("missing bootstrap peer information")
	}
	if len(self.GetAddrs()) == 0 {
		return nil, errors.New("bootstrap peer has no advertised addresses")
	}

	return &admissionv1.BootstrapPeer{
		PeerPub: append([]byte(nil), self.GetPeer().GetPeerPub()...),
		Addrs:   append([]string(nil), self.GetAddrs()...),
	}, nil
}

func resolveInviteSubject(subjectFlag string, args []string) (ed25519.PublicKey, error) {
	flagVal := strings.TrimSpace(subjectFlag)

	if len(args) == 1 && flagVal != "" {
		return nil, errors.New("provide subject as positional argument or --subject, not both")
	}

	var subject string
	switch {
	case len(args) == 1:
		subject = strings.TrimSpace(args[0])
	case flagVal != "":
		subject = flagVal
	default:
		return nil, nil
	}

	pk, err := types.PeerKeyFromString(subject)
	if err != nil {
		return nil, err
	}
	return ed25519.PublicKey(pk.Bytes()), nil
}
