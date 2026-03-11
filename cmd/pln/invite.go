package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
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
	cmd.Flags().Duration("cert-ttl", 0, "Certificate TTL for the invited peer (0 = use node default)")
	cmd.Flags().StringArray("bootstrap", nil, "Bootstrap peer as <peer-pub-hex>@<host:port> (repeatable)")
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

	bootstrapSpecs, err := cmd.Flags().GetStringArray("bootstrap")
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	certTTL, err := getCertTTLFlag(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	bootstraps, err := resolveBootstrapPeers(cmd, bootstrapSpecs)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	encoded, err := createInviteToken(cmd, subjectPub, ttl, certTTL, bootstraps)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}

func createInviteToken(cmd *cobra.Command, subjectPub ed25519.PublicKey, ttl, certTTL time.Duration, bootstrap []*admissionv1.BootstrapPeer) (string, error) {
	if ttl <= 0 {
		return "", errors.New("token ttl must be positive")
	}

	issuerCtx, err := loadTokenIssuerContext(cmd)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("this node cannot issue invites; only delegated admins can sign invite tokens")
		}
		return "", err
	}

	token, err := auth.IssueInviteTokenWithSigner(issuerCtx.signer, subjectPub, bootstrap, time.Now(), ttl, certTTL)
	if err != nil {
		return "", err
	}

	return auth.EncodeInviteToken(token)
}

func resolveBootstrapPeers(cmd *cobra.Command, specs []string) ([]*admissionv1.BootstrapPeer, error) {
	if len(specs) > 0 {
		return parseBootstrapSpecs(specs)
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return nil, err
	}

	cfg, err := config.Load(pollenDir)
	if err != nil {
		return nil, err
	}
	registered, err := cfg.BootstrapProtoPeers()
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

func localBootstrapPeer(cmd *cobra.Command) (*admissionv1.BootstrapPeer, error) {
	client := newControlClient(cmd)
	resp, err := client.GetBootstrapInfo(cmd.Context(), connect.NewRequest(&controlv1.GetBootstrapInfoRequest{}))
	if err != nil {
		return nil, err
	}

	if rec := resp.Msg.GetRecommended(); rec != nil && rec.GetPeer() != nil && len(rec.GetAddrs()) > 0 {
		return &admissionv1.BootstrapPeer{
			PeerPub: append([]byte(nil), rec.GetPeer().GetPeerId()...),
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
		PeerPub: append([]byte(nil), self.GetPeer().GetPeerId()...),
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

func parseBootstrapSpecs(specs []string) ([]*admissionv1.BootstrapPeer, error) {
	byPeer := map[string]*admissionv1.BootstrapPeer{}
	var order []string

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
