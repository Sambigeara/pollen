package main

import (
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/stretchr/testify/require"
)

func TestCertExpiryFooterWithinSkew(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Certificates: []*controlv1.CertInfo{{
			NotAfterUnix: time.Now().Add(-30 * time.Second).Unix(),
		}},
	}

	footer := certExpiryFooter(st)
	require.Contains(t, footer, "membership expires in ")
}

func TestCertExpiryFooterBeyondSkew(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Certificates: []*controlv1.CertInfo{{
			NotAfterUnix: time.Now().Add(-2 * time.Minute).Unix(),
			Health:       controlv1.CertHealth_CERT_HEALTH_EXPIRED,
		}},
	}

	footer := certExpiryFooter(st)
	require.Contains(t, footer, "membership expired")
}
