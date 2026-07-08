// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package types

import "net/netip"

// IsRoutableIP reports whether addr is a valid IP a remote peer could
// usefully dial: loopback, link-local, multicast, and unspecified are
// excluded.
func IsRoutableIP(addr netip.Addr) bool {
	return addr.IsValid() &&
		!addr.IsLoopback() &&
		!addr.IsLinkLocalUnicast() &&
		!addr.IsMulticast() &&
		!addr.IsUnspecified()
}

// IsPublicIP reports whether addr is publicly routable: routable AND
// not in a private (RFC1918 IPv4 or ULA IPv6) range.
func IsPublicIP(addr netip.Addr) bool {
	return IsRoutableIP(addr) && !addr.IsPrivate()
}
