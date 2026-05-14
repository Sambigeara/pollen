terraform {
  required_version = ">= 1.7"
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.49"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.0"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3.4"
    }
  }
}

provider "hcloud" {}
provider "cloudflare" {}

data "http" "cf_ipv4" { url = "https://www.cloudflare.com/ips-v4" }
data "http" "cf_ipv6" { url = "https://www.cloudflare.com/ips-v6" }

# Zone-singleton state (origin-port ruleset, zone settings) lives in
# `infra/shared/`. Reading its state here makes the dependency explicit
# and lets staging compose its hostnames against the same subdomain the
# ruleset matches on.
data "terraform_remote_state" "shared" {
  backend = "local"
  config = {
    path = "${path.module}/../shared/terraform.tfstate"
  }
}

locals {
  # Same shape as prod: one EU and one US Hetzner AMD x86_64 node.
  nodes = {
    eu = { location = "nbg1", hostname = "eu", server_type = "cpx22" }
    us = { location = "ash", hostname = "us", server_type = "cpx21" }
  }

  cf_cidrs = concat(
    split("\n", trimspace(data.http.cf_ipv4.response_body)),
    split("\n", trimspace(data.http.cf_ipv6.response_body)),
  )

  zone_id      = data.terraform_remote_state.shared.outputs.zone_id
  zone_name    = data.terraform_remote_state.shared.outputs.zone_name
  subdomain    = data.terraform_remote_state.shared.outputs.staging_subdomain
  staging_host = "${local.subdomain}.${local.zone_name}"
}

data "hcloud_ssh_key" "pln" {
  name = var.ssh_key_name
}

resource "hcloud_firewall" "pln" {
  name = "pln-staging"

  rule {
    description = "SSH"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = var.ssh_source_ips
  }

  rule {
    description = "Pollen mesh UDP"
    direction   = "in"
    protocol    = "udp"
    port        = "60611"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "Static HTTP origin (Cloudflare CIDRs only)"
    direction   = "in"
    protocol    = "tcp"
    port        = "8080"
    source_ips  = local.cf_cidrs
  }

  rule {
    description = "Anonymous share gateway origin (Cloudflare CIDRs only)"
    direction   = "in"
    protocol    = "tcp"
    port        = "8088"
    source_ips  = local.cf_cidrs
  }

  rule {
    description = "Wire-mode control RPC (mTLS terminated by pln)"
    direction   = "in"
    protocol    = "tcp"
    port        = "7443"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "node" {
  for_each     = local.nodes
  name         = "pln-staging-${each.key}"
  server_type  = each.value.server_type
  image        = "ubuntu-22.04"
  location     = each.value.location
  ssh_keys     = [data.hcloud_ssh_key.pln.id]
  firewall_ids = [hcloud_firewall.pln.id]

  # ssh_keys is forceNew in the hcloud provider. Changing `ssh_key_name`
  # at the variable level resolves to a different key id and would queue
  # a destroy + recreate of every node, wiping /var/lib/pln. Ignoring
  # ssh_keys keeps the variable change a no-op for existing servers;
  # fresh bring-ups still consume the current default.
  lifecycle {
    ignore_changes = [ssh_keys]
  }
}

# Apex staging.pln.sh, proxied, fronts the static handler.
resource "cloudflare_dns_record" "apex" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = local.staging_host
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = true
  comment  = "staging apex → pln-staging-${each.key}"
}

# blob.staging.pln.sh, anonymous blob gateway.
resource "cloudflare_dns_record" "blob" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = "blob.${local.staging_host}"
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = true
  comment  = "staging blob gateway → pln-staging-${each.key}"
}

# fn.staging.pln.sh, anonymous workload gateway.
resource "cloudflare_dns_record" "fn" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = "fn.${local.staging_host}"
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = true
  comment  = "staging fn gateway → pln-staging-${each.key}"
}

# *.staging.pln.sh, wildcard for tenant static sites
# (`<name>-<short-pub>.staging.pln.sh`). CF Universal SSL only covers
# one level under the apex, so HTTPS for these hosts relies on the
# advanced cert pack below. Until ACM is enabled on the zone, leave
# `tenant_wildcard_proxied = false` so the grey-cloud record at least
# answers the hostname over plain HTTP via the origin :8080.
resource "cloudflare_dns_record" "tenant_wildcard" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = "*.${local.staging_host}"
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = var.tenant_wildcard_proxied
  comment  = "staging tenant wildcard → pln-staging-${each.key}"
}

# Per-node hostnames (grey cloud) for SSH and direct mesh dial.
resource "cloudflare_dns_record" "node" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = each.value.name
  type     = "A"
  content  = hcloud_server.node[each.key].ipv4_address
  ttl      = 300
  proxied  = false
  comment  = "direct hostname for pln-staging-${each.key}"
}

# edge.staging.pln.sh, wire-mode endpoint for tenant CLIs.
#
# CF can't proxy custom-protocol mTLS-over-TCP, so the endpoint stays
# grey-cloud and serves multi-A. Resolvers pick essentially at random,
# which trades worst-case round-trip latency for redundancy. The choice
# is deliberate: cross-slot tombstone propagation (Phase 3f) means
# every node accepts seeds and unseeds for any publisher, so the "wrong
# node" problem doesn't bite correctness, only latency. CF Load
# Balancing would add proximity steering but needs the paid
# Load Balancing subscription enabled on the account; defer until
# prod-grade tenant traffic justifies it.
resource "cloudflare_dns_record" "edge" {
  for_each = hcloud_server.node
  zone_id  = local.zone_id
  name     = "edge.${local.staging_host}"
  type     = "A"
  content  = hcloud_server.node[each.key].ipv4_address
  ttl      = 300
  proxied  = false
  comment  = "wire-mode endpoint → pln-staging-${each.key}"
}

# Advanced cert pack covering the two-level staging subdomains.
# Universal SSL only reaches one level under the apex, so blob/fn and
# the tenant-static wildcard need a custom cert. Requires Advanced
# Certificate Manager on the zone (billable; enable via the CF
# dashboard before applying). Validation TXT records are managed by
# CF inside the zone; they do not appear in tf state.
resource "cloudflare_certificate_pack" "staging_wildcard" {
  zone_id               = local.zone_id
  type                  = "advanced"
  hosts                 = [local.staging_host, "*.${local.staging_host}"]
  validation_method     = "txt"
  validity_days         = 90
  certificate_authority = "lets_encrypt"
}

# Origin-port routing for staging hostnames lives in
# `infra/shared/cloudflare_ruleset.origin_port` (the zone permits only
# one custom ruleset per `http_request_origin` phase). Staging brings
# up the nodes, DNS, and the wildcard cert pack; the route rules live
# in the shared module because rulesets are zone-singletons.
