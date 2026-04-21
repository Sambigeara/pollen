terraform {
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

# Cloudflare publishes its edge network CIDRs at these static URLs. We
# fetch them at plan time so the origin firewall stays current without
# a hardcoded list.
data "http" "cf_ipv4" { url = "https://www.cloudflare.com/ips-v4" }
data "http" "cf_ipv6" { url = "https://www.cloudflare.com/ips-v6" }

locals {
  # EU runs cpx22 (Hetzner's new-gen replacement for the deprecated
  # cpx21 in EU DCs); US stays on cpx21. Both AMD x86_64 so one amd64
  # binary serves both.
  nodes = {
    eu = { location = "nbg1", hostname = "eu", server_type = "cpx22" }
    us = { location = "ash", hostname = "us", server_type = "cpx21" }
  }

  cf_cidrs = concat(
    split("\n", trimspace(data.http.cf_ipv4.response_body)),
    split("\n", trimspace(data.http.cf_ipv6.response_body)),
  )
}

resource "hcloud_ssh_key" "pln" {
  name       = "pln-prod"
  public_key = file(var.ssh_public_key_path)
}

resource "hcloud_firewall" "pln" {
  name = "pln-prod"

  rule {
    description = "SSH"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "Pollen mesh UDP"
    direction   = "in"
    protocol    = "udp"
    port        = "60611"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "HTTP (origin for Cloudflare proxy — CF edge CIDRs only)"
    direction   = "in"
    protocol    = "tcp"
    port        = "8080"
    source_ips  = local.cf_cidrs
  }
}

resource "hcloud_server" "node" {
  for_each     = local.nodes
  name         = "pln-prod-${each.key}"
  server_type  = each.value.server_type
  image        = "ubuntu-22.04"
  location     = each.value.location
  ssh_keys     = [hcloud_ssh_key.pln.id]
  firewall_ids = [hcloud_firewall.pln.id]
}

data "cloudflare_zone" "pln" {
  filter = { name = var.zone_name }
}

# Apex (pln.sh) and docs — proxied through Cloudflare so it terminates TLS
# and we stay plain HTTP on the origin.
resource "cloudflare_dns_record" "apex" {
  for_each = hcloud_server.node
  zone_id  = data.cloudflare_zone.pln.id
  name     = var.zone_name
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = true
  comment  = "apex → pln-prod-${each.key}"
}

resource "cloudflare_dns_record" "docs" {
  for_each = hcloud_server.node
  zone_id  = data.cloudflare_zone.pln.id
  name     = "docs"
  type     = "A"
  content  = each.value.ipv4_address
  ttl      = 1
  proxied  = true
  comment  = "docs → pln-prod-${each.key}"
}

# www.pln.sh → pln.sh (301, preserves path + query). A CNAME to apex
# gives CF a host to answer for; the ruleset below rewrites the response.
resource "cloudflare_dns_record" "www" {
  zone_id = data.cloudflare_zone.pln.id
  name    = "www"
  type    = "CNAME"
  content = var.zone_name
  ttl     = 1
  proxied = true
  comment = "www redirects to apex"
}

resource "cloudflare_ruleset" "www_redirect" {
  zone_id = data.cloudflare_zone.pln.id
  name    = "www redirect"
  kind    = "zone"
  phase   = "http_request_dynamic_redirect"

  rules = [{
    description = "Redirect www.pln.sh to apex"
    expression  = "(http.host eq \"www.${var.zone_name}\")"
    action      = "redirect"
    enabled     = true
    action_parameters = {
      from_value = {
        status_code = 301
        target_url = {
          expression = "concat(\"https://${var.zone_name}\", http.request.uri.path)"
        }
        preserve_query_string = true
      }
    }
  }]
}

# Route CF → origin on :8080 instead of the default :80, so the pln
# static-http listener can run as the unprivileged pln user without
# needing CAP_NET_BIND_SERVICE.
resource "cloudflare_ruleset" "origin_port" {
  zone_id = data.cloudflare_zone.pln.id
  name    = "origin port override"
  kind    = "zone"
  phase   = "http_request_origin"

  rules = [{
    description = "Static sites listen on :8080"
    expression  = "(http.host in {\"${var.zone_name}\" \"docs.${var.zone_name}\"})"
    action      = "route"
    enabled     = true
    action_parameters = {
      origin = {
        port = 8080
      }
    }
  }]
}

# Per-node hostnames (grey-cloud, direct to IP) so `pln ctx add prod
# root@<hostname>` resolves straight to the origin and the UDP mesh port
# isn't fronted by Cloudflare (which only proxies HTTPS).
resource "cloudflare_dns_record" "node" {
  for_each = hcloud_server.node
  zone_id  = data.cloudflare_zone.pln.id
  name     = each.value.name
  type     = "A"
  content  = hcloud_server.node[each.key].ipv4_address
  ttl      = 300
  proxied  = false
  comment  = "direct hostname for pln-prod-${each.key}"
}

# Zone security settings. SSL mode stays flexible (origin is plain HTTP
# on :8080 via the origin port ruleset) — not strict.
resource "cloudflare_zone_setting" "always_use_https" {
  zone_id    = data.cloudflare_zone.pln.id
  setting_id = "always_use_https"
  value      = "on"
}

resource "cloudflare_zone_setting" "automatic_https_rewrites" {
  zone_id    = data.cloudflare_zone.pln.id
  setting_id = "automatic_https_rewrites"
  value      = "on"
}

resource "cloudflare_zone_setting" "min_tls_version" {
  zone_id    = data.cloudflare_zone.pln.id
  setting_id = "min_tls_version"
  value      = "1.2"
}

resource "cloudflare_zone_setting" "tls_1_3" {
  zone_id    = data.cloudflare_zone.pln.id
  setting_id = "tls_1_3"
  value      = "on"
}
