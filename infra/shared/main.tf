terraform {
  required_version = ">= 1.7"
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.0"
    }
  }
}

provider "cloudflare" {}

# Zone-singleton state shared by every cluster on this Cloudflare zone.
# Cloudflare permits exactly one custom ruleset per `http_request_origin`
# phase per zone, and zone settings (SSL mode, min TLS, etc) apply
# globally. Lifting these out of any single cluster's tf keeps `just
# prod-down` or `just staging-down` from collaterally tearing the zone
# down.

data "cloudflare_zone" "pln" {
  filter = { name = var.zone_name }
}

locals {
  # Hosts handled by the gateway listener (:8088) for anonymous share URLs.
  # Listed once and reused in both the explicit rule and the catch-all
  # exclusion so they cannot drift.
  staging_gateway_hosts = [
    "blob.${var.staging_subdomain}.${var.zone_name}",
    "fn.${var.staging_subdomain}.${var.zone_name}",
  ]
}

# Route CF → origin on :8080 for static traffic and :8088 for the
# anonymous share gateway. The pln daemons run as the unprivileged
# `pln` user without CAP_NET_BIND_SERVICE, so all listeners sit above
# :1024.
resource "cloudflare_ruleset" "origin_port" {
  zone_id = data.cloudflare_zone.pln.id
  name    = "origin port override"
  kind    = "zone"
  phase   = "http_request_origin"

  rules = [
    {
      description = "Static sites listen on :8080"
      expression  = "(http.host in {\"${var.zone_name}\" \"docs.${var.zone_name}\"})"
      action      = "route"
      enabled     = true
      action_parameters = {
        origin = {
          port = 8080
        }
      }
    },
    {
      description = "Staging static apex → :8080"
      expression  = "(http.host eq \"${var.staging_subdomain}.${var.zone_name}\")"
      action      = "route"
      enabled     = true
      action_parameters = {
        origin = {
          port = 8080
        }
      }
    },
    {
      description = "Staging blob/fn gateway → :8088"
      expression  = "(http.host in {${join(" ", [for h in local.staging_gateway_hosts : "\"${h}\""])}})"
      action      = "route"
      enabled     = true
      action_parameters = {
        origin = {
          port = 8088
        }
      }
    },
    {
      description = "Staging tenant sites (catch-all under *.${var.staging_subdomain}, excluding gateway hosts) → :8080"
      expression  = "(ends_with(http.host, \".${var.staging_subdomain}.${var.zone_name}\") and not (http.host in {${join(" ", [for h in local.staging_gateway_hosts : "\"${h}\""])}}))"
      action      = "route"
      enabled     = true
      action_parameters = {
        origin = {
          port = 8080
        }
      }
    },
  ]
}

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
