output "node_ips" {
  description = "Map of node name → public IPv4."
  value       = { for k, s in hcloud_server.node : k => s.ipv4_address }
}

output "node_hostnames" {
  description = "Map of node name → public grey-cloud hostname."
  value       = { for k, s in hcloud_server.node : k => "${s.name}.${local.zone_name}" }
}

output "bootstrap_targets" {
  description = "Input for `pln bootstrap ssh -` (one name=target per line). Uses IPs to dodge any local DNS caching during fresh bring-up."
  value       = join("\n", [for k, s in hcloud_server.node : "${k}=root@${s.ipv4_address}"])
}

output "edge_host" {
  description = "Wire-mode endpoint tenants paste into `pln ctx add staging pln://<host>:7443`."
  value       = "edge.${local.staging_host}"
}

output "apex_host" {
  description = "Public-facing apex of the staging cluster (proxied via Cloudflare)."
  value       = local.staging_host
}
