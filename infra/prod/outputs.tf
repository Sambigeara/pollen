output "node_ips" {
  description = "Map of node name → public IPv4."
  value       = { for k, s in hcloud_server.node : k => s.ipv4_address }
}

output "node_hostnames" {
  description = "Map of node name → public hostname (grey-cloud DNS record)."
  value       = { for k, s in hcloud_server.node : k => "${s.name}.${var.zone_name}" }
}

output "bootstrap_targets" {
  description = "Input for `pln bootstrap ssh -` (one name=target per line). Uses IPs rather than hostnames to avoid any local DNS caching during fresh bring-up."
  value       = join("\n", [for k, s in hcloud_server.node : "${k}=root@${s.ipv4_address}"])
}
