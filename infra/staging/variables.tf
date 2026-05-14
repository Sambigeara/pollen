variable "ssh_key_name" {
  description = "Name of an existing Hetzner SSH key to attach to staging nodes. Defaults to a separate key (\"pln-staging\") so prod and staging never share a credential. Register the key in Hetzner (one-off) or override with `-var ssh_key_name=...` if the operator prefers to reuse the prod key."
  type        = string
  default     = "pln-staging"
}

variable "ssh_source_ips" {
  description = "CIDRs permitted to reach staging nodes on :22. Defaults to all source IPs so the current operator workflow keeps working; harden by overriding with [\"X.X.X.X/32\"] in a tfvars file or `-var 'ssh_source_ips=[...]'` once an operator IP is committed."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

variable "tenant_wildcard_proxied" {
  description = "Whether *.<subdomain>.<zone_name> is orange-cloud (proxied via Cloudflare) or grey-cloud (direct to origin). HTTPS for two-level subdomains needs Advanced Certificate Manager on the zone (billable; enable via the CF dashboard before flipping this to true). Until then, the wildcard answers grey-cloud and tenant sites are reachable over plain HTTP via the origin :8080."
  type        = bool
  default     = false
}
