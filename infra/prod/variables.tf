variable "ssh_key_name" {
  description = "Name of an existing Hetzner SSH key to attach to prod nodes. Hetzner enforces uniqueness on key material; the key is looked up by name rather than re-registered to avoid resource-replace cascades on state drift."
  type        = string
  default     = "pln-prod"
}

variable "ssh_source_ips" {
  description = "CIDRs permitted to reach prod nodes on :22. Defaults to all source IPs so the current operator workflow keeps working; harden by overriding with [\"X.X.X.X/32\"] in a tfvars file or `-var 'ssh_source_ips=[...]'` once an operator IP is committed."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}
