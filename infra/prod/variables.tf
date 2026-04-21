variable "zone_name" {
  description = "Apex domain managed in Cloudflare."
  type        = string
  default     = "pln.sh"
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key that gets installed as root@ on each node."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}
