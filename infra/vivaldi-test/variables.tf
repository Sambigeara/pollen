variable "pln_version" {
  description = "Pollen release tag to install (e.g. 'v0.1.0-alpha.36'). Required to avoid GitHub API rate limiting across 28 nodes."
  type        = string
}

variable "nodes_per_region" {
  type    = number
  default = 7
}

variable "instance_type" {
  type    = string
  default = "t4g.nano"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}
