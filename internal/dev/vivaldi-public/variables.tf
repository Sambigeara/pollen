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
