variable "region_name" {
  description = "AWS region name (used for resource naming)"
  type        = string
}

variable "nodes_per_region" {
  type = number
}

variable "instance_type" {
  type = string
}

variable "ssh_public_key" {
  description = "Contents of the SSH public key"
  type        = string
}

variable "pln_version" {
  description = "Pollen release tag to install"
  type        = string
}
