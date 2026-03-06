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

variable "vpc_cidr_prefix" {
  description = "First two octets of the VPC CIDR (e.g. '10.1' gives 10.1.0.0/16)"
  type        = string
}
