variable "node_name" {
  type = string
}

variable "instance_type" {
  type = string
}

variable "ssh_public_key" {
  type = string
}

variable "vpc_cidr_prefix" {
  type        = string
  description = "First two octets (e.g. '10.0' gives 10.0.0.0/16)"
}

variable "control_ingress_cidrs" {
  description = "CIDRs allowed to reach the pollen node's TCP control endpoint (port 50051)."
  type        = list(string)
  default     = []
}

variable "control_port" {
  description = "TCP port exposed by `pln up --control-addr`."
  type        = number
  default     = 50051
}
