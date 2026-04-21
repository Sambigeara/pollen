variable "instance_type" {
  description = "EC2 instance type for the dedicated load generator."
  type        = string
  default     = "t4g.small"
}

variable "ssh_public_key" {
  description = "SSH public key contents to install on the instance."
  type        = string
}

variable "scraper_cidr" {
  description = "CIDR allowed to scrape the exerciser's metrics ports."
  type        = string
  default     = "0.0.0.0/0"
}

variable "metrics_port_from" {
  description = "First TCP port in the exerciser metrics range."
  type        = number
  default     = 9192
}

variable "metrics_port_to" {
  description = "Last TCP port in the exerciser metrics range."
  type        = number
  default     = 9201
}
