variable "instance_type" {
  type    = string
  default = "c7g.medium"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "scraper_cidr" {
  description = "CIDR allowed to scrape the exerciser metrics endpoints. Set to your laptop's public IP (e.g. 203.0.113.5/32) to tighten the demo."
  type        = string
  default     = "0.0.0.0/0"
}
