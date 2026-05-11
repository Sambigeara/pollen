terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.49"
    }
  }
}

provider "hcloud" {}

variable "ssh_key_name" {
  type        = string
  default     = "pln-prod"
  description = "Name of an existing Hetzner SSH key to authorise on the test nodes."
}

locals {
  nodes = {
    nbg1-1 = "nbg1"
    nbg1-2 = "nbg1"
    hel1   = "hel1"
  }
}

data "hcloud_ssh_key" "pollen" {
  name = var.ssh_key_name
}

resource "hcloud_firewall" "pollen" {
  name = "pollen-hetzner"

  rule {
    description = "SSH"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "Pollen UDP"
    direction   = "in"
    protocol    = "udp"
    port        = "60611"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "node" {
  for_each     = local.nodes
  name         = "pollen-test-${each.key}"
  server_type  = "cax11"
  image        = "ubuntu-22.04"
  location     = each.value
  ssh_keys     = [data.hcloud_ssh_key.pollen.id]
  firewall_ids = [hcloud_firewall.pollen.id]
}

output "node_ips" {
  value = { for k, s in hcloud_server.node : k => s.ipv4_address }
}

output "root_node_ip" {
  value = hcloud_server.node["nbg1-1"].ipv4_address
}

output "all_ips" {
  value = [for s in hcloud_server.node : s.ipv4_address]
}
