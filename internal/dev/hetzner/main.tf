terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.49"
    }
  }
}

provider "hcloud" {}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

locals {
  nodes = {
    nbg1-1 = "nbg1"
    nbg1-2 = "nbg1"
    hel1   = "hel1"
  }
}

resource "hcloud_ssh_key" "pollen" {
  name       = "pollen-hetzner"
  public_key = file(var.ssh_public_key_path)
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
  ssh_keys     = [hcloud_ssh_key.pollen.id]
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
