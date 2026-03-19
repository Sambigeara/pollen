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

resource "hcloud_ssh_key" "pollen" {
  name       = "pollen-bootstrap"
  public_key = file(var.ssh_public_key_path)
}

resource "hcloud_firewall" "pollen" {
  name = "pollen-bootstrap"

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

resource "hcloud_server" "pollen" {
  name         = "pollen-bootstrap"
  server_type  = "cax11"
  image        = "ubuntu-22.04"
  location     = "nbg1"
  ssh_keys     = [hcloud_ssh_key.pollen.id]
  firewall_ids = [hcloud_firewall.pollen.id]
}

output "ip" { value = hcloud_server.pollen.ipv4_address }
