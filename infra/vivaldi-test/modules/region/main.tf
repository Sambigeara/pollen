terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

resource "aws_key_pair" "pollen" {
  key_name   = "pollen-vivaldi"
  public_key = var.ssh_public_key
}

data "aws_vpc" "default" { default = true }

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "pollen" {
  name        = "pollen-vivaldi-sg"
  description = "Pollen Vivaldi test cluster"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Pollen UDP Control Plane"
    from_port   = 60611
    to_port     = 60611
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu*-22.04-arm64-server-*"]
  }
}

resource "aws_instance" "node" {
  count                       = var.nodes_per_region
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.pollen.id]
  key_name                    = aws_key_pair.pollen.key_name
  associate_public_ip_address = true

  user_data = <<-EOF
    #!/bin/bash
    curl -sL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash -s -- --version ${var.pln_version}
  EOF

  tags = {
    Name = "pollen-vivaldi-${var.region_name}-${count.index}"
  }
}
