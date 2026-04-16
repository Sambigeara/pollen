terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }
}

resource "aws_key_pair" "pollen" {
  key_name   = "pollen-demo"
  public_key = var.ssh_public_key
}

resource "aws_vpc" "main" {
  cidr_block = "${var.vpc_cidr_prefix}.0.0/16"

  tags = { Name = "pollen-demo-${var.node_name}" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "${var.vpc_cidr_prefix}.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = { Name = "pollen-demo-${var.node_name}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "pollen" {
  name   = "pollen-demo-${var.node_name}"
  vpc_id = aws_vpc.main.id

  ingress {
    description = "Pollen UDP"
    from_port   = 60611
    to_port     = 60611
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress" {
    for_each = length(var.control_ingress_cidrs) > 0 ? [1] : []
    content {
      description = "Pollen control gRPC (external clients)"
      from_port   = var.control_port
      to_port     = var.control_port
      protocol    = "tcp"
      cidr_blocks = var.control_ingress_cidrs
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "node" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.pollen.id]
  key_name               = aws_key_pair.pollen.key_name

  user_data = <<-INIT
    #!/bin/bash
    cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/authorized_keys
  INIT

  tags = { Name = "pollen-demo-${var.node_name}" }
}
