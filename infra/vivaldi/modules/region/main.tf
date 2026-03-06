terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

# --- Key Pair & AMI ---

resource "aws_key_pair" "pollen" {
  key_name   = "pollen-vivaldi"
  public_key = var.ssh_public_key
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu*-22.04-arm64-server-*"]
  }
}

# --- VPC & Networking ---

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block = "${var.vpc_cidr_prefix}.0.0/16"
  tags       = { Name = "pollen-vivaldi-${var.region_name}" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "pollen-vivaldi-${var.region_name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "${var.vpc_cidr_prefix}.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "pollen-vivaldi-${var.region_name}-public" }
}

resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "${var.vpc_cidr_prefix}.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  tags              = { Name = "pollen-vivaldi-${var.region_name}-private" }
}

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "pollen-vivaldi-${var.region_name}-nat-eip" }
}

resource "aws_instance" "nat" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t4g.nano"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.nat.id]
  key_name                    = aws_key_pair.pollen.key_name
  associate_public_ip_address = true
  source_dest_check           = false

  user_data = <<-NATEOF
    #!/bin/bash
    set -e
    echo 'net.ipv4.ip_forward = 1' > /etc/sysctl.d/90-nat.conf
    sysctl -w net.ipv4.ip_forward=1
    IFACE=$(ip -o route show default | awk '{print $5}' | head -1)
    iptables -t nat -A POSTROUTING -o "$IFACE" -s ${var.vpc_cidr_prefix}.0.0/16 -j MASQUERADE
    iptables -P FORWARD ACCEPT
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq iptables-persistent
    netfilter-persistent save
  NATEOF

  depends_on = [aws_internet_gateway.main]
  tags       = { Name = "pollen-vivaldi-${var.region_name}-nat" }
}

resource "aws_eip_association" "nat" {
  instance_id   = aws_instance.nat.id
  allocation_id = aws_eip.nat.id
}

# --- Route Tables ---

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "pollen-vivaldi-${var.region_name}-public-rt" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block           = "0.0.0.0/0"
    network_interface_id = aws_instance.nat.primary_network_interface_id
  }
  tags = { Name = "pollen-vivaldi-${var.region_name}-private-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}

# --- Security Groups ---

resource "aws_security_group" "nat" {
  name        = "pollen-vivaldi-${var.region_name}-nat"
  description = "NAT instance - allows all UDP for hole punching"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
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

resource "aws_security_group" "public" {
  name        = "pollen-vivaldi-${var.region_name}-public"
  description = "Public relay node"
  vpc_id      = aws_vpc.main.id

  ingress {
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

resource "aws_security_group" "private" {
  name        = "pollen-vivaldi-${var.region_name}-private"
  description = "Private node behind NAT"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 60611
    to_port     = 60611
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.public.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Instances ---

resource "aws_instance" "public" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.public.id]
  key_name                    = aws_key_pair.pollen.key_name
  associate_public_ip_address = true

  tags = { Name = "pollen-vivaldi-${var.region_name}-public-0" }
}

resource "aws_instance" "private" {
  count                       = var.nodes_per_region - 1
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.private.id
  vpc_security_group_ids      = [aws_security_group.private.id]
  key_name                    = aws_key_pair.pollen.key_name
  associate_public_ip_address = false

  tags = { Name = "pollen-vivaldi-${var.region_name}-private-${count.index}" }
}
