terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

resource "aws_key_pair" "pollen" {
  key_name   = "pollen-vivaldi-public"
  public_key = var.ssh_public_key
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu*-22.04-arm64-server-*"]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block = "${var.vpc_cidr_prefix}.0.0/16"
  tags       = { Name = "pollen-vivaldi-public-${var.region_name}" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "pollen-vivaldi-public-${var.region_name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "${var.vpc_cidr_prefix}.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "pollen-vivaldi-public-${var.region_name}-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "pollen-vivaldi-public-${var.region_name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "public" {
  name        = "pollen-vivaldi-public-${var.region_name}"
  description = "All-public pollen node"
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

resource "aws_instance" "public" {
  count                       = var.nodes_per_region
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.public.id]
  key_name                    = aws_key_pair.pollen.key_name
  associate_public_ip_address = true

  tags = { Name = "pollen-vivaldi-public-${var.region_name}-${count.index}" }
}
