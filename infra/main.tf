terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

provider "aws" {
  region = "eu-west-2" # London
}

resource "aws_key_pair" "pollen" {
  key_name   = "pollen"
  public_key = file("~/.ssh/id_ed25519.pub")
}

resource "aws_security_group" "pollen" {
  name        = "pollen-sg"
  description = "Pollen ingress"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 60611
    to_port     = 60611
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # ingress {
  #   from_port   = 443
  #   to_port     = 443
  #   protocol    = "tcp"
  #   cidr_blocks = ["0.0.0.0/0"]
  # }
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

data "aws_vpc" "default" { default = true }
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_instance" "pollen" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t4g.nano"
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.pollen.id]
  key_name               = aws_key_pair.pollen.key_name
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu*-22.04-arm64-server-*"]
  }
}

resource "aws_eip" "pollen" { instance = aws_instance.pollen.id }

output "ip" { value = aws_eip.pollen.public_ip }
