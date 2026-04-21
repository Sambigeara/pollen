terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ---------------------------------------------------------------------------
# Provider aliases — one per region, 10 globally distributed locations.
# All are default-enabled AWS regions (no opt-in required).
# ---------------------------------------------------------------------------

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

provider "aws" {
  alias  = "us_west_2"
  region = "us-west-2"
}

provider "aws" {
  alias  = "eu_west_1"
  region = "eu-west-1"
}

provider "aws" {
  alias  = "eu_north_1"
  region = "eu-north-1"
}

provider "aws" {
  alias  = "eu_central_1"
  region = "eu-central-1"
}

provider "aws" {
  alias  = "ap_northeast_1"
  region = "ap-northeast-1"
}

provider "aws" {
  alias  = "ap_southeast_1"
  region = "ap-southeast-1"
}

provider "aws" {
  alias  = "ap_southeast_2"
  region = "ap-southeast-2"
}

provider "aws" {
  alias  = "ap_south_1"
  region = "ap-south-1"
}

provider "aws" {
  alias  = "sa_east_1"
  region = "sa-east-1"
}

locals {
  ssh_public_key = file(var.ssh_public_key_path)

  # Aggregate module outputs into a flat name→ip map for use in outputs.
  modules = {
    use = module.use.ip
    usw = module.usw.ip
    euw = module.euw.ip
    eun = module.eun.ip
    euc = module.euc.ip
    tok = module.tok.ip
    sgp = module.sgp.ip
    syd = module.syd.ip
    mum = module.mum.ip
    sao = module.sao.ip
  }

  # Ingress allow-list for every pollen node's control endpoint.
  # Only the dedicated exerciser host needs external TCP access.
  node_control_ingress_cidrs = [module.exerciser.eip_cidr]
}

# ---------------------------------------------------------------------------
# Dedicated exerciser host — one EC2 instance in us-east-1 that runs the
# load generator under systemd and targets any of the 10 pollen nodes via
# their TCP control endpoints. No pln daemon on this host.
# ---------------------------------------------------------------------------

module "exerciser" {
  source    = "./modules/exerciser-host"
  providers = { aws = aws.us_east_1 }

  ssh_public_key = local.ssh_public_key
  scraper_cidr   = var.scraper_cidr
}

# ---------------------------------------------------------------------------
# Nodes — one per region, each gets its own VPC and public instance.
# ---------------------------------------------------------------------------

module "use" {
  source    = "./modules/node"
  providers = { aws = aws.us_east_1 }

  node_name             = "use"
  vpc_cidr_prefix       = "10.0"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "usw" {
  source    = "./modules/node"
  providers = { aws = aws.us_west_2 }

  node_name             = "usw"
  vpc_cidr_prefix       = "10.1"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "euw" {
  source    = "./modules/node"
  providers = { aws = aws.eu_west_1 }

  node_name             = "euw"
  vpc_cidr_prefix       = "10.2"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "eun" {
  source    = "./modules/node"
  providers = { aws = aws.eu_north_1 }

  node_name             = "eun"
  vpc_cidr_prefix       = "10.3"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "euc" {
  source    = "./modules/node"
  providers = { aws = aws.eu_central_1 }

  node_name             = "euc"
  vpc_cidr_prefix       = "10.4"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "tok" {
  source    = "./modules/node"
  providers = { aws = aws.ap_northeast_1 }

  node_name             = "tok"
  vpc_cidr_prefix       = "10.5"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "sgp" {
  source    = "./modules/node"
  providers = { aws = aws.ap_southeast_1 }

  node_name             = "sgp"
  vpc_cidr_prefix       = "10.6"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "syd" {
  source    = "./modules/node"
  providers = { aws = aws.ap_southeast_2 }

  node_name             = "syd"
  vpc_cidr_prefix       = "10.7"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "mum" {
  source    = "./modules/node"
  providers = { aws = aws.ap_south_1 }

  node_name             = "mum"
  vpc_cidr_prefix       = "10.8"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}

module "sao" {
  source    = "./modules/node"
  providers = { aws = aws.sa_east_1 }

  node_name             = "sao"
  vpc_cidr_prefix       = "10.9"
  instance_type         = var.instance_type
  ssh_public_key        = local.ssh_public_key
  control_ingress_cidrs = local.node_control_ingress_cidrs
}
