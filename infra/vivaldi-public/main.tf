terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

locals {
  ssh_public_key = file(var.ssh_public_key_path)
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

provider "aws" {
  alias  = "eu_west_1"
  region = "eu-west-1"
}

provider "aws" {
  alias  = "ap_southeast_1"
  region = "ap-southeast-1"
}

provider "aws" {
  alias  = "sa_east_1"
  region = "sa-east-1"
}

module "us_east_1" {
  source    = "./modules/region-public"
  providers = { aws = aws.us_east_1 }

  region_name      = "us-east-1"
  vpc_cidr_prefix  = "10.1"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
}

module "eu_west_1" {
  source    = "./modules/region-public"
  providers = { aws = aws.eu_west_1 }

  region_name      = "eu-west-1"
  vpc_cidr_prefix  = "10.2"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
}

module "ap_southeast_1" {
  source    = "./modules/region-public"
  providers = { aws = aws.ap_southeast_1 }

  region_name      = "ap-southeast-1"
  vpc_cidr_prefix  = "10.3"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
}

module "sa_east_1" {
  source    = "./modules/region-public"
  providers = { aws = aws.sa_east_1 }

  region_name      = "sa-east-1"
  vpc_cidr_prefix  = "10.4"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
}

output "public_ips_by_region" {
  value = {
    eu_west_1      = module.eu_west_1.public_ips
    us_east_1      = module.us_east_1.public_ips
    ap_southeast_1 = module.ap_southeast_1.public_ips
    sa_east_1      = module.sa_east_1.public_ips
  }
}

output "root_node_ip" {
  value = module.eu_west_1.public_ips[0]
}

output "all_public_ips" {
  value = concat(
    module.eu_west_1.public_ips,
    module.us_east_1.public_ips,
    module.ap_southeast_1.public_ips,
    module.sa_east_1.public_ips,
  )
}

output "ssh_commands" {
  value = {
    for region, info in {
      eu_west_1      = module.eu_west_1.public_ips
      us_east_1      = module.us_east_1.public_ips
      ap_southeast_1 = module.ap_southeast_1.public_ips
      sa_east_1      = module.sa_east_1.public_ips
    } : region => [for ip in info : "ssh -o StrictHostKeyChecking=no ubuntu@${ip}"]
  }
}

output "cluster_status_cmd" {
  value = "ssh -o StrictHostKeyChecking=no ubuntu@${module.eu_west_1.public_ips[0]} 'sudo pln status'"
}
