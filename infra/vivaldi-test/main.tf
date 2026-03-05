terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

locals {
  ssh_public_key = file(var.ssh_public_key_path)
  ssh_opts       = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

  root_ip = module.eu_west_1.public_ip

  all_public_ips = [
    module.eu_west_1.public_ip,
    module.us_east_1.public_ip,
    module.ap_southeast_1.public_ip,
    module.sa_east_1.public_ip,
  ]

  # Private nodes: "private_ip:bastion_public_ip" per region
  all_private_nodes = concat(
    [for ip in module.eu_west_1.private_ips : "${ip}:${module.eu_west_1.public_ip}"],
    [for ip in module.us_east_1.private_ips : "${ip}:${module.us_east_1.public_ip}"],
    [for ip in module.ap_southeast_1.private_ips : "${ip}:${module.ap_southeast_1.public_ip}"],
    [for ip in module.sa_east_1.private_ips : "${ip}:${module.sa_east_1.public_ip}"],
  )

  # Join nodes: "target_ip:bastion_ip:is_relay"
  # bastion_ip is empty for public nodes (direct SSH), populated for private nodes
  all_join_nodes = concat(
    # eu-west-1: only private nodes (root handled separately via cluster_init)
    [for ip in module.eu_west_1.private_ips : "${ip}:${module.eu_west_1.public_ip}:0"],
    # Other regions: public node as relay + private nodes
    ["${module.us_east_1.public_ip}::1"],
    [for ip in module.us_east_1.private_ips : "${ip}:${module.us_east_1.public_ip}:0"],
    ["${module.ap_southeast_1.public_ip}::1"],
    [for ip in module.ap_southeast_1.private_ips : "${ip}:${module.ap_southeast_1.public_ip}:0"],
    ["${module.sa_east_1.public_ip}::1"],
    [for ip in module.sa_east_1.private_ips : "${ip}:${module.sa_east_1.public_ip}:0"],
  )
}

# --- Providers ---

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

# --- Region Modules ---

module "us_east_1" {
  source    = "./modules/region"
  providers = { aws = aws.us_east_1 }

  region_name      = "us-east-1"
  vpc_cidr_prefix  = "10.1"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "eu_west_1" {
  source    = "./modules/region"
  providers = { aws = aws.eu_west_1 }

  region_name      = "eu-west-1"
  vpc_cidr_prefix  = "10.2"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "ap_southeast_1" {
  source    = "./modules/region"
  providers = { aws = aws.ap_southeast_1 }

  region_name      = "ap-southeast-1"
  vpc_cidr_prefix  = "10.3"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "sa_east_1" {
  source    = "./modules/region"
  providers = { aws = aws.sa_east_1 }

  region_name      = "sa-east-1"
  vpc_cidr_prefix  = "10.4"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

# --- Step 1: Wait for cloud-init to finish on all nodes ---

resource "null_resource" "wait_for_install" {
  triggers = {
    public_ips  = join(",", local.all_public_ips)
    private_ids = join(",", local.all_private_nodes)
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-SCRIPT
      set -euo pipefail
      SSH_OPTS="${local.ssh_opts}"

      ssh_to() {
        local TARGET="$1"; shift
        local BASTION="$1"; shift
        if [ -n "$BASTION" ]; then
          ssh $SSH_OPTS -o "ProxyCommand=ssh $SSH_OPTS -W %h:%p ubuntu@$BASTION" ubuntu@$TARGET "$@"
        else
          ssh $SSH_OPTS ubuntu@$TARGET "$@"
        fi
      }

      PIDS=()

      # Public nodes (direct SSH)
      for IP in ${join(" ", local.all_public_ips)}; do
        (
          for i in $(seq 1 90); do
            if ssh_to "$IP" "" "which pln" &>/dev/null; then
              echo "$IP: pln ready"
              exit 0
            fi
            sleep 2
          done
          echo "$IP: timed out waiting for pln"
          exit 1
        ) &
        PIDS+=($!)
      done

      # Private nodes (via bastion)
      for entry in ${join(" ", local.all_private_nodes)}; do
        TARGET=$(echo "$entry" | cut -d: -f1)
        BASTION=$(echo "$entry" | cut -d: -f2)
        (
          for i in $(seq 1 90); do
            if ssh_to "$TARGET" "$BASTION" "which pln" &>/dev/null; then
              echo "$TARGET (via $BASTION): pln ready"
              exit 0
            fi
            sleep 2
          done
          echo "$TARGET (via $BASTION): timed out waiting for pln"
          exit 1
        ) &
        PIDS+=($!)
      done

      FAILED=0
      for pid in "$${PIDS[@]}"; do
        if ! wait $pid; then FAILED=$((FAILED + 1)); fi
      done
      if [ $FAILED -gt 0 ]; then
        echo "$FAILED node(s) failed readiness check"
        exit 1
      fi
      echo "All nodes ready"
    SCRIPT
  }
}

# --- Step 2: Init root node (public node of eu-west-1) ---

resource "null_resource" "cluster_init" {
  depends_on = [null_resource.wait_for_install]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-SCRIPT
      set -euo pipefail
      SSH_OPTS="${local.ssh_opts}"
      ROOT="${local.root_ip}"
      ssh $SSH_OPTS ubuntu@$ROOT "sudo pln init"
      ssh $SSH_OPTS ubuntu@$ROOT "sudo pln up --public -d"
      sleep 3
    SCRIPT
  }
}

# --- Step 3: Generate invites and join all other nodes ---

resource "null_resource" "cluster_join" {
  depends_on = [null_resource.cluster_init]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-SCRIPT
      set -euo pipefail
      SSH_OPTS="${local.ssh_opts}"
      ROOT="${local.root_ip}"
      NODES="${join(" ", local.all_join_nodes)}"

      ssh_to() {
        local TARGET="$1"; shift
        local BASTION="$1"; shift
        if [ -n "$BASTION" ]; then
          ssh $SSH_OPTS -o "ProxyCommand=ssh $SSH_OPTS -W %h:%p ubuntu@$BASTION" ubuntu@$TARGET "$@"
        else
          ssh $SSH_OPTS ubuntu@$TARGET "$@"
        fi
      }

      # Phase A: generate invites sequentially on root
      declare -a TOKENS
      for entry in $NODES; do
        TOKEN=$(ssh $SSH_OPTS ubuntu@$ROOT "sudo pln invite --ttl 10m" 2>/dev/null)
        TOKENS+=("$TOKEN")
      done

      # Phase B: join all nodes in parallel
      PIDS=()
      IDX=0
      for entry in $NODES; do
        TARGET=$(echo "$entry" | cut -d: -f1)
        BASTION=$(echo "$entry" | cut -d: -f2)
        IS_RELAY=$(echo "$entry" | cut -d: -f3)
        TOKEN="$${TOKENS[$IDX]}"

        if [ "$IS_RELAY" = "1" ]; then
          ssh_to "$TARGET" "$BASTION" "sudo pln join --public '$TOKEN'" &
        else
          ssh_to "$TARGET" "$BASTION" "sudo pln join '$TOKEN'" &
        fi
        PIDS+=($!)
        IDX=$((IDX + 1))
      done

      FAILED=0
      for pid in "$${PIDS[@]}"; do
        if ! wait $pid; then FAILED=$((FAILED + 1)); fi
      done

      if [ $FAILED -gt 0 ]; then
        echo "WARNING: $FAILED node(s) failed to join"
        exit 1
      fi

      echo "All nodes joined successfully"
    SCRIPT
  }
}

# --- Outputs ---

output "ips_by_region" {
  value = {
    eu_west_1 = {
      public  = module.eu_west_1.public_ip
      private = module.eu_west_1.private_ips
    }
    us_east_1 = {
      public  = module.us_east_1.public_ip
      private = module.us_east_1.private_ips
    }
    ap_southeast_1 = {
      public  = module.ap_southeast_1.public_ip
      private = module.ap_southeast_1.private_ips
    }
    sa_east_1 = {
      public  = module.sa_east_1.public_ip
      private = module.sa_east_1.private_ips
    }
  }
}

output "root_node_ip" {
  value = local.root_ip
}

output "ssh_commands" {
  value = {
    for region, info in {
      eu_west_1      = { public_ip = module.eu_west_1.public_ip, private_ips = module.eu_west_1.private_ips }
      us_east_1      = { public_ip = module.us_east_1.public_ip, private_ips = module.us_east_1.private_ips }
      ap_southeast_1 = { public_ip = module.ap_southeast_1.public_ip, private_ips = module.ap_southeast_1.private_ips }
      sa_east_1      = { public_ip = module.sa_east_1.public_ip, private_ips = module.sa_east_1.private_ips }
    } : region => concat(
      ["ssh -o StrictHostKeyChecking=no ubuntu@${info.public_ip}"],
      [for ip in info.private_ips : "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o 'ProxyCommand=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@${info.public_ip}' ubuntu@${ip}"],
    )
  }
}

output "cluster_status_cmd" {
  value = "ssh -o StrictHostKeyChecking=no ubuntu@${local.root_ip} 'sudo pln status'"
}
