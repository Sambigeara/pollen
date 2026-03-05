terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}

locals {
  ssh_public_key = file(var.ssh_public_key_path)
  ssh_opts       = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

  # Flat list of all IPs for the wait step (count is known at plan time).
  all_ips = concat(
    module.eu_west_1.public_ips,
    module.us_east_1.public_ips,
    module.ap_southeast_1.public_ips,
    module.sa_east_1.public_ips,
  )

  # Root node is the first node in eu-west-1.
  root_ip = module.eu_west_1.public_ips[0]

  # Build a flat list of all IPs with metadata for the join script.
  # Format: "ip:is_relay" where is_relay=1 for the first node of each non-root region.
  all_join_nodes = concat(
    # eu-west-1: skip index 0 (root), rest are regular nodes
    [for i, ip in slice(module.eu_west_1.public_ips, 1, length(module.eu_west_1.public_ips)) : "${ip}:0"],
    # Other regions: index 0 is relay, rest are regular
    [for i, ip in module.us_east_1.public_ips : "${ip}:${i == 0 ? 1 : 0}"],
    [for i, ip in module.ap_southeast_1.public_ips : "${ip}:${i == 0 ? 1 : 0}"],
    [for i, ip in module.sa_east_1.public_ips : "${ip}:${i == 0 ? 1 : 0}"],
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
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "eu_west_1" {
  source    = "./modules/region"
  providers = { aws = aws.eu_west_1 }

  region_name      = "eu-west-1"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "ap_southeast_1" {
  source    = "./modules/region"
  providers = { aws = aws.ap_southeast_1 }

  region_name      = "ap-southeast-1"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

module "sa_east_1" {
  source    = "./modules/region"
  providers = { aws = aws.sa_east_1 }

  region_name      = "sa-east-1"
  nodes_per_region = var.nodes_per_region
  instance_type    = var.instance_type
  ssh_public_key   = local.ssh_public_key
  pln_version      = var.pln_version
}

# --- Step 1: Wait for cloud-init to finish on all nodes ---

resource "null_resource" "wait_for_install" {
  triggers = {
    ips = join(",", local.all_ips)
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-SCRIPT
      set -euo pipefail
      SSH_OPTS="${local.ssh_opts}"
      PIDS=()
      for IP in ${join(" ", local.all_ips)}; do
        (
          for i in $(seq 1 30); do
            if ssh $SSH_OPTS -o ConnectTimeout=5 ubuntu@$IP "which pln" &>/dev/null; then
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

# --- Step 2: Init root node (first node of eu-west-1) ---

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

      # Phase A: generate invites sequentially on root (fast local signing)
      declare -a TOKENS
      for entry in $NODES; do
        TOKEN=$(ssh $SSH_OPTS ubuntu@$ROOT "sudo pln invite --ttl 10m" 2>/dev/null)
        TOKENS+=("$TOKEN")
      done

      # Phase B: join all nodes in parallel
      PIDS=()
      IDX=0
      for entry in $NODES; do
        IP=$(echo "$entry" | cut -d: -f1)
        IS_RELAY=$(echo "$entry" | cut -d: -f2)
        TOKEN="$${TOKENS[$IDX]}"

        if [ "$IS_RELAY" = "1" ]; then
          ssh $SSH_OPTS ubuntu@$IP "sudo pln join --public '$TOKEN'" &
        else
          ssh $SSH_OPTS ubuntu@$IP "sudo pln join '$TOKEN'" &
        fi
        PIDS+=($!)
        IDX=$((IDX + 1))
      done

      # Wait for all joins
      FAILED=0
      for pid in "$${PIDS[@]}"; do
        if ! wait $pid; then
          FAILED=$((FAILED + 1))
        fi
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
    us_east_1      = module.us_east_1.public_ips
    eu_west_1      = module.eu_west_1.public_ips
    ap_southeast_1 = module.ap_southeast_1.public_ips
    sa_east_1      = module.sa_east_1.public_ips
  }
}

output "root_node_ip" {
  value = local.root_ip
}

output "ssh_commands" {
  value = {
    for region, ips in {
      us_east_1      = module.us_east_1.public_ips
      eu_west_1      = module.eu_west_1.public_ips
      ap_southeast_1 = module.ap_southeast_1.public_ips
      sa_east_1      = module.sa_east_1.public_ips
    } : region => [for ip in ips : "ssh -o StrictHostKeyChecking=no ubuntu@${ip}"]
  }
}

output "cluster_status_cmd" {
  value = "ssh -o StrictHostKeyChecking=no ubuntu@${local.root_ip} 'sudo pln status'"
}
