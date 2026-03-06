#!/usr/bin/env python3
"""Dynamic Ansible inventory from vivaldi Terraform output.

Groups:
  root     – eu-west-1 public node (cluster root)
  relays   – public nodes in all other regions
  private  – all private nodes (SSH via bastion)
  all      – every node
"""

import json
import os
import subprocess
import sys

TERRAFORM_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "vivaldi")


def terraform_output():
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=TERRAFORM_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def build_inventory():
    tf = terraform_output()
    ips = tf["ips_by_region"]["value"]
    root_ip = tf["root_node_ip"]["value"]
    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

    inventory = {
        "_meta": {"hostvars": {}},
        "all": {"children": ["root", "relays", "private"]},
        "root": {"hosts": []},
        "relays": {"hosts": []},
        "private": {"hosts": []},
    }

    for region, info in ips.items():
        public_ip = info["public"]
        private_ips = info["private"]
        region_name = region.replace("_", "-")

        # Public node
        host_vars = {
            "ansible_user": "ubuntu",
            "ansible_ssh_common_args": ssh_args,
            "region": region_name,
        }
        if public_ip == root_ip:
            inventory["root"]["hosts"].append(public_ip)
            host_vars["node_role"] = "root"
        else:
            inventory["relays"]["hosts"].append(public_ip)
            host_vars["node_role"] = "relay"
        inventory["_meta"]["hostvars"][public_ip] = host_vars

        # Private nodes — SSH via the region's public node as bastion
        proxy_args = (
            f"{ssh_args} -o ProxyCommand="
            f"\"ssh {ssh_args} -W %h:%p ubuntu@{public_ip}\""
        )
        for pip in private_ips:
            inventory["private"]["hosts"].append(pip)
            inventory["_meta"]["hostvars"][pip] = {
                "ansible_user": "ubuntu",
                "ansible_ssh_common_args": proxy_args,
                "region": region_name,
                "node_role": "private",
                "bastion_ip": public_ip,
            }

    return inventory


if __name__ == "__main__":
    if "--list" in sys.argv:
        print(json.dumps(build_inventory(), indent=2))
    elif "--host" in sys.argv:
        # Per-host vars already in _meta
        print(json.dumps({}))
    else:
        print(json.dumps(build_inventory(), indent=2))
