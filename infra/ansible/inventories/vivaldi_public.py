#!/usr/bin/env python3
"""Dynamic Ansible inventory for the all-public Vivaldi case.

Groups:
  root     - eu-west-1 public node 0
  public   - all public nodes except root
  all      - every node
  region_* - per-region public nodes
"""

import json
import os
import subprocess
import sys

TERRAFORM_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "vivaldi-public")


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
    ips = tf["public_ips_by_region"]["value"]
    root_ip = tf["root_node_ip"]["value"]
    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

    inventory = {
        "_meta": {"hostvars": {}},
        "all": {"children": ["root", "public"]},
        "root": {"hosts": []},
        "public": {"hosts": []},
    }

    for region, region_ips in ips.items():
        region_group = f"region_{region}"
        inventory["all"]["children"].append(region_group)
        inventory[region_group] = {"hosts": []}

        for idx, public_ip in enumerate(region_ips):
            host_vars = {
                "ansible_user": "ubuntu",
                "ansible_ssh_common_args": ssh_args,
                "region": region.replace("_", "-"),
                "node_role": "public",
                "region_index": idx,
            }
            inventory[region_group]["hosts"].append(public_ip)
            if public_ip == root_ip:
                inventory["root"]["hosts"].append(public_ip)
                host_vars["node_role"] = "root"
            else:
                inventory["public"]["hosts"].append(public_ip)
            inventory["_meta"]["hostvars"][public_ip] = host_vars

    return inventory


if __name__ == "__main__":
    if "--list" in sys.argv:
        print(json.dumps(build_inventory(), indent=2))
    elif "--host" in sys.argv:
        print(json.dumps({}))
    else:
        print(json.dumps(build_inventory(), indent=2))
