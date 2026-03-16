#!/usr/bin/env python3
"""Dynamic Ansible inventory for the Hetzner test cluster.

Groups:
  root   - nbg1 node (cluster root)
  public - fsn1 and ash nodes
  all    - every node
"""

import json
import os
import subprocess
import sys

TERRAFORM_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "hetzner")


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
    node_ips = tf["node_ips"]["value"]
    root_ip = tf["root_node_ip"]["value"]
    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

    inventory = {
        "_meta": {"hostvars": {}},
        "all": {"children": ["root", "public"]},
        "root": {"hosts": []},
        "public": {"hosts": []},
    }

    for location, ip in node_ips.items():
        host_vars = {
            "ansible_user": "root",
            "ansible_ssh_common_args": ssh_args,
            "location": location,
            "node_role": "public",
        }
        if ip == root_ip:
            inventory["root"]["hosts"].append(ip)
            host_vars["node_role"] = "root"
        else:
            inventory["public"]["hosts"].append(ip)
        inventory["_meta"]["hostvars"][ip] = host_vars

    return inventory


if __name__ == "__main__":
    if "--list" in sys.argv:
        print(json.dumps(build_inventory(), indent=2))
    elif "--host" in sys.argv:
        print(json.dumps({}))
    else:
        print(json.dumps(build_inventory(), indent=2))
