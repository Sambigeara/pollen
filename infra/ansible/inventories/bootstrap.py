#!/usr/bin/env python3
"""Dynamic Ansible inventory from bootstrap Terraform output.

Single host with direct SSH access (no bastion).
"""

import json
import os
import subprocess
import sys

TERRAFORM_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "bootstrap")


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
    ip = tf["ip"]["value"]
    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

    return {
        "_meta": {
            "hostvars": {
                ip: {
                    "ansible_user": "ubuntu",
                    "ansible_ssh_common_args": ssh_args,
                }
            }
        },
        "all": {"hosts": [ip]},
    }


if __name__ == "__main__":
    if "--list" in sys.argv:
        print(json.dumps(build_inventory(), indent=2))
    elif "--host" in sys.argv:
        print(json.dumps({}))
    else:
        print(json.dumps(build_inventory(), indent=2))
