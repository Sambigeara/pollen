# Infrastructure

Terraform provisions instances. Ansible installs and manages `pln`.

```
infra/
  bootstrap/       # Single public node in eu-west-2
  vivaldi/         # 28-node mixed cluster (4 regions × 7 nodes, 1 public + 6 private per region)
  vivaldi-public/  # 28-node all-public cluster (4 regions × 7 public nodes)
  ansible/         # Shared playbooks, per-environment inventories
```

## Bootstrap (single node)

```bash
# Provision
cd infra/bootstrap
terraform init && terraform apply

# Install pln
cd ../ansible
ansible-playbook install.yml -i inventories/bootstrap.py -e pln_version=v0.1.0-alpha.36

# Upgrade pln (no teardown)
ansible-playbook upgrade.yml -i inventories/bootstrap.py -e pln_version=v0.1.0-alpha.37
```

## Vivaldi (28-node cluster)

```bash
cd infra

# Provision, install, and form cluster (idempotent)
just deploy-vivaldi v0.1.0-alpha.36

# Tear down all infrastructure
just destroy-vivaldi
```

## Vivaldi Public (28-node all-public cluster)

```bash
cd infra

# Provision, install, and form cluster (idempotent)
just deploy-vivaldi-pub v0.1.0-alpha.65

# Tear down all infrastructure
just destroy-vivaldi-pub
```

## Prerequisites

- [just](https://github.com/casey/just) (`brew install just`)
- Terraform / OpenTofu
- Ansible (`brew install ansible`)
- AWS credentials configured
- SSH key at `~/.ssh/id_ed25519`
