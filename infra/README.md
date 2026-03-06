# Infrastructure

Terraform provisions instances. Ansible installs and manages `pln`.

```
infra/
  bootstrap/       # Single public node in eu-west-2
  vivaldi/         # 28-node geo-distributed cluster (4 regions × 7 nodes)
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
# Provision infrastructure
cd infra/vivaldi
terraform init && terraform apply

# Full setup: install → init root → join all nodes
cd ../ansible
ansible-playbook site.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.36

# Upgrade pln across all 28 nodes (no teardown!)
ansible-playbook upgrade.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.37
```

### Individual steps

```bash
# Install only (no cluster formation)
ansible-playbook install.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.36

# Init root node only
ansible-playbook init-cluster.yml -i inventories/vivaldi.py

# Join remaining nodes only
ansible-playbook join-cluster.yml -i inventories/vivaldi.py
```

## Prerequisites

- Terraform / OpenTofu
- Ansible (`brew install ansible`)
- AWS credentials configured
- SSH key at `~/.ssh/id_ed25519`
