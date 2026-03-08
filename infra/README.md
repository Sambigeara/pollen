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
# Provision infrastructure
cd infra/vivaldi
terraform init && terraform apply

# Full setup: install → init root → join all nodes
cd ../ansible
ansible-playbook site.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.36

# Upgrade pln across all 28 nodes (no teardown!)
ansible-playbook upgrade.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.37
```

## Vivaldi Public (28-node all-public cluster)

```bash
# Provision infrastructure
cd infra/vivaldi-public
terraform init && terraform apply

# Install only
cd ../ansible
ansible-playbook install.yml -i inventories/vivaldi_public.py -e pln_version=v0.1.0-alpha.65

# Init root node only
ansible-playbook init-cluster.yml -i inventories/vivaldi_public.py

# Join remaining public nodes only
ansible-playbook join-public-cluster.yml -i inventories/vivaldi_public.py

# Collect status/log artifacts
ansible-playbook collect-topology-artifacts.yml -i inventories/vivaldi_public.py -e artifact_dir=/tmp/pollen-vivaldi-public
```

### Individual steps

```bash
# Install only (no cluster formation)
ansible-playbook install.yml -i inventories/vivaldi.py -e pln_version=v0.1.0-alpha.36

# Init root node only
ansible-playbook init-cluster.yml -i inventories/vivaldi.py

# Join remaining nodes only
ansible-playbook join-cluster.yml -i inventories/vivaldi.py

# Purge all state and start fresh
ansible-playbook purge.yml -i inventories/vivaldi.py
ansible-playbook init-cluster.yml -i inventories/vivaldi.py
ansible-playbook join-cluster.yml -i inventories/vivaldi.py
```

## Prerequisites

- Terraform / OpenTofu
- Ansible (`brew install ansible`)
- AWS credentials configured
- SSH key at `~/.ssh/id_ed25519`
