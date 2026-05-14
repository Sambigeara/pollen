# Infrastructure

Public, production-facing infrastructure for Pollen lives here. Experimental
clusters and CI verify tooling live in [`internal/dev/`](../internal/dev/).

```
infra/
  shared/           # zone-singleton CF state (ruleset + zone settings)
  prod/             # pln.sh production cluster (2 nodes + cloudflare DNS)
  staging/          # staging.pln.sh cluster (substrate iteration surface)
  justfile          # shared + prod + staging lifecycle (plan, apply, down)
```

## Terraform state is local

Both clusters and the shared module keep state on the operator's laptop. There
is no remote backend, no lock, no versioning, and no replication. Loss of the
laptop loses tracking of every cloud resource these modules manage.

This is a known TODO. Before any further bring-ups, migrate state to a remote
backend (S3 + DynamoDB or Terraform Cloud) so a lost laptop is recoverable.

Copy `.env.example` to `../.env` before running anything:

```bash
cp .env.example ../.env  # fill in tokens
```

`just` recipes load `../.env` automatically. Raw `terraform` commands invoked
outside `just` (e.g. the migration imports below) do not, so source it
explicitly first:

```bash
set -a; source ../.env; set +a   # exports CLOUDFLARE_API_TOKEN and HCLOUD_TOKEN
```

## Bring up the shared zone state

The `infra/shared/` module owns the Cloudflare resources that apply to the
whole zone: the `http_request_origin` ruleset (routing both prod and staging
hosts) and the global zone settings (SSL mode, min TLS, automatic HTTPS
rewrites). Cloudflare permits exactly one custom ruleset per phase per zone,
so these resources have to live in one place. Moving them out of any single
cluster keeps `prod-down` or `staging-down` from collaterally destroying state
the other cluster depends on.

```bash
cd infra
just shared-plan      # terraform plan → shared.tfplan
just shared-apply     # apply the saved plan (refuses if .tf sources newer)
```

`prod-plan`, `staging-plan`, and their `*-down` counterparts assert that
`shared/terraform.tfstate` exists and exposes a `zone_id` output before they
run, so the shared module must be applied at least once before either cluster
module is touched.

### First-time migration from the old shape

If your prod state still owns the ruleset, zone settings, and the staging
cert pack (i.e. you are on the commit before this restructure), the migration
moves those resources from prod state into shared state (and into staging
state for the cert pack) without destroying the underlying Cloudflare
objects. The `removed { ... lifecycle { destroy = false } }` blocks in
`prod/main.tf` make this safe; they require Terraform 1.7 or newer.

The order matters. Shared state has to exist and own the singletons before
prod re-plans, because `prod/main.tf` reads `terraform_remote_state.shared`
to resolve `zone_id`. Walk the runbook in order; do not skip ahead.

```bash
cd infra
set -a; source ../.env; set +a   # CF + Hetzner tokens for raw terraform commands

# 1. Collect the resource IDs the migration needs. The ruleset, zone
#    settings, and cert pack each carry an opaque CF id; the zone id is
#    shared across all of them. Run these from prod state before it
#    loses ownership of the resources.
cd prod
RULESET_ID=$(terraform state show cloudflare_ruleset.origin_port \
  | awk -F\" '/^    id /{print $2; exit}')
CERT_PACK_ID=$(terraform state show cloudflare_certificate_pack.staging_wildcard \
  | awk -F\" '/^    id /{print $2; exit}')
ZONE_ID=$(terraform state show cloudflare_ruleset.origin_port \
  | awk -F\" '/^    zone_id /{print $2; exit}')
echo "ZONE_ID=$ZONE_ID"
echo "RULESET_ID=$RULESET_ID"
echo "CERT_PACK_ID=$CERT_PACK_ID"

# 2. Initialise the shared module and import the singletons into it.
#    The CF provider v5 expects `zones/<ZONE_ID>/<RULESET_ID>` for
#    rulesets and `<ZONE_ID>/<SETTING_ID>` for zone settings.
cd ../shared
terraform init -input=false
terraform import cloudflare_ruleset.origin_port                    "zones/$ZONE_ID/$RULESET_ID"
terraform import cloudflare_zone_setting.always_use_https          "$ZONE_ID/always_use_https"
terraform import cloudflare_zone_setting.automatic_https_rewrites  "$ZONE_ID/automatic_https_rewrites"
terraform import cloudflare_zone_setting.min_tls_version           "$ZONE_ID/min_tls_version"
terraform import cloudflare_zone_setting.tls_1_3                   "$ZONE_ID/tls_1_3"

# 3. Apply the shared module. This persists outputs (zone_id, zone_name,
#    staging_subdomain) that prod and staging read via terraform_remote_state.
cd ..
just shared-plan
just shared-apply

# 4. Import the cert pack into staging state. Staging is the new owner
#    because the wildcard covers only the staging cluster's hostnames.
cd staging
terraform init -input=false
terraform import cloudflare_certificate_pack.staging_wildcard "$ZONE_ID/$CERT_PACK_ID"

# 5. Pre-flight before any `staging-plan`/`staging-apply`: confirm the
#    `pln-staging` SSH key is registered in Hetzner and that ACM is on
#    in the Cloudflare dashboard. See "Bring up staging" → Pre-flight
#    below for both checks. The staging variable defaults to a separate
#    key from prod so a compromised credential cannot cross clusters;
#    register it as a one-off before continuing.

# 6. Re-plan prod. With shared state in place and the `removed{}` blocks
#    still present, the only effect should be that prod state stops
#    tracking the migrated resources. Underlying CF objects stay put.
cd ..
just prod-plan         # expect: removed{} block effects only, no destroys, no creates
just prod-apply

# 7. Re-plan staging. Same expectation: no destroys, no creates.
just staging-plan
just staging-apply
```

Once the migration is verified and both clusters re-plan clean, delete the
`removed {}` blocks at the bottom of `infra/prod/main.tf`.

## Bring up prod

```bash
cd infra

just prod-plan            # terraform plan → prod.tfplan; review before applying
just prod-apply           # apply the saved plan (refuses if .tf sources newer)
just prod-bootstrap       # pln bootstrap ssh --admin: auto-install + join + config :8080
just prod-seed            # seed pln.sh apex + docs.pln.sh
```

No prod recipe carries `-auto-approve`; every state-changing step requires
the operator to acknowledge the plan or type a confirmation. The prod ctx is
an SSH-bridged admin handle, so no local prod daemon is required. Add the
ctx once with `pln ctx add prod root@<eu-ip>` after `prod-apply`.

Verify:

```bash
just prod-status                          # mesh view through the prod ctx
curl -I https://pln.sh/install.sh         # 200 via CF edge
curl -sI https://www.pln.sh/ | head -1    # 301 → pln.sh
open https://docs.pln.sh/                 # coming-soon page
```

## Updating content

Edit `web/apex/index.html` or `web/docs/index.html` (or the install script at
`scripts/install.sh`) and re-seed:

```bash
just prod-seed
```

`pln seed` on a directory is idempotent per name; it supersedes the existing
site with a fresh manifest.

## Tear down

```bash
just prod-down              # typed-confirmation; terraform destroy
```

`prod-down` requires the operator to type `destroy pln-prod` literally; the
default answer is "abort". `staging-down` and `shared-down` follow the same
pattern. `shared-down` additionally refuses to run while either cluster's
state still has resources, so the operator cannot accidentally tear down the
zone-singletons while a cluster still depends on them.

## Bring up staging

The staging cluster is the iteration surface for new substrate features (wire
mode, anonymous gateways, per-publisher static URLs). It mirrors prod's shape
on `staging.pln.sh`, opens the Pollen Cloud listeners, and shares the same
Cloudflare zone through the `infra/shared/` module. Prod and staging never
share Hetzner SSH keys, firewalls, or DNS records; a compromised staging
credential cannot reach prod.

Pre-flight:

1. Register a `pln-staging` SSH key in Hetzner. This is a one-off; the
   variable defaults to that name. Reusing the prod key means a single
   compromised credential can SSH into both clusters, so the default keeps
   the clusters isolated. Override with `-var ssh_key_name=pln-prod` on
   `staging-plan` if isolation is explicitly not wanted.
2. Enable Cloudflare Advanced Certificate Manager (ACM) on the `pln.sh`
   zone via the dashboard. ACM is billable. The
   `cloudflare_certificate_pack.staging_wildcard` resource fails creation
   without it, because Universal SSL only covers one level under the apex
   and the wildcard `*.staging.pln.sh` is two levels deep.

```bash
cd infra

just shared-plan && just shared-apply   # zone-singleton resources (one-off)
just staging-plan                       # terraform plan → staging.tfplan
just staging-apply                      # apply the saved plan
pln ctx add staging-admin root@$(cd staging && terraform output -json node_ips | jq -r .eu)
just staging-bootstrap                  # installs via install.sh + systemd unit
just staging-deploy-dev                 # overlay locally-built binary + Phase 7 listener config
just staging-status
```

Re-run `staging-deploy-dev` whenever you advance the local branch and want
staging to track it. The released binary that `bootstrap` lays down is only
there to provision the systemd unit and the `pln` system user; everything
substantive comes from the dev overlay. Each `staging-deploy-dev` keeps a
`pln.prev` copy on every node and rolls back automatically if the
post-restart health probe fails.

Wire-mode tenant flow from any machine:

```bash
pln ctx add staging pln://edge.staging.pln.sh:7443
pln ctx use staging
pln join <token>                                # mint token with `PLN_CONTEXT=staging-admin pln invite`
pln seed ./photo.png
pln share photo.png                             # prints https://blob.staging.pln.sh/<token>
```

### Tenant HTTPS and ACM

Tenant static-site HTTPS (`<name>-<short-pub>.staging.pln.sh`) needs the
Advanced Certificate Manager cert pack from the pre-flight step. Until ACM
is enabled the tenant wildcard answers grey-cloud over plain HTTP at the
origin:

```bash
curl -H 'Host: mysite-<pub>.staging.pln.sh' http://<edge-ip>:8080/
```

Once ACM is enabled on the CF dashboard, switch the wildcard to proxied:

```bash
just staging-plan && just staging-apply  # default is unproxied
# or, with the toggle flipped:
cd staging && terraform apply -var tenant_wildcard_proxied=true
```

`blob.staging.pln.sh` and `fn.staging.pln.sh` are one level deep and Universal
SSL covers them; they are proxied unconditionally.

### Wire-endpoint resolution

The `edge.staging.pln.sh` record stays grey-cloud and serves multi-A. CF can
not proxy custom-protocol mTLS-over-TCP, so resolvers pick essentially at
random, which trades worst-case round-trip latency for redundancy. The
choice is deliberate: cross-slot tombstone propagation (Phase 3f) means
every node accepts seeds and unseeds for any publisher, so the "wrong node"
problem doesn't bite correctness, only latency. CF Load Balancing would add
proximity steering but needs the paid Load Balancing subscription enabled on
the account, so anycast and proximity-steered routing are deferred until
tenant traffic warrants it.

### Tear down staging

```bash
just staging-down               # typed-confirmation; terraform destroy
```

## Hardening SSH access

By default both clusters allow SSH (`:22`) from `0.0.0.0/0`. Lock this down by
passing `-var 'ssh_source_ips=["X.X.X.X/32"]'` on plan, or commit a tfvars
file:

```hcl
# infra/prod/operator.auto.tfvars (gitignored: *.tfvars in .gitignore)
ssh_source_ips = ["198.51.100.42/32"]
```

`*.tfvars` is gitignored, so personal IPs do not leak through the repo.
