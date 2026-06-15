# Infrastructure

Public, production-facing infrastructure for Pollen lives here. Experimental
clusters and CI verify tooling live in [`internal/dev/`](../internal/dev/).

```
infra/
  prod/             # pln.sh cluster + zone-singleton CF state (ruleset, settings, cert)
  staging/          # staging.pln.sh cluster (substrate iteration surface)
  justfile          # prod + staging lifecycle (plan, apply, down)
```

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
default answer is "abort". `staging-down` follows the same pattern. The
zone-singleton ruleset, zone settings, and staging cert pack live in prod
state, so `prod-down` removes them too; staging's HTTP routing and HTTPS
depend on them, so decommission staging alongside prod when taking the whole
zone down.

## Bring up staging

The staging cluster is the iteration surface for new substrate features (wire
mode, anonymous gateways, per-publisher static URLs). It mirrors prod's shape
on `staging.pln.sh` and opens the Pollen Cloud listeners. The zone-singleton
ruleset and the staging wildcard cert live in prod state (Cloudflare permits
one ruleset per `http_request_origin` phase per zone). Prod and staging never
share Hetzner SSH keys, firewalls, or DNS records; a compromised staging
credential cannot reach prod.

Pre-flight:

1. Register a `pln-staging` SSH key in Hetzner. This is a one-off; the
   variable defaults to that name. Override with `-var ssh_key_name=pln-prod`
   on `staging-plan` to share the prod key instead.
2. Enable Cloudflare Advanced Certificate Manager (ACM) on the `pln.sh`
   zone via the dashboard. ACM is billable. The
   `cloudflare_certificate_pack.staging_wildcard` resource fails creation
   without it, because Universal SSL only covers one level under the apex
   and the wildcard `*.staging.pln.sh` is two levels deep.

```bash
cd infra

just staging-plan                       # terraform plan → staging.tfplan
just staging-apply                      # apply the saved plan
pln ctx add staging-root root@$(cd staging && terraform output -json node_ips | jq -r .eu)
just staging-bootstrap                  # installs via install.sh + systemd unit
just staging-deploy-dev                 # overlay locally-built binary + listener config
just staging-status
```

Re-run `staging-deploy-dev` whenever you advance the local branch and want
staging to track it. The released binary that `bootstrap` lays down only
provisions the systemd unit and the `pln` system user; the dev overlay
supplies the running binary. Each `staging-deploy-dev` keeps a
`pln.prev` copy on every node and rolls back automatically if the
post-restart health probe fails.

Wire-mode tenant flow from any machine:

```bash
pln ctx add staging pln://edge.staging.pln.sh:7443
pln ctx use staging
pln join <token>                                # mint token with `PLN_CONTEXT=staging-root pln invite`
pln seed ./photo.png
pln share photo.png                             # prints https://blob.staging.pln.sh/<token>
```

### Tenant HTTPS and ACM

Tenant static-site HTTPS (`<name>-<short-pub>.staging.pln.sh`) needs the
Advanced Certificate Manager cert pack, which is enabled on the zone. The
tenant wildcard is proxied by default, so these sites are served over HTTPS
through Cloudflare:

```bash
curl -I https://mysite-<pub>.staging.pln.sh/
```

For a fresh bring-up on a zone without ACM, keep the wildcard grey-cloud so it
still answers over plain HTTP at the origin:

```bash
cd staging && terraform apply -var tenant_wildcard_proxied=false
```

`blob.staging.pln.sh` and `fn.staging.pln.sh` are one level deep and Universal
SSL covers them; they are proxied unconditionally.

### Wire-endpoint resolution

The `edge.staging.pln.sh` record stays grey-cloud and serves multi-A:
Cloudflare cannot proxy custom-protocol mTLS-over-TCP, so resolvers pick a
node at random. Cross-slot tombstone propagation means every node accepts
seeds and unseeds for any publisher, so a random pick costs round-trip latency
without affecting correctness. CF Load Balancing would add proximity steering
but needs the paid Load Balancing subscription enabled on the account.

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
