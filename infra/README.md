# Infrastructure

Public, production-facing infrastructure for Pollen lives here. Experimental
clusters and CI verify tooling live in [`internal/dev/`](../internal/dev/).

```
infra/
  prod/             # pln.sh production cluster (2 nodes + cloudflare DNS/rules)
  prod/laptop/      # launchd plist for the laptop-side prod pln daemon
  justfile          # prod lifecycle (laptop-install, up, bootstrap, seed, down)
```

## Bring up prod

```bash
cd infra

just prod-laptop-install  # launchd: start prod pln on laptop (PLN_DIR=~/.pln-prod, :60711)
just prod-up              # terraform: 2× cpx21 + cloudflare DNS + rules
just prod-bootstrap       # pln bootstrap ssh --admin: auto-install + join + config :8080
just prod-seed            # seed pln.sh apex + docs.pln.sh
```

Verify:

```bash
just prod-status                          # mesh view from the laptop prod daemon
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

`pln seed` (on a directory) is idempotent per name: it supersedes the existing site with
a fresh manifest.

## Tear down

```bash
just prod-down              # terraform destroy — removes nodes and DNS records
just prod-laptop-uninstall  # stop + remove the launchd agent (state preserved)
```
