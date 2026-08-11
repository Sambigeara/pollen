# Compute as a layer, not a system

Musings on orchestrating workloads over a peer-to-peer mesh of wildly
unequal machines. Not a design doc, not a requirements doc. A set of
positions I hold, why I hold them, and where I think they stop being
true.

Most of what follows is drawn from decisions I made building
[Pollen](https://pln.sh): a self-organising mesh and WASM runtime with
no scheduler, no leader, and no control plane. Some of those decisions
held up better than I expected. A few of them will not survive contact
with this problem, and I have tried to be honest about which.

---

## 0. Two framing corrections

**The devices are heterogeneous, and that is the entire point.** If they
were homogeneous this would be a solved problem with a dozen mature
answers. The interesting version is the one where a Mac Studio, a CI
box, a four-year-old laptop with 8% battery, and a phone in someone's
pocket are all candidate hosts for the same unit of work. Every design
decision below is downstream of that.

**"Compute" is not a system to build. It is a contract to honour.** The
deliverable is a layer you address as though it were one machine. The
mesh, the VM, the storage, and the identity model are all *below* it,
and the value of the whole thing is proportional to how little of them
leaks through.

---

## 1. Write the API before anything else

The one artifact that should not change is the smallest one:

```
call(name, input) -> output
```

No host. No port. No replica count. No region. No "which node do I ask".
If a location can leak into an identifier, you do not have a single
machine — you have a directory service with extra steps.

In Pollen this is the URI scheme, and the scheme *is* the abstraction
boundary:

```
pln://seed/<name>/<function>
pln://service/<name>
```

That is the whole addressing surface. A workload calling another
workload writes the same string whether the callee is in-process, on the
laptop next to it, or three relay hops away in another continent. It
cannot express a preference about that, which is exactly the property
you want, because the moment it *can*, someone will, and then you own
their placement decisions forever.

Test for whether you have the layer: can you write down its entire API
on an index card, and does anything on that card mention a machine?

---

## 2. The layers, and what each owes upward

The useful discipline is not "draw boxes" — it is to state, in one
sentence, the question each layer answers, and then guarantee that the
layer above never learns *how* it was answered.

| Layer | Question it answers | What the layer above must never learn |
|---|---|---|
| Identity & authority | May this principal do this thing? | How trust is rooted or renewed |
| Membership & transport | Give me an authenticated stream to peer X | NAT, relays, punching, connection reuse |
| Truth | What does the world look like right now? | How state propagates, or from whom |
| Content | Give me the bytes for this hash | Which peer served them |
| Locality | Rank these peers by distance from me | RTT, coordinates, path costs |
| **Compute** | **Run this, return the answer** | **Everything above** |
| Execution (the VM) | Run these bytes with this input and this budget | Where the bytes came from, who is calling |

Two things I would fight for in that table.

**Locality is a layer, not a field.** Compute asks "rank these peers"
and gets an ordering. It never learns that the answer came from Vivaldi
coordinates, or that a relay detour was priced in, or that ties broke
deterministically to keep a remote cache warm. In Pollen this is a
~120-line package with one interface (`Cost(dest) (float64, bool)`) and
a fallback to straight-line coordinate distance when the router has no
path. Blob fetch and workload invocation share it, so "nearest" means
one thing system-wide. Cheap to build, and it means the day someone
wants to weight by battery or egress cost, exactly one file changes.

**The VM belongs *below* compute, and it is not the interesting part.**
If the orchestration layer knows it is WASM, two layers have fused. The
contract I would hold the runtime team to is roughly:

```
Execute(artifact, input, budget) -> (output, usage)
```

`budget` is memory, wall-clock, and whatever else the device wants to
cap. `usage` is what it actually consumed, because that is the signal
placement runs on. Nothing about modules, instances, linking, or
compilation. If that interface holds, you can build and test the entire
compute layer against a fake executor that sleeps for 50ms — and you
should, long before the real one is ready.

---

## 3. Six decisions I would make again

### 3.1 Everything is a signed fact, and there is one way in

In Pollen every mutation — a peer joining, a workload being published, a
node claiming a replica, a credential being revoked, a resource being
retired — is the same shape: a signed assertion, merged into one
converging document. Every one of them passes the same four stages:

1. **authenticate** — the signature is real, the body matches its hash,
   the signer's authority chains to a root, is in-date, is not revoked
2. **authorise** — that authority actually carries this capability, and
   the signer satisfies its own policy
3. **account** — the signer's budget has room for one more
4. **admit** — merge and propagate

The decision worth stealing is not the four stages. It is that **locally
originated writes take exactly the same path as gossiped ones.** There
is no fast path for "but I made this one myself". Every class of bug
where a node trusts itself more than it trusts its peers is deleted by
construction, and the pipeline is testable as a pure function over a
fact and a state snapshot.

### 3.2 Ownership is derived, not handed off

This is the one I would argue hardest for, because it dissolves a whole
category of questions rather than answering them.

The instinct with jobs is to model a lifecycle with an owner at each
stage, and then design the handoff protocol: leases, heartbeats,
fencing tokens, takeover, split-brain resolution. That protocol is
where distributed systems go to die.

The alternative: **ownership is a pure function of a converged view.**
Nobody is assigned anything. Every node independently evaluates the same
function over the same snapshot and decides, locally, whether *it* is
the one that should be running a thing. Placement is the fixed point of
that function, not a decision anyone made.

In Pollen the core of it is genuinely small — a function taking specs,
claims, and the peer set, returning `claim` or `release` actions for the
local node. No I/O, no clock, no network. Everything else in the
placement package is plumbing around that.

Now re-ask the lifecycle questions in this frame:

- *Who owns each stage?* Nobody. Ownership is `argmin` over a view.
- *How do you agree on ownership?* You do not agree. You compute. Same
  view, same function, same answer.
- *How does handoff work?* There is no handoff. There is **overlap**: a
  node that is relinquishing marks itself draining and keeps serving
  while no longer counting toward the replica target, so the new
  claimant comes up before the old one goes down. Make-before-break,
  with no coordination between the two parties.
- *What when the owner goes offline?* Its claim goes stale in the shared
  view. The function's fixed point moves. Some other node's next local
  evaluation returns `claim`. Nothing was detected, nothing was elected,
  nothing failed over.
- *What during a partition?* Both sides evaluate against their own view.
  Both keep serving. On rejoin the document converges and the fixed
  point settles. **Read the honest caveat in §5 before you like this too
  much.**

### 3.3 Determinism is a coordination mechanism

If every node runs the same function over the same input, you get
agreement for free — but only if the function is *actually*
deterministic, including its tie-breaks. It is worth being obsessive
about this, because it is where the free lunch is:

- Electing a node to take on a replica: lexicographically smallest peer
  key among eligible non-replicas. No messages. Every node computes the
  same winner, and only the winner acts on it.
- Deciding who relinquishes when over-provisioned: same rule, over the
  current replica set.
- Ranking candidate hosts: distance first, peer key as tie-break — so
  repeated fetches keep hitting the same warm copy instead of scattering
  across equidistant peers.

You still need randomness in exactly one place — call routing, where you
want power-of-two-choices to avoid herding every caller onto the same
"best" replica. The rule I would write down: **deterministic where the
decision is shared, random where the decision is per-call.**

### 3.4 Load is data, not an exception

The converged document in Pollen carries, per node, things like resource
telemetry, a backoff TTL, per-workload call counts, and a traffic
heatmap. That means saturation is something you *read*, not something
you discover by getting a 503.

Two consequences worth having:

- Scaling decisions are made from gossiped saturation
  (`|replicas in backoff| / |replicas|`), so growth happens at the node
  seeing the heaviest unserved demand rather than wherever the scheduler
  felt like.
- Overload is a *routing* signal, not a user-facing failure. A node
  refusing a call because admission would breach its budget returns a
  distinguishable, node-wide, retryable error, and the caller falls
  through to the next replica. The user never sees it.

For borrowed consumer devices this matters more than it does in a
datacentre, because the capacity signals are weird and fast-moving:
battery, thermal state, whether the screen is on, whether the owner just
started a build. All of those are just fields in the same telemetry
attribute, and they should be — inventing a separate "device health"
subsystem for them is how you end up with two sources of truth about
capacity.

### 3.5 Authority and location are orthogonal

Who may publish a thing, and where its bytes physically live, are
different questions and should never be entangled. Content addressing
gives you this almost for free: receivers verify the hash on arrival, so
you can pull from any holder without trusting the holder. Trust the
bytes, not the source. Deduplication and one-shot transfer fall out.

The corollary I like more: **published work should reference its
publisher's authority rather than embed a copy of it.** In Pollen a
published resource names its publisher; the publisher's grant is
resolved from converged state at check time. So work keeps running after
its publisher goes offline or leaves entirely, and revoking a publisher
invalidates everything it published, cluster-wide, on the next check,
with no per-resource cleanup pass. Embedding a credential snapshot into
each artifact gets you the opposite of both properties.

### 3.6 Liveness must be provable offline

Every node in Pollen proves it is alive by minting a short-lived session
for itself: it signs over the credential already on its disk with its
own key. No round-trip. No issuer. No root. A node keeps authenticating
to peers while the machine that admitted it, and the machine that
founded the cluster, are both switched off.

The long-lived credential carries a hard deadline — that deadline *is*
the "how long does a stolen key stay usable" bound, and it is the only
thing that renewal touches. Renewal goes to any reachable peer holding
delegation authority, not to a specific issuer, and the renewing peer
re-clamps to its own chain so renewal can never widen authority.

For a network of employee laptops that are asleep, tethered, or on a
plane most of the time, "can this device prove it is allowed to work
without reaching anything" is not a nice-to-have. It is the difference
between a mesh and a fleet with a phone-home requirement.

---

## 4. Heterogeneity is a property, not a taxonomy

The temptation is a device-class enum: `phone | laptop | workstation |
server`. Resist it. Every one of those is a bundle of assumptions that
will be wrong for someone, and the enum ends up load-bearing in fifteen
places.

The version that scales: bake arbitrary key/value properties into a
node's identity at admission time, let a published workload declare
policy clauses over those properties, and have placement filter
candidates through an eligibility predicate before it elects anyone.

Then "only run this on machines with 32GB and a GPU", "only run this on
company-owned hardware", "only run this in the EU", and "never run this
on a device on battery" are all the same mechanism, and the compute
layer contains zero knowledge of what a phone is.

One subtlety worth stealing from a bug I hit: the eligibility filter has
to be applied *inside* the election, not after it. Elect first and
filter later, and the election keeps picking the same ineligible peer
forever while the replica count sits pinned at its floor and nothing
ever gets a chance to claim.

---

## 5. Where these answers stop

If I only wrote the sections above, I would be doing the thing I am
arguing against — bringing clay I already have to a table that has not
decided what it is making. The honest list of where Pollen's model does
not obviously transfer:

**Derived ownership buys at-least-once, and nothing stronger.** Both
sides of a partition keeping their workloads running is only a feature
because Pollen's unit of work is a stateless request/response call.
Re-derivation is safe precisely because running it twice is fine. If the
job model here needs exactly-once, or a single writer, or ordered side
effects, then derived ownership is the *wrong* mechanism for those jobs
and you need a real consensus primitive at that specific point. My
strong preference: name that boundary explicitly and keep it small,
rather than smearing weak consistency across everything and hoping. The
worst outcome is a system that is *nearly* strongly consistent.

**Jobs are not calls.** A stateless call can be re-derived. A
long-running job with intermediate state can only be *resumed*. The
question I would put at the front of the R&D queue: what is the smallest
amount of state a job must externalise for its ownership to remain
re-derivable, and what does that cost? That is a genuine research
question with a real trade curve (checkpoint frequency against redundant
work on churn), and the answer shapes everything above it.

**Full state convergence has a ceiling.** One document that every node
holds is a wonderful simplification, and it works well at cluster scale
with modest churn. A nation state of laptops, joining and leaving all
day, is a different regime. I do not know where the knee is, and I would
want to *measure* it before choosing a data structure. The likely
answers — interest-scoped subscriptions so a node only converges on what
it participates in, or a two-tier membership with a stable spine and
ephemeral leaves — both preserve the programming model above while
changing the mechanics below. Which is the point of having the layer.

**The device owner is a principal, and Pollen has no concept of them.**
Pollen assumes an admitted cluster under one root, where node operators
and cluster operators are the same people. Borrowed employee hardware is
BYOD-shaped: the person whose laptop it is needs a veto, a resource cap,
and visibility into what ran. Delegated capabilities get you most of the
machinery, but "the device owner can revoke, throttle, or evict without
being an admin" is a genuinely new principal type, and I would rather
design it in now than bolt consent on later.

**Content addressing proves the bytes, not the answer.** You can verify
that a peer served the artifact you asked for. You cannot verify that it
*executed* it honestly. Inside a trusted fleet that is fine and I would
not spend a day on it. If the trust boundary ever widens, verified
execution — redundant execution, spot-checking, attestation — is a large
open area that materially changes the compute layer's shape. Worth
deciding early whether it is in scope, because retrofitting it is
brutal.

**Coordinate spaces behave differently out here.** Vivaldi is cheap and
good enough over reasonably-behaved networks, and I had to add a
low-RTT floor before error estimates would settle on LAN links at all.
Consumer devices behind carrier-grade NAT, on flaky wifi, with
asymmetric paths, are a harsher environment than the one it was
evaluated in. Worth an early experiment, because "nearest" is load
bearing in both placement and routing.

---

## 6. How I would sequence the work

Deliberately backwards from how this usually goes.

1. **Write the compute API on an index card.** It is the only artifact
   that should not change. Everything else is negotiable.

2. **Build a churn simulator before you build anything real.** Nodes,
   connections, state propagation, and *no actual work being done*.
   Then ask it the questions that matter: how long to converge after a
   40% node loss; what happens to placement during a partition and on
   rejoin; how much state moves when a node joins; whether the placement
   function has stable fixed points or oscillates when three nodes
   disagree for 200ms. This is cheap, it is the fastest way to turn
   "explore what's possible" into requirements, and it is *far* easier
   to do before there is a codebase with opinions.

3. **Only then pick data structures.** Whatever survives step 2 has
   earned its place. Whatever we already have on the table gets to
   compete on the same footing as anything else — and some of it will
   win, just not by default and not under its current name.

4. **Fake the VM the whole way through.** `Execute` that sleeps and
   returns its input. Every orchestration property worth having can be
   demonstrated without a real runtime existing, and the day it does
   exist, it should be a drop-in.

5. **Pick the demo that proves it now.** Mine: put the cluster under
   sustained load, unplug the fastest machine, and have the graph show
   nothing happening. No spike, no errors, no operator action. If the
   only way to notice a machine died is to look at where the work went,
   the abstraction is real.

---

## 7. The thing I would guard against

Existing tools do not just give you implementations. They give you
*seams* — and seams are the hardest thing to un-choose later.

Reach for a database and placement becomes a query, which means someone
has to be authoritative, which means you have a control plane whether
you wanted one or not. Reach for a scheduler-shaped system and partition
becomes an outage, because the shape assumes the control plane is
reachable. Neither of those is a bad tool. They are tools whose seams
encode a set of answers to questions we have not asked yet.

The order that works: decide what should stay true while nodes and
connections churn, write down the flows that keep it true, and *then*
work backwards to the structures and algorithms that implement those
flows. Most of the R&D we have already done survives that process. It
just may not survive it under its current name, and that is fine — that
is what it means for the R&D to have been worth doing.

The prize is worth the discipline. There is more latent compute sitting
idle on desks and in pockets than most companies will ever rent, and
nobody has an abstraction that makes it usable as one machine. That is
not a faster horse. It does not exist yet, which is precisely why nobody
can tell us what to build.
