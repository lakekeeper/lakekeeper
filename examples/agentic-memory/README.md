# Governed agent memory and skills

**Thesis:** an agent's memory and its skills are data, and Lakekeeper governs them like
any other data — at the **credential-vending layer**. A denied agent receives no storage
credentials, so it *physically cannot* read the objects. And the one thing no agent
framework can promise: **an agent cannot approve its own skill**, because it holds keys
that cannot write the approved library.

Two agents keep private memory in one catalog, propose skills they are unable to publish,
and a human reviewer promotes one. Nothing in the agent code enforces any of it.

```
  agent_memory/
    ├── agent_a/        entries (dataset) + recall (lance)   agent-a: read+write
    ├── agent_b/        entries + recall                     agent-b: read+write
    └── shared/         entries + recall                     both: read only
  skills/
    ├── proposed/
    │     ├── <agent-a>   agent-a writes its own queue, and sees no other
    │     └── <agent-b>
    ├── approved          agents READ · only the reviewer WRITES   ← the wall
    ├── rejected          refusals with reasons, readable by the proposer
    └── decisions         ICEBERG · one row per approval or rejection
  governance/
    └── policy            the triage rules · governance agent reads, cannot write
```

## What it demonstrates

| Beat | What the catalog does |
|---|---|
| An agent reads and writes its own memory | vends scoped STS credentials for that namespace |
| agent-b asks for agent-a's memory | **404** — it will not admit a table you may not see exists |
| An agent searches every scope it knows | denied scopes vend nothing and drop out of the results |
| An agent writes the shared tier | **denied** — it holds `select` there, not `modify` |
| An agent approves its own skill | **denied** — no write credentials for `skills.approved` |
| An agent reads another's proposal queue | **404** — its grant covers only its own queue |
| The reviewer promotes the skill | one grant apart; the agent can then load it |
| A governance agent triages every queue | reads across scopes neither agent can; ranks the queue to *ok · warning · critical* and puts the worst first |
| That agent tries to approve | **denied** — triage decides reading order, not outcomes |
| That agent tries to weaken its own rules | **denied** — the rule set is governed data, `select` only |
| Every decision | appends a row to `skills.decisions` — an Iceberg table an auditor can query |
| Every one of the above | lands in the audit log, denials included |

## Two rules that shaped the layout

**The isolation boundary is the table.** Everything holding one agent's content — its
entries *and* its embeddings — lives inside that agent's own namespace. One shared recall
table would let any agent that could read it vector-search everyone's memories, whatever
the paths inside looked like.

**There is no write-without-read grant.** The OpenFGA model reads data through
`can_read_data: select_effective`, and `select_effective` includes `modify_effective`, so
granting write necessarily grants read. Read can be granted without write; the reverse
cannot. That is why each agent files proposals into a table of
its own rather than a shared queue — and it makes attribution structural, since an agent
holds no credentials for a table named after another principal.

## When this is the right tool — and when git is

Most production "skills" today are markdown files in a repo, reviewed by PR and shipped
on deploy. That is already governance: versioned, attributed, reviewed, auditable, free.
**If your skills are human-authored and ship on deploy, use git.** Nothing here beats it.

This is for the case git cannot reach:

| | git | this |
|---|---|---|
| Human writes a procedure, ships on deploy | **use git** | pointless |
| An **agent** writes one at runtime, at volume | nothing to review until you build the machinery | the artifact is the review queue |
| Approval must take effect **without a deploy** | no | agents load it on the next run |
| Different tenants get different skills | awkward — a repo is global | a grant per scope |
| The reviewer is a domain expert, not an engineer | poor fit | console, not a git workflow |
| The author must be **unable** to publish | process and CI; an agent with repo rights can commit | it holds no credentials to write |

A human still approves every skill here — that is the control, not a gap. The point is not
that people are removed from the loop; it is that **the agent is refused the ability to
publish**, by credential vending rather than by policy everyone agrees to follow.

The obvious alternative is to have the agent open a pull request, and for many teams that
is the right answer — you inherit review, history and CI. It loses where a PR is global
rather than per-tenant, where approval must land without a deploy, and where an agent
holding repo credentials could merge its own work.

**Known limit:** human review does not scale past a certain proposal volume. At hundreds
of proposals a day the reviewer is the bottleneck, and the pressure becomes policy-driven
promotion. Governance tags are the natural hook for that (`agent.skill.status` applied by
a policy principal rather than a person), but this example does not demonstrate it.

## The decision log is Iceberg

`skills.decisions` records one row per approval or rejection: when, which skill and
version, who proposed it, who signed, what triage said, which rules fired, the reviewer's
reason, and **`rules_version`** — which rule set was in force at the time. That last column
separates *"we did not check for that then"* from *"we checked and missed it"*.

This is the one place Iceberg earns its keep here, and the reasons that ruled it out for
agent memory are exactly the ones that do not apply:

| | agent memory | decision log |
|---|---|---|
| Shape | isolated per scope — no single table to query | central, one table |
| Readers | agents, via the SDK or MCP | auditors, in SQL |
| Writes | one writer per scope | many reviewers, append-only, ordered |

Generic tables have no commit coordination; Iceberg does. And because it is an ordinary
Iceberg table, the audit question is an ordinary query — from the notebook, or from Trino,
Spark or DuckDB against the same catalog under the same grants.

The catalog's own audit log still records that each write happened and who made it. What
it cannot know is the reviewer's *reason*, or which findings they were looking at. That is
application meaning, and this is where it lives.

## Rules are data, not code

The triage rules live at `governance.policy/triage-rules.json`, not in `review.py`.
Deciding *what counts as suspicious* is the real privilege in this system: weaken a rule
and the triage becomes theatre, silently and with no diff for anyone to review. So the
rule set gets the same treatment as everything else — the governance agent reads it and
cannot change it, and edits are versioned and audited like any other write. It also
answers "which rules were in force when this was approved?".

They are deliberately a **policy, not a skill**. A skill is instructions a model reads and
follows; a policy is rules an engine executes. Storing these as a `SKILL.md` would invite
someone to have the model interpret them — reintroducing exactly the manipulability the
deterministic split exists to remove.

**The verdicts are deterministic; the model only writes the one-line summary.** A
governance agent is by construction a thing that reads attacker-controlled text, so a
model that also decided outcomes could be argued with by the text under review.

**Where it stops:** regexes catch phrasing, not intent. A carefully worded malicious
procedure passes every rule here. Triage buys attention, not safety.

## Requirements

Much lighter than the [agentic-medallion](../agentic-medallion/) example — no torch, no
CLIP:

- **Docker or Podman** with Compose v2
- **~4 GB disk**, of which ~2.5 GB is two local models (`nomic-embed-text` ≈ 275 MB,
  `qwen2.5:3b` ≈ 2 GB)
- **~4 GB RAM** for the Docker VM

Everything runs locally. No API keys, no external calls at run time.

## Quick start

```bash
cd examples/agentic-memory
./up.sh                 # stack + both models; prints the URLs when ready
```

If something already holds the default ports — a natively running Lakekeeper takes 8181
and 9000 — override them:

```bash
LK_PORT=8185 S3_PORT=9010 JUPYTER_PORT=8890 KEYCLOAK_PORT=30085 ./up.sh
```

Then open **JupyterLab** and work through `notebooks/` in order.

### The notebooks

1. **`00-setup.ipynb`** — peter logs in *interactively* (device code: the cell blocks and
   prints a URL you approve in your browser, as **peter / `iceberg`**), creates the
   warehouse and namespaces, and applies the grants. The grant cell is the whole security
   model; everything later follows from it.
2. **`01-agent-learns.ipynb`** — both agents run the same graph, remember what they learn,
   recall it by meaning, and draft a skill for review.
3. **`02-governed.ipynb`** — the denials, the promotion, and the audit trail.
4. **`03-governance-agent.ipynb`** — a governance agent triages the whole queue so the
   reviewer reads the one proposal that matters — and is
   itself refused both the ability to approve and the ability to weaken its own rules.

Edit notebooks through `build_notebooks.py` (they are generated, so diffs stay readable)
and re-run `python build_notebooks.py`.

## Models, and swapping them

Airgapped by default. The provider sits behind two calls — `chat()` and `embed()` — so
pointing it elsewhere is config, not code:

```bash
LLM_PROVIDER=anthropic LLM_API_KEY=... CHAT_MODEL=claude-sonnet-5 ./up.sh
LLM_PROVIDER=openai LLM_API_KEY=... LLM_BASE_URL=https://api.deepseek.com/v1 \
  CHAT_MODEL=deepseek-chat ./up.sh
```

**The governance story is model-independent** — the catalog decides access before a model
is involved at all. One asymmetry to respect: the chat model can change freely, the
**embedding model cannot**. Vectors from different models are not comparable, so the
recall table records its embedder at creation and `MemoryStore` refuses a mismatch rather
than returning confident nonsense.

The agent graph is deterministic (`recall → act → reflect → propose`) rather than a
model-driven tool loop. This example is about where the access boundary sits, and a model
that occasionally forgot to call a tool would make a governance demo look like a flaky
one. It also keeps the model small enough to run on a laptop.

## Seeing the audit trail

Audit logging is on by default. Every access above — including each denial — is a record:

```bash
docker compose logs lakekeeper \
  | grep '"event_source":"audit"' \
  | jq -c '{v: .audit_format, actor: .actor.principal, action: .action.action_name, decision}'
```

`event_source` and `audit_format` are stable contracts — the first will not be renamed, the
second tells you which record shape you are parsing. If you build anything on these records,
pin to a release: an unreleased build declares the version the *next* release will carry and
may not emit all of it yet.

`actor` is authoritative. A `"decision": "denied"` line is how you would notice an agent
probing scopes it has no business in.

## Scope of the guarantee

Be precise about what this does and does not control, because the distinction is the
product:

- It governs **shared, durable** memory. It cannot stop an agent keeping something in its
  context window or writing to its own local disk.
- **Tags classify; they do not gate.** No authorizer reads tag values. They are for
  discovery, and for separating who may classify from who may read — not a wall.
- The Grants API used in `grants.py` is a **preview** API and may change.

## Reset

```bash
./down.sh            # reset, keep the ~2.5 GB of models
./down.sh --purge    # drop the models too
```

## Layout

```
up.sh / down.sh          bring the stack up / reset it
docker-compose.yaml      catalog + OpenFGA + Keycloak + Silo + JupyterLab + Ollama
keycloak/realm.json      peter (device code) + agent-a / agent-b (service accounts)
mlib.py                  config, device login, service-account tokens, id lookups
grants.py                the grants — the file worth reading
llm.py                   chat + embeddings behind one interface, provider-agnostic
agent.py                 the LangGraph graph: recall → act → reflect → propose
build_notebooks.py       generates notebooks/ (edit here, not the .ipynb)
```

Demo secrets only — the Keycloak client secrets and storage keys are throwaway values.
Never reuse them.
