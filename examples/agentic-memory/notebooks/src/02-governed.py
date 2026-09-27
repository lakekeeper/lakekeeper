# %% [markdown]
# # 02 · Governance in action
#
# Six things, in order. None of them is enforced by the agent code — every one is the
# catalog answering a credential request.

# %%
import sys; sys.path.insert(0, '/work')
import mlib, grants, llm
from mlib import (NS_AGENT_A, NS_AGENT_B, NS_SHARED, NS_SKILLS,
                  AGENT_A, AGENT_B, GOVERNANCE)
from pylakekeeper import Client, ClientCredentials, NotFoundError
from pylakekeeper.agents import MemoryStore, SkillStore

WAREHOUSE_ID = mlib.warehouse_id(mlib.get_token(*AGENT_A))
embedder = llm.Embedder()

def client_for(creds):
    return Client(base_url=mlib.LAKEKEEPER_URL, warehouse=WAREHOUSE_ID,
                  auth=ClientCredentials(token_url=mlib.KEYCLOAK_TOKEN_URL,
                                         client_id=creds[0], client_secret=creds[1],
                                         scope='lakekeeper'))

a_client, b_client = client_for(AGENT_A), client_for(AGENT_B)

# %% [markdown]
# ## 1 · Shared memory is shared
#
# Memory promoted to the shared tier is readable by both agents — central, not a `.md` file
# sitting in one process.

# %%
shared_a = MemoryStore(a_client, NS_SHARED, embed=embedder)
shared_b = MemoryStore(b_client, NS_SHARED, embed=embedder)

# Both agents see the same shared tier — one place, not a file per process.
print('agent-a sees:', shared_a.list())
print('agent-b sees:', shared_b.list(), ' <- the same objects, read under its own identity')

# But neither may write it: they hold `select` there, not `modify`.
for label, store in (('agent-a', shared_a), ('agent-b', shared_b)):
    try:
        store.put('memories/house-style.md', 'Always quote distances in kilometres.')
        print(f'{label} WROTE shared memory (unexpected)')
    except Exception as exc:
        print(f'{label} cannot write shared memory:', type(exc).__name__)

# %% [markdown]
# ## 2 · Scope isolation
#
# agent-b asks for agent-a's private memory. Lakekeeper answers **404**, not 403 — it does
# not admit that a table the caller may not see exists at all.

# %%
intruder = MemoryStore(b_client, NS_AGENT_A, embed=embedder)
try:
    print(intruder.list())
    print('agent-b READ agent-a memory (unexpected!)')
except NotFoundError as exc:
    print('DENIED — agent-b cannot see agent-a memory:', exc.status_code, str(exc)[:80])

# %% [markdown]
# And the fan-out version, which is how an agent actually searches: it asks every scope it
# knows about, and the ones it may not read simply drop out. The agent never enumerates its
# own permissions — the catalog answers by refusing.

# %%
scopes = [MemoryStore(b_client, ns, embed=embedder) for ns in (NS_AGENT_B, NS_AGENT_A, NS_SHARED)]
hits = MemoryStore.search_many(scopes, 'delivery units', k=5)
print('agent-b searched 3 scopes, got hits from:', sorted({h.scope for h in hits}) or '(none)')
print('agent_memory.agent_a among them:', any(h.scope == NS_AGENT_A for h in hits))

# %% [markdown]
# ## 3 · An agent cannot approve its own skill
#
# The agent runs the *same* `approve()` a reviewer runs. It fails — not because of a check
# in this code, but because Lakekeeper vends no write credentials for `skills.approved`, so
# the agent holds keys that cannot PUT there.

# %%
a_skills = SkillStore(a_client, NS_SKILLS)
queued = a_skills.list_proposed()
print('agent-a queue:', [(p.name, p.version) for p in queued])

try:
    a_skills.approve(queued[0])
    print('agent-a APPROVED its own skill (unexpected!)')
except Exception as exc:
    print('DENIED — no write credentials for skills.approved:', type(exc).__name__)

# %% [markdown]
# ## 4 · The human promotes it
#
# peter holds `modify` on `skills.approved`. One call, and the skill becomes available to
# every agent granted the library.

# %%
peter = mlib.device_login()

class PeterAuth:
    def __init__(self, s): self._s = s
    def auth_header(self): return f'Bearer {self._s.token}'
    def invalidate(self): pass

peter_client = Client(base_url=mlib.LAKEKEEPER_URL, warehouse=WAREHOUSE_ID, auth=PeterAuth(peter))
reviewer = SkillStore(peter_client, NS_SKILLS, proposer=queued[0].proposer)

skill = reviewer.read_proposed(queued[0])
print('reviewing:', skill.name, 'by', skill.proposer)
print(skill.body[:400])

# %%
reviewer.approve(queued[0])
print('approved library now:', a_skills.list())
print()
print('agent-a can load it:', a_skills.load(queued[0].name).name)

# %% [markdown]
# ## 5 · Cross-agent oversight, through the same door
#
# A governance principal granted `select` on the whole `agent_memory` namespace reads every
# agent's memory — via the normal authorizer, not a back channel. Its reads are audited like
# anyone else's, which is the point: oversight that is itself accountable.

# %%
# The governance principal, granted `select` on the whole agent_memory namespace in
# notebook 00 — not peter, and not a back channel. Same authorizer, different identity.
gov_client = client_for(GOVERNANCE)
print('governance reads across every scope:')
for ns in (NS_AGENT_A, NS_AGENT_B, NS_SHARED):
    store = MemoryStore(gov_client, ns, embed=embedder)
    try:
        print(f'  {ns:28s} {len(store.list())} entries')
    except Exception as exc:
        print(f'  {ns:28s} denied ({type(exc).__name__})')

# And it is not a skeleton key: it can read memory and still cannot publish a skill.
try:
    SkillStore(gov_client, NS_SKILLS, proposer=mlib.agent_user_id(AGENT_A)).approve(
        SkillStore(gov_client, NS_SKILLS, proposer=mlib.agent_user_id(AGENT_A))
        .list_proposed()[0])
    print('governance APPROVED a skill (unexpected!)')
except Exception as exc:
    print('  and cannot approve anything:', type(exc).__name__)

# %% [markdown]
# ## 6 · The receipt
#
# Every one of the above — the reads, the writes, the denials — is in Lakekeeper's audit
# log, on by default. `actor` is authoritative; `decision` says what happened.
#
# Run this in a terminal on the host:
#
# ```bash
# docker compose logs lakekeeper \
#   | grep '"event_source":"audit"' \
#   | jq -c '{actor: .actor.principal, action: .action.action_name, entity: .entity.entity_type, decision}'
# ```
#
# A denial appears as `"decision": "denied"` with the actor that attempted it — which is how
# you would notice an agent probing scopes it has no business in.

# %%
# Same thing from in here, via the compose socket if it is mounted; otherwise run the
# command above on the host.
print('audit log: docker compose logs lakekeeper | grep \'"event_source":"audit"\'')

# %% [markdown]
# ---
#
# **What held the line.** Not the agent framework, not the prompt, not this notebook. The
# agent was refused storage credentials, so the bytes were unreachable — the same result it
# would get with different code, a different model, or a jailbroken prompt.
#
# **Scope of the guarantee.** This governs *shared, durable* memory. It cannot stop an agent
# keeping something in its context window or writing to its own local disk. And note the
# one-way asymmetry in the model: read can be granted without write, but **write cannot be
# granted without read** — so "write-only" queues do not exist, which is why each agent has
# a proposal table of its own.
#
# **Tags classify; they do not gate.** No authorizer reads tag values. Use them for
# discovery and for separating who may classify from who may read — not as a wall.
