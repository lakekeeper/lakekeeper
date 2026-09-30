# %% [markdown]
# # 00 · Governance setup
#
# peter — a **person** — logs in interactively, builds the warehouse, and hands out
# exactly the access each agent gets. Nothing below is automated away: the grants in the
# last cell *are* the demo, and every later behaviour follows from them.
#
# Two kinds of identity appear here, and the split is deliberate:
#
# | | How it authenticates | Why |
# |---|---|---|
# | **peter** | device code (browser approval) | a human is present, so no secret sits in a notebook |
# | **agent-a / agent-b** | client credentials | autonomous, no human in the loop |

# %%
import sys; sys.path.insert(0, '/work')
import mlib, grants, llm
from mlib import NS_AGENT_A, NS_AGENT_B, NS_SHARED, NS_SKILLS, AGENT_A, AGENT_B, WAREHOUSE_NAME

print('models →', llm.describe())

# %% [markdown]
# ## peter logs in
#
# This cell **blocks**. It prints a URL and a code: open it, sign in as **peter / `iceberg`**,
# approve. The kernel runs inside the docker network and cannot show you a login screen, so
# the URL is rewritten to an address your host browser can reach.

# %%
peter = mlib.device_login()
PETER = peter.token

# %%
# Bootstrap makes the first authenticated principal the platform admin.
import requests
r = requests.post(
    f'{mlib.MANAGEMENT_URL}/v1/bootstrap',
    headers=mlib.auth(PETER),
    json={'accept-terms-of-use': True, 'is-operator': True},
    timeout=30,
)
print('bootstrap:', r.status_code, '(400 = already bootstrapped)')

# %% [markdown]
# ## The warehouse
#
# One S3 warehouse on Silo, with STS enabled — that is what lets Lakekeeper mint
# short-lived, prefix-scoped credentials instead of handing out a static key.

# %%
import requests
body = {
    'warehouse-name': WAREHOUSE_NAME,
    'project-id': '00000000-0000-0000-0000-000000000000',
    'storage-profile': {
        'type': 's3', 'bucket': 'agentmem', 'key-prefix': 'warehouse',
        'region': 'local-01',
        'endpoint': mlib.S3_ENDPOINT, 'sts-endpoint': mlib.S3_ENDPOINT,
        'assume-role-arn': None, 'path-style-access': True,
        'flavor': 's3-compat', 'sts-enabled': True,
    },
    'storage-credential': {
        'type': 's3', 'credential-type': 'access-key',
        'access-key-id': 'silo-root-user', 'secret-access-key': 'silo-root-password',
    },
}
r = requests.post(f'{mlib.MANAGEMENT_URL}/v1/warehouse', headers=mlib.auth(PETER), json=body, timeout=30)
# Re-running this notebook is fine: a second create returns 400 with
# CreateWarehouseStorageProfileOverlap (not 409 — the name is free, the storage prefix is not).
if r.status_code not in (200, 201, 204):
    kind = r.json().get('error', {}).get('type', '')
    assert kind in ('CreateWarehouseStorageProfileOverlap', 'WarehouseAlreadyExists'), r.text
    print('warehouse already exists — reusing it')
else:
    print('warehouse created')
WAREHOUSE_ID = mlib.warehouse_id(PETER)
print('warehouse id:', WAREHOUSE_ID)

# %% [markdown]
# ## Namespaces and tables
#
# The rule that shapes this layout: **the isolation boundary is the table.** So everything
# holding one agent's content — its entries *and* its embeddings — lives inside that agent's
# own namespace. A single shared recall table would let one agent vector-search another's
# memories, whatever the paths inside it looked like.
#
# Skills are the opposite: one central library, because a procedure approved once should be
# available to everyone granted it.

# %%
from pylakekeeper import Client, ClientCredentials
from pylakekeeper.agents import MemoryStore, SkillStore

def client_for(creds):
    """A pylakekeeper Client for a service account."""
    return Client(
        base_url=mlib.LAKEKEEPER_URL,
        warehouse=WAREHOUSE_ID,
        auth=ClientCredentials(
            token_url=mlib.KEYCLOAK_TOKEN_URL,
            client_id=creds[0], client_secret=creds[1], scope='lakekeeper',
        ),
    )

class PeterAuth:
    """peter's device-code session, in the shape pylakekeeper's Auth expects."""
    def __init__(self, session): self._s = session
    def auth_header(self): return f'Bearer {self._s.token}'
    def invalidate(self): pass

peter_client = Client(base_url=mlib.LAKEKEEPER_URL, warehouse=WAREHOUSE_ID, auth=PeterAuth(peter))

for ns in (NS_AGENT_A, NS_AGENT_B, NS_SHARED, NS_SKILLS, f'{NS_SKILLS}.proposed', mlib.NS_POLICY):
    grants.create_namespace(PETER, ns)
print('namespaces:', NS_AGENT_A, NS_AGENT_B, NS_SHARED, NS_SKILLS,
      f'{NS_SKILLS}.proposed', mlib.NS_POLICY)

# %%
embedder = llm.Embedder()

# peter creates the tables, so the agents need no `create` right of their own.
for ns in (NS_AGENT_A, NS_AGENT_B, NS_SHARED):
    MemoryStore(peter_client, ns, embed=embedder).ensure_tables()

# One approved library, plus one proposal table per agent — named for the agent, so an
# agent has no credentials for a queue filed under anyone else's name.
SkillStore(peter_client, NS_SKILLS, proposer='peter').ensure_tables()
for agent_id in ('agent-a', 'agent-b'):
    uid = mlib.agent_user_id(mlib.AGENTS[agent_id])
    SkillStore(peter_client, NS_SKILLS, proposer=uid).ensure_tables()
    print('proposal queue for', uid)

# %% [markdown]
# ## The grants — this is the demo
#
# Read the table below as the whole security model. Everything notebook 02 shows follows
# from it, and nothing in the agent code enforces any of it.
#
# | principal | resource | privilege | effect |
# |---|---|---|---|
# | agent-a | namespace `agent_memory.agent_a` | `modify` | read **and** write its own memory |
# | agent-a | namespace `agent_memory.shared` | `select` | read the shared tier |
# | agent-a | table `skills.approved` | `select` | load approved skills, **cannot write them** |
# | agent-a | table `skills.proposed.<agent-a>` | `modify` | file its own proposals |
# | agent-b | …the same, in its own scope | | and **nothing** on agent-a's |
#
# Note what is *absent*: neither agent has anything on the other's memory namespace, and
# neither has `modify` on `skills.approved`. Those two absences are the demo.

# %%
from pylakekeeper.agents import path_safe

# Privilege names come from the authorizer and differ per resource type — a namespace has
# `create`, a generic table does not. Read them off the server rather than hardcoding.
print('namespace    :', grants.assignable_privileges(PETER, 'namespace'))
print('generic table:', grants.assignable_privileges(PETER, 'generic-table'))
print()

approved_id = grants.generic_table_id(PETER, NS_SKILLS, 'approved')

for agent_id, creds in mlib.AGENTS.items():
    uid = mlib.agent_user_id(creds)
    own_ns = NS_AGENT_A if agent_id == 'agent-a' else NS_AGENT_B

    # Its own memory: read + write. `modify` implies `select` in the OpenFGA model.
    grants.grant_namespace(PETER, WAREHOUSE_ID, mlib.namespace_id(PETER, own_ns), uid, 'modify')
    # The shared tier: read only.
    grants.grant_namespace(PETER, WAREHOUSE_ID, mlib.namespace_id(PETER, NS_SHARED), uid, 'select')
    # The approved library: read only — granted on the TABLE, because a grant on the
    # `skills` namespace would flow down into every agent's proposal queue.
    grants.grant_generic_table(PETER, WAREHOUSE_ID, approved_id, uid, 'select')
    # Its own proposal queue: read + write, and nobody else's.
    queue_id = grants.generic_table_id(PETER, f'{NS_SKILLS}.proposed', path_safe(uid))
    grants.grant_generic_table(PETER, WAREHOUSE_ID, queue_id, uid, 'modify')

    print(f'{agent_id:8s} -> modify {own_ns} | select {NS_SHARED} | select skills.approved | modify its own queue')

# --- The governance agent: reads everything, writes nothing --------------------
# Deciding what counts as suspicious is the real privilege in this system, so the rule
# set is governed data, not code. The governance agent reads it; only peter can change it.
import json, review
policy = MemoryStore(peter_client, mlib.NS_POLICY, entries_table=mlib.POLICY_TABLE)
policy.ensure_tables()
policy.put(review.POLICY_FILE, json.dumps(review.rules_as_policy(), indent=2), index=False)

gov_uid = mlib.agent_user_id(mlib.GOVERNANCE)
grants.grant_namespace(PETER, WAREHOUSE_ID,
                       mlib.namespace_id(PETER, f'{NS_SKILLS}.proposed'), gov_uid, 'select')
grants.grant_namespace(PETER, WAREHOUSE_ID,
                       mlib.namespace_id(PETER, 'agent_memory'), gov_uid, 'select')
grants.grant_generic_table(PETER, WAREHOUSE_ID,
                           grants.generic_table_id(PETER, mlib.NS_POLICY, mlib.POLICY_TABLE),
                           gov_uid, 'select')
print(f'governance -> select on skills.proposed, agent_memory, {mlib.NS_POLICY}.{mlib.POLICY_TABLE}')
print('           -> nothing on skills.approved, and no write on the policy it enforces')

print('\nsetup complete — run 01-agent-learns next')
