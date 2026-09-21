# %% [markdown]
# # 01 · The agents work, and remember
#
# Two agents run the **same graph** over the **same code**. Each writes memory into its own
# scope, and each drafts a skill it would like approved.
#
# The graph is deterministic — `recall → load_skills → act → reflect → propose` — rather than
# a model-driven tool loop. This example is about where the access boundary sits, and an LLM
# that occasionally forgot to call a tool would make a governance demo look like a flaky one.
# It also keeps the model small enough to run locally.

# %%
import sys; sys.path.insert(0, '/work')
import mlib, llm
from mlib import NS_AGENT_A, NS_AGENT_B, NS_SHARED, NS_SKILLS, AGENT_A, AGENT_B
from pylakekeeper import Client, ClientCredentials
from pylakekeeper.agents import MemoryStore, SkillStore
from agent import Agent

WAREHOUSE_ID = mlib.warehouse_id(mlib.get_token(*AGENT_A))
embedder = llm.Embedder()

def client_for(creds):
    return Client(
        base_url=mlib.LAKEKEEPER_URL, warehouse=WAREHOUSE_ID,
        auth=ClientCredentials(token_url=mlib.KEYCLOAK_TOKEN_URL,
                               client_id=creds[0], client_secret=creds[1], scope='lakekeeper'),
    )

def build(creds, own_ns, name):
    c = client_for(creds)
    return Agent(
        name=name,
        memory=MemoryStore(c, own_ns, embed=embedder),
        shared=MemoryStore(c, NS_SHARED, embed=embedder),
        skills=SkillStore(c, NS_SKILLS),
    )

agent_a = build(AGENT_A, NS_AGENT_A, 'agent-a')
agent_b = build(AGENT_B, NS_AGENT_B, 'agent-b')
print('models →', llm.describe())

# %% [markdown]
# ## agent-a handles a task
#
# Watch the `learned` line: that is the agent deciding what is worth keeping, and writing it
# through vended credentials into its own scope.

# %%
result = agent_a.run('A customer asks for delivery times in kilometres, not miles.')
print('answer :', result['answer'])
print('learned:', result.get('learned'))

# %%
# It is a governed object now — listable, readable, and audited like any other data.
for path in agent_a.memory.list():
    print(path, '→', agent_a.memory.get(path)[:80])

# %% [markdown]
# ## agent-b handles a different task, in its own scope

# %%
result_b = agent_b.run('Summarise an overdue invoice for a customer in Berlin.')
print('answer :', result_b['answer'])
print('learned:', result_b.get('learned'))
print('paths  :', agent_b.memory.list())

# %% [markdown]
# ## Recall
#
# The same agent, asked something related, finds what it wrote earlier — by meaning, not by
# filename. The vectors live in a Lance table inside the agent's own namespace, so this
# search reaches nothing outside it.

# %%
for hit in agent_a.memory.search('what units does the customer want?', k=3):
    print(f'{hit.distance:.3f}  {hit.scope}  {hit.text[:70]}')

# %% [markdown]
# ## Proposing a skill
#
# The agent drafts a reusable procedure and files it. Note what it *cannot* do: publish it.
# The proposal lands in the agent's own queue and stays there until a human acts.

# %%
run = agent_a.run('A customer asks for delivery times in kilometres, not miles.', propose=True)
print('proposed:', run.get('proposed'))

for p in agent_a.skills.list_proposed():
    print(f'  queued: {p.name}@{p.version}  by {p.proposer}')

print('\napproved library right now:', agent_a.skills.list())

# %% [markdown]
# On a fresh stack the approved library is still empty: nothing the agent did could publish
# anything. The next notebook shows what happens when it tries to publish anyway, and who
# can.
