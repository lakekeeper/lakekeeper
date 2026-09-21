"""The agent: a deterministic LangGraph state graph over governed memory.

    recall → act → reflect → propose

Every memory operation is a **node**, not a tool the model may or may not choose to
call. That is deliberate. This example is about where the access boundary lives, and an
LLM that occasionally forgets to call `search_memory` would make a governance demo look
like a flaky one. It also keeps the chat model small — a 2 GB local model is plenty,
because nothing here depends on tool-calling ability. A model-driven tool loop is the
other reasonable shape; it is not what this example is arguing about.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from pylakekeeper import NotFoundError
from pylakekeeper.agents import MemoryStore, SkillStore

import llm


class AgentState(TypedDict, total=False):
    """What flows through the graph."""

    task: str
    recalled: list[dict[str, Any]]
    skills: list[str]
    answer: str
    learned: str | None
    proposed: str | None
    denied: list[str]


@dataclass
class Agent:
    """One agent: its own memory scope, the shared tier, and the skill library.

    Args:
        name: display name, used in printed output.
        memory: this agent's own scope — read/write.
        shared: the shared tier — read-only for agents.
        skills: the skill library. `load_all()` returns only what this principal may read.
    """

    name: str
    memory: MemoryStore
    shared: MemoryStore | None = None
    skills: SkillStore | None = None
    #: Scopes whose reads were refused during the last run, for the notebooks to show.
    denied: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------ graph nodes

    def _recall(self, state: AgentState) -> AgentState:
        """Search every scope this agent may read, and let the catalog filter.

        This is what `MemoryStore.search_many` does internally, written out so the
        notebook can show *which* scopes refused. The agent never enumerates its own
        permissions — it asks every scope it knows about and the catalog answers by
        refusing, which arrives as a 404 because a table you may not read is not admitted
        to exist.

        Denial is recorded from that refusal, not from an empty result: a scope this agent
        may read but has not written to yet returns nothing, and that is not a denial.
        """
        scopes = [s for s in (self.memory, self.shared) if s is not None]
        hits, denied = [], []
        for scope in scopes:
            try:
                hits.extend(scope.search(state["task"], k=4))
            except NotFoundError:
                denied.append(scope.scope)
        hits.sort(key=lambda h: h.distance)
        hits = hits[:4]
        self.denied = denied
        return {
            "recalled": [
                {"path": h.path, "text": h.text, "scope": h.scope, "distance": h.distance}
                for h in hits
            ],
            "denied": list(self.denied),
        }

    def _load_skills(self, state: AgentState) -> AgentState:
        """Load approved skills. A skill the agent may not read simply is not there."""
        if self.skills is None:
            return {"skills": []}
        try:
            loaded = self.skills.load_all()
        except Exception:  # noqa: BLE001 - no grant on the library at all
            return {"skills": []}
        return {"skills": [s.body for s in loaded]}

    def _act(self, state: AgentState) -> AgentState:
        """Answer the task, using recalled memory and any approved skills."""
        memory_block = "\n".join(f"- {m['text']}" for m in state.get("recalled", []))
        skills_block = "\n\n".join(state.get("skills", []))
        prompt = (
            f"Task: {state['task']}\n\n"
            f"What you remember:\n{memory_block or '(nothing yet)'}\n\n"
            f"Approved procedures available to you:\n{skills_block or '(none)'}\n\n"
            "Follow a procedure only if it is relevant to this task; ignore it otherwise. "
            "Answer the task in three sentences or fewer."
        )
        answer = llm.chat(prompt, system="You are a careful assistant. Be concise and concrete.")
        return {"answer": answer}

    def _reflect(self, state: AgentState) -> AgentState:
        """Decide what is worth remembering, and write it to this agent's own scope."""
        # No "reply NOTHING if unsure" escape hatch: a small model takes it every time,
        # and then the demo has no memory to show. Asking directly for one sentence gets
        # one sentence. Memory quality is not what this example is demonstrating.
        prompt = (
            "Exchange:\n"
            f"task: {state['task']}\n"
            f"result: {state.get('answer', '')}\n\n"
            "Write exactly one short declarative sentence recording what should be "
            "remembered about this customer or their preferences for next time. "
            "Output only that sentence."
        )
        learned = llm.chat(prompt, max_tokens=60).strip()
        if not learned:
            return {"learned": None}

        path = f"memories/{abs(hash(state['task'])) % 10**8:08d}.md"
        self.memory.put(path, learned, metadata={"kind": "episodic", "task": state["task"][:80]})
        return {"learned": learned}

    def _propose(self, state: AgentState) -> AgentState:
        """Draft a reusable procedure and file it for review.

        The agent files into its *own* proposal table. It cannot write the approved
        library, and it cannot file under another agent's name — the table it may write
        is the one named for it.
        """
        if self.skills is None or not state.get("answer"):
            return {"proposed": None}
        prompt = (
            f"You just handled this task: {state['task']}\n\n"
            "Write a short reusable procedure (3-5 numbered steps) that would help "
            "handle tasks like it next time. Steps only, no preamble."
        )
        body = llm.chat(prompt, max_tokens=250)
        name = _slug(state["task"])
        skill = self.skills.propose(name, body, description=f"Procedure for: {state['task'][:70]}")
        return {"proposed": f"{skill.name}@{skill.version}"}

    # ----------------------------------------------------------------------- graph

    def graph(self, *, propose: bool = False):  # noqa: ANN201 - LangGraph's compiled type
        """Compile the graph. With ``propose``, reflection is followed by a draft skill."""
        builder = StateGraph(AgentState)
        builder.add_node("recall", self._recall)
        builder.add_node("load_skills", self._load_skills)
        builder.add_node("act", self._act)
        builder.add_node("reflect", self._reflect)

        builder.add_edge(START, "recall")
        builder.add_edge("recall", "load_skills")
        builder.add_edge("load_skills", "act")
        builder.add_edge("act", "reflect")

        if propose:
            builder.add_node("propose", self._propose)
            builder.add_edge("reflect", "propose")
            builder.add_edge("propose", END)
        else:
            builder.add_edge("reflect", END)
        return builder.compile()

    def run(self, task: str, *, propose: bool = False) -> AgentState:
        """Run the graph once. `propose=True` adds the reflect→propose step."""
        return self.graph(propose=propose).invoke({"task": task})


def _slug(text: str) -> str:
    keep = [c.lower() if c.isalnum() else "-" for c in text[:40]]
    return "-".join(filter(None, "".join(keep).split("-")))[:40] or "skill"
