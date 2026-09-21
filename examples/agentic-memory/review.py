"""Skill triage for the governance agent.

The reviewer's problem is volume, not judgement: forty proposals arrive overnight and one
of them matters. This ranks them so a human reads the right three.

**The verdicts are deterministic. The model only writes prose.** That split is the whole
design. A governance agent is, by construction, a thing that reads attacker-controlled
text — a proposed skill is exactly that — so if the model also decided the outcome, a
skill could talk its way through by addressing the reviewer instead of the task. Rules
cannot be argued with, replay identically tomorrow, and can be shown to an auditor.

The model's one job is a readable one-line summary, clearly labelled advisory. Nothing it
writes changes a severity.

And the governance agent cannot act on any of this: it holds `select` on the proposal
queues and nothing on `skills.approved`. It sorts the inbox; a person still signs.

**The rules themselves are governed data, not code.** They live in a `dataset` table at
`governance.policy` as `triage-rules.json`, because the real privilege in this system is
not approving a skill — it is deciding what counts as suspicious. Anyone who can weaken a
rule makes the triage theatre, silently and with no diff to review. So the governance
agent holds `select` on that table and nothing more: it reads the rules it enforces and
cannot change them. Editing them is a security-admin act, versioned and audited like any
other write, which also answers "which rule set was in force when this was approved?"

They are a *policy*, deliberately not a skill. A skill is instructions a model reads and
follows; a policy is rules an engine executes. Storing these as a `SKILL.md` would invite
someone to have the model interpret them, which is precisely the manipulability the
deterministic split exists to avoid.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import IntEnum


class Severity(IntEnum):
    """Ordered so `max()` gives a skill's overall verdict."""

    OK = 0
    WARNING = 1
    CRITICAL = 2

    @property
    def label(self) -> str:
        return {0: "ok", 1: "warning", 2: "critical"}[int(self)]


@dataclass(frozen=True)
class Rule:
    name: str
    severity: Severity
    pattern: re.Pattern[str]
    why: str


def _rx(*alternatives: str) -> re.Pattern[str]:
    return re.compile("|".join(alternatives), re.IGNORECASE)


#: The built-in rule set, used when no governed policy is available (and as the seed for
#: `governance.policy`). Deliberately blunt: a rule that fires on something harmless costs
#: a reviewer ten seconds, one that stays quiet on an exfiltration costs rather more.
DEFAULT_RULES: list[Rule] = [
    Rule(
        "prompt-injection",
        Severity.CRITICAL,
        _rx(r"ignore (all )?(previous|prior) instructions", r"disregard .{0,20}instructions",
            r"system prompt", r"you are now", r"pretend to be"),
        "tries to steer whatever model reads it, not the task",
    ),
    Rule(
        "credential-handling",
        Severity.CRITICAL,
        _rx(r"\bapi[_ -]?key\b", r"\bsecret\b", r"\bpassword\b", r"\bcredential", r"\btoken\b",
            r"access[_ -]?key"),
        "references credentials; skills should never carry or request them",
    ),
    Rule(
        "destructive",
        Severity.CRITICAL,
        _rx(r"\bdelete all\b", r"\bdrop (table|database)\b", r"\btruncate\b", r"rm\s+-rf",
            r"\bpurge\b"),
        "describes an irreversible operation",
    ),
    Rule(
        "network-egress",
        Severity.WARNING,
        _rx(r"https?://", r"\bcurl\b", r"\bwebhook\b", r"\bupload to\b", r"\bpost to\b",
            r"\bexternal (api|service|endpoint)\b"),
        "sends data somewhere outside the system",
    ),
    Rule(
        "broad-scope",
        Severity.WARNING,
        _rx(r"\ball customers\b", r"\bevery (customer|user|account)\b", r"\bentire database\b",
            r"\bacross all\b"),
        "acts across many records rather than the one in hand",
    ),
    Rule(
        "contact-exfiltration",
        Severity.WARNING,
        _rx(r"\bemail (it|them|the)\b", r"\bsend .{0,30}(to|via) (email|slack)\b",
            r"\bforward .{0,20}to\b"),
        "moves content to a channel outside the agent's task",
    ),
]


POLICY_FILE = "triage-rules.json"


def rules_as_policy(rules: list[Rule] | None = None) -> dict:
    """Serialise a rule set to the governed policy document."""
    return {
        "version": 1,
        "rules": [
            {
                "name": r.name,
                "severity": r.severity.label,
                "pattern": r.pattern.pattern,
                "why": r.why,
            }
            for r in (rules if rules is not None else DEFAULT_RULES)
        ],
    }


def rules_from_policy(document: dict) -> list[Rule]:
    """Parse a policy document into executable rules."""
    by_label = {s.label: s for s in Severity}
    parsed: list[Rule] = []
    for entry in document.get("rules", []):
        parsed.append(
            Rule(
                name=entry["name"],
                severity=by_label[entry["severity"]],
                pattern=re.compile(entry["pattern"], re.IGNORECASE),
                why=entry["why"],
            )
        )
    return parsed


def load_rules(policy_store=None) -> list[Rule]:  # noqa: ANN001 - a MemoryStore-like reader
    """Read the governed rule set, falling back to the built-in one.

    `policy_store` is anything with `.get(path) -> str`; a `MemoryStore` scoped to
    `governance.policy` satisfies it. A principal without `select` there raises, and that
    is the correct outcome — triage with unknown rules is worse than no triage.
    """
    if policy_store is None:
        return list(DEFAULT_RULES)
    import json as _json

    return rules_from_policy(_json.loads(policy_store.get(POLICY_FILE)))


@dataclass
class Finding:
    rule: str
    severity: Severity
    why: str
    excerpt: str


@dataclass
class Verdict:
    """One triaged proposal."""

    proposer: str
    name: str
    version: str
    findings: list[Finding] = field(default_factory=list)
    #: Advisory prose from the model. Never affects `severity`.
    summary: str = ""

    @property
    def severity(self) -> Severity:
        return max((f.severity for f in self.findings), default=Severity.OK)

    @property
    def needs_human(self) -> bool:
        return self.severity is Severity.CRITICAL

    def __str__(self) -> str:
        marks = {Severity.OK: "ok      ", Severity.WARNING: "WARNING ", Severity.CRITICAL: "CRITICAL"}
        return f"{marks[self.severity]} {self.name}@{self.version}"


def analyse(body: str, rules: list[Rule] | None = None) -> list[Finding]:
    """Run every rule over a skill body. No model involved."""
    findings: list[Finding] = []
    for rule in rules if rules is not None else DEFAULT_RULES:
        match = rule.pattern.search(body)
        if match:
            start = max(0, match.start() - 30)
            excerpt = body[start : match.end() + 30].replace("\n", " ").strip()
            findings.append(
                Finding(rule=rule.name, severity=rule.severity, why=rule.why, excerpt=excerpt)
            )
    return findings


def summarise(body: str, findings: list[Finding]) -> str:
    """One advisory line from the model. Optional, and never load-bearing."""
    try:
        import llm
    except Exception:  # pragma: no cover - the triage still works without a model
        return ""
    flagged = ", ".join(f.rule for f in findings) or "nothing flagged"
    prompt = (
        "A proposed agent procedure was checked by rules, which flagged: "
        f"{flagged}.\n\nProcedure:\n{body[:800]}\n\n"
        "In one sentence, tell a human reviewer what this procedure does. "
        "Do not judge whether it is safe. Output only that sentence."
    )
    try:
        return llm.chat(prompt, max_tokens=60).strip()
    except Exception:  # pragma: no cover - never let triage fail on a model outage
        return ""


def triage(skills, *, rules: list[Rule] | None = None, summarise_with_model: bool = True) -> list[Verdict]:
    """Triage `(ProposedSkill, Skill)` pairs, most urgent first."""
    verdicts: list[Verdict] = []
    for proposed, skill in skills:
        findings = analyse(skill.body, rules)
        verdicts.append(
            Verdict(
                proposer=proposed.proposer,
                name=proposed.name,
                version=proposed.version,
                findings=findings,
                summary=summarise(skill.body, findings) if summarise_with_model else "",
            )
        )
    verdicts.sort(key=lambda v: (-int(v.severity), v.name))
    return verdicts


def tally(verdicts: list[Verdict]) -> dict[str, int]:
    counts = {"ok": 0, "warning": 0, "critical": 0}
    for v in verdicts:
        counts[v.severity.label] += 1
    return counts


def report(verdicts: list[Verdict]) -> str:
    """The reviewer's inbox, most urgent first."""
    counts = tally(verdicts)
    lines = [
        f"{len(verdicts)} proposals: "
        f"{counts['ok']} ok · {counts['warning']} warning · {counts['critical']} critical",
        "",
    ]
    for v in verdicts:
        lines.append(f"  {v}   by {v.proposer.split('~')[-1][:18]}")
        if v.summary:
            lines.append(f"      {v.summary}")
        for f in v.findings:
            lines.append(f"      [{f.severity.label}] {f.rule}: {f.why}")
            lines.append(f"          …{f.excerpt[:90]}…")
        lines.append("")
    if counts["critical"]:
        lines.append(f"  {counts['critical']} need a human before anything is approved.")
    return "\n".join(lines)
