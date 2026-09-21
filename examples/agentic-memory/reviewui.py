"""A click-through review queue for the notebook.

The triage decides reading order; this is where a human actually signs. It exists because
"peter approves things" is easy to assert in prose and worth showing as an interaction:
the reviewer sees what the rules flagged, opens what matters, and every button press is a
real catalog write under peter's own identity.

Two pieces of friction are deliberate, and they are the same idea at two scales:

* **A critical proposal cannot be approved until its body has been opened.** Rules cannot
  separate malicious intent from clumsy phrasing, so the most they can honestly do is
  force a person to look. Approving anyway stays possible — after reading.
* **Bulk approval covers ok and warning, never critical.** A button that could sweep a
  critical finding through would quietly undo the rule above. Warnings bulk-approve behind
  a confirm step, because "flagged, and I accept it" should take two clicks, not one.
"""

from __future__ import annotations

import ipywidgets as widgets
from IPython.display import display
from pylakekeeper.agents import ProposedSkill

import review

_COLOUR = {"ok": "#2f7d4f", "warning": "#9a5a08", "critical": "#a33a2e"}
_BUTTON = {"ok": "success", "warning": "warning", "critical": "danger"}


def _badge(severity: str) -> widgets.HTML:
    return widgets.HTML(
        f'<span style="background:{_COLOUR[severity]};color:white;padding:2px 8px;'
        f'border-radius:3px;font-size:11px;font-weight:600;letter-spacing:.06em">'
        f"{severity.upper()}</span>"
    )


class Row:
    """One proposal, its controls, and whatever has been decided about it."""

    def __init__(self, verdict, body: str, reviewer, log: widgets.Output) -> None:
        self.verdict = verdict
        self.body = body
        self.reviewer = reviewer
        self.log = log
        self.settled = False
        self.key = f"{verdict.name}@{verdict.version}"
        self.severity = verdict.severity.label

        self.show = widgets.Button(description="Read it", icon="eye",
                                   layout=widgets.Layout(width="110px"))
        self.approve = widgets.Button(description="Approve", button_style=_BUTTON[self.severity],
                                      layout=widgets.Layout(width="110px"))
        self.reject = widgets.Button(description="Reject", layout=widgets.Layout(width="110px"))
        self.reason = widgets.Text(placeholder="reason (required to reject)",
                                   layout=widgets.Layout(width="300px"))
        self.body_box = widgets.Output()
        self.status = widgets.HTML("")

        if verdict.severity is review.Severity.CRITICAL:
            self.approve.disabled = True
            self.approve.tooltip = "Open the proposal before approving a critical finding"

        self.show.on_click(self._on_show)
        self.approve.on_click(lambda _: self.do_approve())
        self.reject.on_click(lambda _: self._on_reject())

    # ------------------------------------------------------------------ rendering

    def widget(self) -> widgets.VBox:
        v = self.verdict
        header = widgets.HBox(
            [
                _badge(self.severity),
                widgets.HTML(
                    f"<b>{v.name}</b> "
                    f'<span style="color:#888">@{v.version} · '
                    f'by {v.proposer.split("~")[-1][:22]}</span>'
                ),
                self.status,
            ],
            layout=widgets.Layout(align_items="center"),
        )
        detail = [widgets.HTML(f'<i style="color:#666">{v.summary}</i>')] if v.summary else []
        for f in v.findings:
            detail.append(
                widgets.HTML(
                    f'<div style="font-size:12px;color:{_COLOUR[f.severity.label]}">'
                    f"&nbsp;&nbsp;<b>{f.rule}</b> — {f.why}</div>"
                    f'<div style="font-size:11px;color:#888;font-family:monospace">'
                    f"&nbsp;&nbsp;&nbsp;&nbsp;…{f.excerpt[:100]}…</div>"
                )
            )
        controls = widgets.HBox([self.show, self.approve, self.reject, self.reason])
        return widgets.VBox(
            [header, *detail, controls, self.body_box],
            layout=widgets.Layout(border="1px solid #ddd", padding="10px",
                                  margin="0 0 8px 0", width="100%"),
        )

    # -------------------------------------------------------------------- actions

    def _on_show(self, _) -> None:
        self.approve.disabled = False
        self.approve.tooltip = ""
        with self.body_box:
            self.body_box.clear_output()
            print(self.body)

    def _settle(self, message: str, colour: str) -> None:
        self.settled = True
        for b in (self.approve, self.reject, self.show):
            b.disabled = True
        self.status.value = f'<span style="color:{colour};font-weight:600">&nbsp;{message}</span>'

    def do_approve(self) -> bool:
        """Approve this proposal. Returns whether the catalog accepted it."""
        if self.settled:
            return False
        v = self.verdict
        try:
            self.reviewer.approve(ProposedSkill(v.proposer, v.name, v.version))
            self._settle("approved", _COLOUR["ok"])
            with self.log:
                print(f"APPROVED  {self.key}")
            return True
        except Exception as exc:  # noqa: BLE001 - shown, never swallowed
            with self.log:
                print(f"failed to approve {self.key}: {type(exc).__name__}: {exc}")
            return False

    def _on_reject(self) -> None:
        if self.settled:
            return
        reason = self.reason.value.strip()
        if not reason:
            with self.log:
                print(f"{self.key}: a reason is required to reject")
            return
        v = self.verdict
        try:
            self.reviewer.reject(ProposedSkill(v.proposer, v.name, v.version), reason=reason)
            self._settle("rejected", _COLOUR["critical"])
            with self.log:
                print(f"REJECTED  {self.key} — {reason}")
        except Exception as exc:  # noqa: BLE001
            with self.log:
                print(f"failed to reject {self.key}: {type(exc).__name__}: {exc}")


def _bulk_button(rows: list[Row], severity: str, log: widgets.Output) -> widgets.Button:
    """Approve every still-pending row at one severity.

    Warnings take two clicks. They were flagged for a reason, and "I have seen these and
    accept them" is a different act from clearing the ones nothing fired on.
    """
    pending = [r for r in rows if r.severity == severity]
    confirm_first = severity == "warning"
    button = widgets.Button(
        description=f"Approve all {severity} ({len(pending)})",
        button_style=_BUTTON[severity],
        layout=widgets.Layout(width="230px"),
        disabled=not pending,
    )
    state = {"armed": not confirm_first}

    def on_click(_):
        todo = [r for r in pending if not r.settled]
        if not todo:
            button.disabled = True
            return
        if not state["armed"]:
            state["armed"] = True
            plural = severity if len(todo) == 1 else f"{severity}s"
            button.description = f"Confirm: approve {len(todo)} {plural}?"
            return
        done = sum(1 for r in todo if r.do_approve())
        with log:
            print(f"— bulk approved {done} {severity} proposal(s)")
        button.disabled = True
        button.description = f"Approve all {severity} ({len(pending)})"
        state["armed"] = not confirm_first

    button.on_click(on_click)
    return button


def review_queue(verdicts, bodies: dict[str, str], reviewer) -> list[Row]:
    """Render the queue, most urgent first, with a real write behind every button.

    Args:
        verdicts: from :func:`review.triage`, already sorted.
        bodies: ``{"name@version": skill body}`` for the "Read it" panel.
        reviewer: a `SkillStore`-like object authenticated as the human. An agent's would
            make every button fail at the catalog, which is the correct outcome.
    """
    counts = review.tally(verdicts)
    log = widgets.Output()
    rows = [Row(v, bodies.get(f"{v.name}@{v.version}", ""), reviewer, log) for v in verdicts]

    display(
        widgets.HTML(
            f'<h3 style="margin:0 0 4px">Review queue</h3>'
            f'<div style="color:#666;margin-bottom:10px">{len(verdicts)} proposals · '
            f'<span style="color:{_COLOUR["ok"]}">{counts["ok"]} ok</span> · '
            f'<span style="color:{_COLOUR["warning"]}">{counts["warning"]} warning</span> · '
            f'<span style="color:{_COLOUR["critical"]}">{counts["critical"]} critical</span>'
            "</div>"
        )
    )
    display(
        widgets.HBox(
            [
                _bulk_button(rows, "ok", log),
                _bulk_button(rows, "warning", log),
                widgets.HTML(
                    '<span style="color:#888;font-size:12px;padding-left:10px">'
                    "critical is excluded from bulk actions — open it and decide</span>"
                ),
            ],
            layout=widgets.Layout(margin="0 0 14px 0", align_items="center"),
        )
    )
    for row in rows:
        display(row.widget())
    display(log)
    return rows
