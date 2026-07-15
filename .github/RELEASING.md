# Releasing — runbook

## The three files

| File | What | Who maintains |
|------|------|----------------|
| `CHANGELOG.md` | release-please index: PR-title **headlines** + PR links + SHAs. Headlines only — never hand-edited. | release-please (auto) |
| `site/docs/about/release-notes.md` | Curated, **customer-facing** release notes — the published "Release Notes" page on the docs site. | summarised at release from PR descriptions |
| `.github/RELEASING.md` | This runbook. | maintainers |

`CHANGELOG.md` is the *index* used to find a release's PRs; the customer-facing prose
on the site page is **summarised from each PR's description** — not from the changelog
titles.

## Per-PR (during development)

Write a clear PR description of the user-visible change and its benefit (not the
implementation). That's the only ask — no special block, no label.

Optionally add a dedicated **`## Release notes`** section in the PR description to fix
the exact customer-facing wording for a subtle or high-impact change.

## At release

release-please maintains `CHANGELOG.md` and, on a merged release PR, cuts a **draft**
GitHub Release + tag (`vX.Y.Z`).

```bash
VERSION=0.12.3        # the release just cut, without the leading v
TAG="v$VERSION"
NOTES=site/docs/about/release-notes.md
```

1. **List the PRs in this release** from the new `CHANGELOG.md` section:
   ```bash
   awk -v v="$VERSION" '
     $0 ~ "^## \\[" v "\\]" {f=1; next}
     f && /^## \[/ {exit}
     f' CHANGELOG.md | grep -oE '#[0-9]+' | tr -d '#' | sort -un
   ```
2. **Read each PR's description and summarise it** (agent-assisted is fine; use a PR's
   `## Release notes` section verbatim when it has one):
   ```bash
   gh pr view <N> --repo lakekeeper/lakekeeper --json title,body
   ```
3. **Add the `## $TAG (date)` section** at the top of `$NOTES` (newest first): group into
   Highlights / Features / Bug Fixes / Breaking Changes / Upgrade Notes; one line per
   item; link the PRs as `[#NNNN](https://github.com/lakekeeper/lakekeeper/pull/NNNN)`.
4. **Commit `$NOTES` to `main`** (a normal commit; the site redeploys from it). Do **not**
   edit it inside the release-please PR — release-please force-regenerates that branch on
   every push to `main` and would clobber the change.
5. **Set the GitHub Release body** from the new section:
   ```bash
   gh release edit "$TAG" --repo lakekeeper/lakekeeper \
     --notes-file <(awk -v t="## $TAG" 'index($0,t)==1{f=1;next} f&&/^## /{exit} f' "$NOTES")
   ```

## House style

Keep entries customer-facing and short — **one line per change, benefit first**. Inline
only the single most important setting (flag / env var); link everything else to the
docs. Add `### Highlights` only when 2-3 changes genuinely stand out. Omit empty
sections. Link the (public) PRs as Markdown links. Credit external contributors with
`(thanks @handle)`.

Sections, in order: **Highlights · Features · Bug Fixes · Breaking Changes · Upgrade
Notes**.

## What to leave out / collapse

The `CHANGELOG.md` PR list is raw input, not the release notes. Curate it:

- **Don't list a bug fix for code first introduced in the same release.** If the feature
  and its follow-up fix both land in this version, the bug never shipped — fold the fix
  into the feature (or drop it). Check with `git show <last-release-tag>:<path>`: if the
  fixed code/route didn't exist at the previous release, it's a same-release fix. (A fix
  for a path that *did* exist at the last release is a real, listable fix.)
- **One line per feature, even when it spanned several PRs.** Backend + management API +
  console PRs for the same capability are a single entry citing all the PRs together.
- **Highlight what matters to OSS users.** The OSS authorizer is OpenFGA; changes that
  only affect the built-in/internal authorization store are not OSS highlights (OpenFGA
  already covers most of that ground) — keep them to a modest Features line or omit.
- **Don't re-announce features that shipped in a parallel `rel-*` patch.** Patch releases
  are cut from `rel-*` branches, so `main`'s release-please CHANGELOG re-lists those PRs
  under the next minor (it diffs `last-main-release...this`). Put them in the patch's own
  notes section and omit them here. Add the patch section too if it was never written up.

## Notes

- No CI generation, no API key, no `git-cliff`. Summarising is a manual/agent-assisted
  pass at release; clear PR descriptions are what make it easy.
- The page is published on the docs site and its sections are reused by Lakekeeper
  Enterprise's upstream-changes rollup — keep them customer-facing and accurate.
- If this becomes a bottleneck (much higher PR volume, or notes get skipped), graduate
  to changelog fragments (`changie`/towncrier or a homegrown assemble step).
