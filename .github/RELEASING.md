# Releasing — runbook

## The files

| File | What | Who maintains |
|------|------|----------------|
| `CHANGELOG.md` | release-please index: PR-title **headlines** + PR links + SHAs. Headlines only — never hand-edited. | release-please (auto) |
| `site/docs/about/release-notes.md` | Curated, **customer-facing** release notes — the published "Release Notes" page on the docs site. | summarised at release from PR descriptions |
| `audit-format/` | The audit log format changes: `unreleased/*.md`, one fragment per change since the last release tag. | `just audit-format-release` after the tag |
| `.github/RELEASING.md` | This runbook. | maintainers |

`CHANGELOG.md` is the *index* used to find a release's PRs; the customer-facing prose
on the site page is **summarised from each PR's description** — not from the changelog
titles.

## Per-PR (during development)

Write a clear PR description of the user-visible change and its benefit (not the
implementation). That's the only ask — no special block, no label.

Optionally add a dedicated **`## Release notes`** section in the PR description to fix
the exact customer-facing wording for a subtle or high-impact change.

## Before the release

Audit log checks, in this order:

1. **`main` is green**, including the `check-audit-format` job of the latest push to `main`. That job checks `main` as merged, so it catches a pull request that passed against an older base. If it failed, fix it before anything else: on a branch from `main`, run `just update-audit-fixtures` and `just update-audit-schema`, add the fragment the error asks for, and merge that as a pull request.
2. **Merge any open `sync-enterprise-docs` pull request as it is.** Lakekeeper Plus opens it. It carries `docs/docs/audit/schema-plus.json`, `docs/docs/api/management-open-api-plus.yaml` and `site/docs/about/enterprise-release-notes.md`; never edit those files here.
3. **The version is pinned when it has to be.** `release-please/release-please-config.json` holds `"release-as"` only when a release must have a version release-please would not pick.

One-time repository setting: require branches to be up to date before merging (or use a merge queue) on `main` and `rel-*`. Without it a pull request can merge with a check result computed against an older `main`; the push run then fails after the fact.

## At release

release-please maintains `CHANGELOG.md` and, on a merged release PR, cuts a draft GitHub Release + tag (`vX.Y.Z`). The `publish-binary` job publishes it automatically once the binaries are built, with release-please's body; step 6 replaces that body.

```bash
VERSION=0.12.3        # the release just cut, without the leading v
TAG="v$VERSION"
NOTES=site/docs/about/release-notes.md
git switch main && git pull && git fetch --tags
git switch -c "release-notes-$VERSION"   # every change below goes on this branch
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
   Leave audit log format changes out of these lines: step 4 adds them.
4. **Fold in the audit log format changes.** `AUDIT_FORMAT` is derived, so nothing needs bumping. The new tag is the new baseline.

   ```bash
   just audit-format-release-notes "$VERSION"   # prints the block; changes nothing
   ```

   The command reads the tag and lists the fragments the tag carries that the release before it did not carry with the same text. It fails when the version moved but no fragment says why. It prints either `_No audit log format changes in this release._`, or a block starting with `#### Audit log format`:

   - **No changes:** paste nothing, and skip `just audit-format-release`.
   - **The block has a "Breaking changes" list:** paste it unchanged as the last part of `### Breaking Changes` in the `## $TAG` section. Create that heading if the section has none.
   - **Otherwise:** paste it unchanged as the last part of `### Upgrade Notes`, created the same way.

   Then clear the fragments:

   ```bash
   just audit-format-release "$VERSION"
   ```

   It refuses, and changes nothing, until the first line of every fragment appears in the `## $TAG` section of `$NOTES`: a fragment's text exists nowhere else. It then deletes the fragments. When the format moved, it also prints a row such as `| 0.14.0 | \`1.0\` |`: add it at the bottom of the table under "Which release ships which version" in `docs/docs/logging.md`. When it prints no row, the table needs none.

   The first release with an audit format, 0.14.0, ships `1.0` whatever its fragments say: there was no earlier version to raise. Its block still lists every change, measured against what 0.13 emitted, and its last line says "(the first version)".

5. **Open one pull request to `main`** with `$NOTES`, the deleted `audit-format/unreleased/*.md` fragments and the table row. `check-audit-format` must pass on it, with no warning about shipped fragments. Do **not** edit the release-please PR — release-please force-regenerates that branch on every push to `main` and would clobber the change.
6. **Set the GitHub Release body** from the new section, once step 5 has merged:

   ```bash
   gh release edit "$TAG" --repo lakekeeper/lakekeeper \
     --notes-file <(awk -v t="## $TAG" 'index($0,t)==1{f=1;next} f&&/^## /{exit} f' "$NOTES")
   ```

7. **Snapshot the versioned docs** for a minor release (`X.Y.0`), once step 5 has merged. The docs site serves the highest `X.Y.x` folder on the `docs` branch as `latest`, so until this step `latest` is the previous minor. The folder is built from the tag, so it shows what the release emits, plus the table row from step 4:

   ```bash
   MINOR="${VERSION%.*}.x"                                  # e.g. 0.14.x
   git worktree add ../lakekeeper-docs origin/docs -b "docs-$MINOR"
   mkdir "../lakekeeper-docs/$MINOR"
   git archive "$TAG" docs/docs docs/hooks docs/mkdocs.yml | tar -x -C "../lakekeeper-docs/$MINOR" --strip-components=1
   git show main:docs/docs/logging.md > "../lakekeeper-docs/$MINOR/docs/logging.md"   # carries the new table row
   sed -i.bak -E "s|^(site_name:[[:space:]]+docs/).*|\1$MINOR|" "../lakekeeper-docs/$MINOR/mkdocs.yml" && rm "../lakekeeper-docs/$MINOR/mkdocs.yml.bak"
   ```

   Check that `logging.md` in the snapshot differs from the tag's only by the table row (`git diff --no-index`); if `main` changed it further since the tag, keep the tag's version and add the row by hand. Commit the folder to the `docs` branch through a pull request. In the same release-notes pull request or a follow-up, add `- Release X.Y.x: "!include versions/X.Y.x/mkdocs.yml"` above the previous release in the `versions` nav of `site/mkdocs.yml`. For a patch release, the folder stays as it is: a patch never changes the audit log format.

## After the release

1. **Remove `"release-as"`** from `release-please/release-please-config.json` on `main` in a pull request, if it was set. Left in place, the next release is cut under the same version again.
2. **Tell Lakekeeper Plus the tag commit.** A Plus release pins a Lakekeeper release (see its `docs/releasing.md`); it needs `git rev-parse "$TAG^{commit}"`.
3. From now on every pull request that changes what an audit record carries adds a fragment, and CI computes and checks the version. Nothing else is manual until the next release.

## Patch releases

A patch is released from a `rel-X-Y` branch cut at the `vX.Y.0` tag. That tag holds the `"release-as"` of the minor release, so before merging the branch's release-please pull request, set `"release-as"` to the patch version on the branch in a pull request (as `chore: Release-As 0.13.4` did on `rel-0-13`).

A patch never changes the audit log format: CI rejects a pull request to the branch that adds a fragment, moves `AUDIT_FORMAT` or changes a record. `just audit-format-release-notes "$VERSION"` prints `_No audit log format changes in this release._` for it, so a patch's notes carry no audit log block, `just audit-format-release` is not run, and the table gets no row.

## House style

Keep entries customer-facing and short — **one line per change, benefit first**. Inline
only the single most important setting (flag / env var); link everything else to the
docs. Add `### Highlights` only when 2-3 changes genuinely stand out. Omit empty
sections. Link the (public) PRs as Markdown links. Credit external contributors with
`(thanks @handle)`.

Sections, in order: **Highlights · Features · Bug Fixes · Breaking Changes · Upgrade
Notes**. The audit log block goes where step 4 says.

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
