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

Optionally add a dedicated **`## Release notes`** section in the PR description to propose
the customer-facing wording for a subtle or high-impact change.

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
   `## Release notes` section as the starting point when it has one):

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

   The command reads the tag and lists the fragments the tag carries that the release before it did not carry with the same text. It fails when the version moved but no fragment says why. It prints either `_No audit log format changes in this release._`, or a block starting with `### Audit log format`:

   - **No changes:** paste nothing, and skip `just audit-format-release`.
   - **Otherwise:** paste it unchanged as the last section of `## $TAG`, after `### Upgrade Notes`. If the block has a "Breaking changes" list, also add this item at the end of `### Breaking Changes`: `- **Audit log format.** Existing parsers of the audit log must be updated. See Audit log format below.`

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

   Check that `logging.md` in the snapshot differs from the tag's only by the table row (`git diff --no-index`); if `main` changed it further since the tag, keep the tag's version and add the row by hand. Commit the folder to the `docs` branch through a pull request. Once that pull request has merged, add `- Release X.Y.x: "!include versions/X.Y.x/mkdocs.yml"` above the previous release under `Docs:` in `site/mkdocs.yml`, in a pull request to `main`. For a patch release, the folder stays as it is: a patch never changes the audit log format.

## After the release

1. **Remove `"release-as"`** from `release-please/release-please-config.json` in a pull request, if it was set: on `main` after a minor release, on the `rel-*` branch after a patch release. Left in place, the next release is cut under the same version again.
2. **Tell Lakekeeper Plus the tag commit.** A Plus release pins a Lakekeeper release (see its `docs/releasing.md`); it needs `git rev-parse "$TAG^{commit}"`.
3. From now on every pull request that changes what an audit record carries adds a fragment, and CI computes and checks the version. Nothing else is manual until the next release.

## Patch releases

A patch is released from a `rel-X-Y` branch cut at the `vX.Y.0` tag. That tag holds the `"release-as"` of the minor release, so before merging the branch's release-please pull request, set `"release-as"` to the patch version on the branch in a pull request (as `chore: Release-As 0.13.4` did on `rel-0-13`).

A patch never changes the audit log format: CI rejects a pull request to the branch that adds a fragment, moves `AUDIT_FORMAT` or changes a record. `just audit-format-release-notes "$VERSION"` prints `_No audit log format changes in this release._` for it, so a patch's notes carry no audit log block, `just audit-format-release` is not run, and the table gets no row.

## House style

Many readers do not speak English as their first language. Write so that they can follow without a dictionary.

- **Full sentences in plain English.** Short sentences, common words, active voice. No idioms and no internal terms (crate, trait or function names; "single-flight", "seam", "read-through"), except in a bullet written for people who build on the crates. Name things as the docs and the API name them.
- **One bullet per change, benefit first.** A short bold label, then one to three sentences: what the user can do now, or what no longer goes wrong. Inline only the single most important setting (env var, field or endpoint).
- **Compact: link the docs, don't repeat them.** A new feature gets one bullet and a link to its docs section; the details live there. If a feature has no docs yet, write them before the release instead of putting the details here. Link with absolute, versioned URLs (`https://docs.lakekeeper.io/docs/X.Y.x/<page>/#anchor`): for a minor release, the `X.Y.x` folder only exists after step 7, and the site link check skips absolute URLs, so check each page and anchor by hand.
- **Breaking changes and upgrade notes say what to do.** Name who is affected and the concrete action. Say who is not affected when that is most readers ("Prebuilt binaries and images are not affected").
- Add `### Highlights` only when 2-3 changes genuinely stand out. Omit empty sections. Link the (public) PRs as Markdown links. Credit external contributors with `(thanks @handle)`.

Sections, in order: **Highlights · Features · Bug Fixes · Breaking Changes · Upgrade Notes**. The audit log block goes where step 4 says.

## What to leave out / collapse

The `CHANGELOG.md` PR list is raw input, not the release notes. Curate it:

- **Compare against the newest patch of the previous minor.** Readers assume that `X.Y.0` contains everything from the newest `X.(Y-1).z` that exists on release day (0.14.0 contains 0.13.6). release-please diffs against the last release on `main`, so its list re-includes every PR that was backported to a `rel-*` branch. Leave those PRs out, and describe a later change to such a feature as the change since that patch. Add the patch's own section too if it was never written up. List the backports with `git log $(git describe --tags --abbrev=0 origin/main)..<newest-patch-tag>` and match them to `main` by PR number: cherry-picks get new SHAs, and a few carry no PR number.
- **Describe the final state once.** A feature that was added and then changed or fixed several times in the same release gets one bullet that describes how it works at release, citing all its PRs. Leave out states that never shipped.
- **Don't list a bug fix for code first introduced in the same release.** If the feature and its follow-up fix both land in this version, the bug never shipped: fold the fix into the feature or drop it. Check with `git show <newest-patch-tag>:<path>`: if the fixed code or route didn't exist there, it's a same-release fix. A fix is listable only when the bug itself is present at `<newest-patch-tag>`; code that existed there may have broken on `main` later.
- **One line per feature, even when it spanned several PRs.** Backend + management API + console PRs for the same capability are a single entry citing all the PRs together.
- **Highlight what matters to OSS users.** The OSS authorizer is OpenFGA; changes that only affect the built-in/internal authorization store are not OSS highlights (OpenFGA already covers most of that ground): keep them to a modest Features line or omit them. Changes that only matter under Cedar belong in the Lakekeeper+ notes.
- **Check PR descriptions against the merged code.** Descriptions go stale during review: endpoint names, defaults and status codes change. Read the squash commit and its diff. release-please's breaking-changes list is also incomplete (it misses footers such as `BREAKING CHANGE (OpenFGA authorizer only):`), so also run `git log <newest-patch-tag>..<tag> --grep 'BREAKING CHANGE'`.

## Notes

- No CI generation, no API key, no `git-cliff`. Summarising is a manual/agent-assisted
  pass at release; clear PR descriptions are what make it easy.
- The page is published on the docs site and its sections are reused by Lakekeeper
  Enterprise's upstream-changes rollup — keep them customer-facing and accurate.
- If this becomes a bottleneck (much higher PR volume, or notes get skipped), graduate
  to changelog fragments (`changie`/towncrier or a homegrown assemble step).
