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

## How release-please cuts a release

The `Release` workflow (`.github/workflows/release.yml`) runs release-please on every push to `main` and to `rel-*`. release-please keeps one pull request per branch open, from `release-please--branches--<branch>`, titled `chore(<branch>): release X.Y.Z`. Merging it tags `vX.Y.Z`, creates the GitHub Release and publishes binaries and images.

Three things decide what that pull request contains:

| Setting | Where | What it decides |
|---|---|---|
| `target-branch` | release-please step in `.github/workflows/release.yml` | The branch release-please reads and opens its pull request against. Without it, release-please uses `main`, whichever branch was pushed. |
| `"release-as"` | `release-please/release-please-config.json` | The version of the next release. This repository always sets it. Left at a released version, release-please proposes that version again. |
| `"."` | `release-please/.release-please-manifest.json` | The last release of the branch. Merging a release pull request updates it. |

release-please opens a pull request only once the branch has a commit since the last release outside the `exclude-paths` of `release-please/release-please-config.json` (`.github`, `.cargo`, `release-please`, `.gitignore`, `renovate.json`). Commits that only change those paths open none.

## Minor release (`X.Y.0` from `main`)

### Before the release

Check each point. If one fails, fix it first.

1. **`main` is green**, including the `check-audit-format` job of the latest push to `main`. That job checks `main` as merged, so it catches a pull request that passed against an older base. If it failed: on a branch from `main`, run `just update-audit-fixtures` and `just update-audit-schema`, add the fragment the error asks for, and merge that as a pull request.
2. **Any open `sync-enterprise-docs` pull request is merged as it is.** Lakekeeper Plus opens it. It carries `docs/docs/audit/schema-plus.json`, `docs/docs/api/management-open-api-plus.yaml` and `site/docs/about/enterprise-release-notes.md`; never edit those files here.
3. **The release pull request is correct:**

    ```bash
    git fetch origin
    gh pr list --base main --head release-please--branches--main   # one PR: chore(main): release X.Y.0
    git show origin/main:release-please/release-please-config.json | grep release-as   # "X.Y.0"
    git show origin/main:release-please/.release-please-manifest.json                   # the last minor release
    ```

    `target-branch` stays commented out on `main`.

If the release pull request is missing or wrong:

| What you see | Cause | Fix |
|---|---|---|
| No pull request | The `Release` workflow failed on the last push to `main`. | Check `gh run list --workflow release.yml --branch main -L 3` and re-run the failed run. |
| No pull request, and the `Release Please` job log says `No commits for path: ., skipping` | Every commit since the last release changes only excluded paths. | There is nothing to release. |
| The pull request has the wrong version | `"release-as"` on `main` is wrong. | Commit `chore: Release as X.Y.0` to `main`, changing only `"release-as"`. The push updates the pull request. |

### At release

Merge the release pull request. release-please tags `vX.Y.0` and creates a draft GitHub Release. The `publish-binary` job publishes it once the binaries are built, with release-please's text as the body; step 6 below replaces it.

Write the release notes on a branch from `main`:

```bash
VERSION=0.15.0        # the release just cut, without the leading v
TAG="v$VERSION"
NOTES=site/docs/about/release-notes.md
git switch main && git pull && git fetch --tags
git switch --no-track -c "release-notes-$VERSION" origin/main
```

1. **List the pull requests in this release** from the new `CHANGELOG.md` section:

    ```bash
    awk -v v="$VERSION" '
      $0 ~ "^## \\[" v "\\]" {f=1; next}
      f && /^## \[/ {exit}
      f' CHANGELOG.md | grep -oE '#[0-9]+' | tr -d '#' | sort -un
    ```

2. **Read each pull request's description and summarise it** (agent-assisted is fine; use a pull request's `## Release notes` section as the starting point when it has one). Follow [House style](#house-style) and [What to leave out](#what-to-leave-out--collapse).

    ```bash
    gh pr view <N> --repo lakekeeper/lakekeeper --json title,body
    ```

3. **Add the `## $TAG (date)` section** at the top of `$NOTES` (newest first): group into Highlights / Features / Bug Fixes / Breaking Changes / Upgrade Notes; one line per item; link the pull requests as `[#NNNN](https://github.com/lakekeeper/lakekeeper/pull/NNNN)`. Leave audit log format changes out of these lines: the next step adds them.
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

5. **Open one pull request to `main`** with `$NOTES`, the deleted `audit-format/unreleased/*.md` fragments and the table row. `check-audit-format` must pass on it, with no warning about shipped fragments. Do **not** edit the release pull request: release-please regenerates that branch on every push to `main`.
6. **Set the GitHub Release body** from the new section, once the pull request has merged:

    ```bash
    gh release edit "$TAG" --repo lakekeeper/lakekeeper \
      --notes-file <(awk -v t="## $TAG" 'index($0,t)==1{f=1;next} f&&/^## /{exit} f' "$NOTES")
    ```

7. **Snapshot the versioned docs**, once the pull request has merged. The docs site serves the highest `X.Y.x` folder on the `docs` branch as `latest`, so until this step `latest` is the previous minor. The folder is built from the tag, so it shows what the release emits, plus the table row from step 4:

    ```bash
    VERSION=0.15.0        # the release just cut, without the leading v
    TAG="v$VERSION"
    MINOR="${VERSION%.*}.x"                                  # e.g. 0.15.x
    git fetch origin && git fetch --tags
    git worktree add ../lakekeeper-docs origin/docs -b "docs-$MINOR"
    mkdir "../lakekeeper-docs/$MINOR"
    git archive "$TAG" docs/docs docs/hooks docs/mkdocs.yml | tar -x -C "../lakekeeper-docs/$MINOR" --strip-components=1
    git show origin/main:docs/docs/logging.md > "../lakekeeper-docs/$MINOR/docs/logging.md"   # carries the new table row
    sed -i.bak -E "s|^(site_name:[[:space:]]+docs/).*|\1$MINOR|" "../lakekeeper-docs/$MINOR/mkdocs.yml" && rm "../lakekeeper-docs/$MINOR/mkdocs.yml.bak"
    ```

    Check that `logging.md` in the snapshot differs from the tag's only by the table row (`git diff --no-index`); if `main` changed it further since the tag, keep the tag's version and add the row by hand. Commit the folder to the `docs` branch through a pull request. Merging it deploys the site.

8. **Add the release line to the site menu**, once the `docs` pull request has merged: add `- Release X.Y.x: "!include versions/X.Y.x/mkdocs.yml"` above the previous release under `Docs:` in `site/mkdocs.yml`, in a pull request to `main`.

### After the release

1. **Pin the next minor on `main`.** Commit `chore: Release as X.(Y+1).0` to `main`, changing only `"release-as"`. Until then, the release pull request on `main` proposes `X.Y.0` again.
2. **Tell Lakekeeper Plus the tag commit.** A Plus release pins a Lakekeeper release (see its `docs/releasing.md`); it needs `git rev-parse "$TAG^{commit}"`.
3. **Note the release-notes pull request of step 5.** The first patch release of this line cherry-picks it.
4. From now on every pull request that changes what an audit record carries adds a fragment, and CI computes and checks the version. Nothing else is manual until the next release.

## Patch release (`X.Y.Z` from `rel-X-Y`)

A patch release of the `X.Y` line is cut from the `rel-X-Y` branch, never from `main`. `main` already carries changes for the next minor.

### Before the release

#### The branch is set up

Check it with:

```bash
B=rel-0-14            # the release branch
git fetch origin && git fetch --tags
git show "origin/$B:.github/workflows/release.yml" | grep target-branch                     # target-branch: rel-0-14
git show "origin/$B:release-please/release-please-config.json" | grep release-as           # "0.14.1", the patch to cut
git show "origin/$B:release-please/.release-please-manifest.json"                          # 0.14.0, the last release of the line
git log --oneline "$(git describe --tags --abbrev=0 "origin/$B")..origin/$B"               # only the setup commits and backports
```

**If the branch does not exist**, create it from the `vX.Y.0` tag. Never cut it from `main`: `main` has moved on since the tag, and the branch would carry changes of the next minor.

```bash
git switch -c rel-0-14 v0.14.0
```

Then make the setup commits on it, each changing only what it names:

1. `chore(ci): rel-0-14 target branch`: in `.github/workflows/release.yml`, replace the commented-out `target-branch` line of the release-please step with `target-branch: rel-0-14`.
2. `chore: Release as 0.14.1`: set `"release-as"` in `release-please/release-please-config.json` to the first patch. The tag still holds the minor version.
3. Cherry-pick the release-notes pull request of the `X.Y.0` release from `main` (for 0.14.0: #2096). It deletes the audit format fragments that `X.Y.0` shipped and adds its row to `docs/docs/logging.md`. Without it, `check-audit-format` on the branch warns about shipped fragments.

Push the branch with `git push -u origin rel-0-14`. The setup commits only change excluded paths, so release-please opens no pull request yet.

**If the branch was cut from `main`**, and nothing was released from it: recreate it from the tag as above and cherry-pick the backports again. Ask a maintainer before you delete or force-push a `rel-*` branch. If a release was already cut from it, ask a maintainer.

#### The backports are in

Cherry-pick each change from `main` with `git cherry-pick <sha>`. Backport:

- bug fixes for code that exists on the release line,
- dependency updates that fix a security advisory, and changes that keep CI working,
- docs changes that apply to the release line, together with the code they describe.

Never backport a breaking change. A patch never changes the audit log format: CI rejects a pull request to the branch that adds a fragment, moves `AUDIT_FORMAT` or changes a record.

The site serves the `X.Y.x` docs from the `docs` branch, so backported docs reach readers only when the folder is refreshed after the release. A push to `rel-X-Y` does not deploy the site. `rel-0-13` and `rel-0-14` were cut before that rule and still deploy the site from their own `.github/workflows/site.yml`: after a push there that changes `docs/` or `site/`, run `gh workflow run site.yml --ref main`.

#### The release pull request is correct

```bash
gh pr list --base "$B" --head "release-please--branches--$B"   # one PR: chore(rel-0-14): release 0.14.1
```

If it is missing or wrong:

| What you see | Cause | Fix |
|---|---|---|
| No pull request, although the branch has backports | `target-branch` is missing, so release-please read `main` and updated `main`'s pull request instead. | Make setup commit 1. Then check that `main`'s release pull request still has the right version. |
| No pull request, and the `Release Please` job log says `No commits for path: ., skipping` | Every commit since the last release changes only excluded paths. | Backport a fix first. A patch with no fix has nothing to release. |
| No pull request, and the `Release` workflow failed on the last push | | Check `gh run list --workflow release.yml --branch "$B" -L 3` and re-run the failed run. |
| The pull request has the last released version, or the next minor | `"release-as"` on the branch is stale. | Commit `chore: Release as X.Y.Z` to the branch, changing only `"release-as"`. |
| The pull request lists changes of the next minor | The branch was cut from `main`. | See "If the branch was cut from `main`" above. |

### At release

Merge the release pull request. release-please tags `vX.Y.Z` and creates a draft GitHub Release. The `publish-binary` job publishes it once the binaries are built, with release-please's text as the body; step 5 below replaces it.

The release notes live on `main`. Write them on a branch from `main`:

```bash
VERSION=0.14.1        # the release just cut, without the leading v
TAG="v$VERSION"
NOTES=site/docs/about/release-notes.md
git switch main && git pull && git fetch --tags
git switch --no-track -c "release-notes-$VERSION" origin/main
```

1. **List the pull requests in this release.** The `CHANGELOG.md` section is on the tag, not on `main`:

    ```bash
    git show "$TAG:CHANGELOG.md" | awk -v v="$VERSION" '
      $0 ~ "^## \\[" v "\\]" {f=1; next}
      f && /^## \[/ {exit}
      f' | grep -oE '#[0-9]+' | tr -d '#' | sort -un
    ```

2. **Read each pull request's description and summarise it** (agent-assisted is fine). Follow [House style](#house-style).

    ```bash
    gh pr view <N> --repo lakekeeper/lakekeeper --json title,body
    ```

3. **Add the `## $TAG (date)` section** to `$NOTES`. Keep the sections in version order: `v0.13.7` goes below `v0.14.0` and above `v0.13.6`. Group into Bug Fixes / Upgrade Notes, and the other sections only when needed; one line per item; link the pull requests as `[#NNNN](https://github.com/lakekeeper/lakekeeper/pull/NNNN)`.
4. **Confirm that the audit log format did not change:**

    ```bash
    just audit-format-release-notes "$VERSION"   # must print: _No audit log format changes in this release._
    ```

    A patch's notes carry no audit log block, `just audit-format-release` is not run, and the table in `docs/docs/logging.md` gets no row. If the command prints anything else, stop and ask a maintainer.

5. **Open a pull request to `main`** with `$NOTES`. Once it has merged, set the GitHub Release body from the new section:

    ```bash
    gh release edit "$TAG" --repo lakekeeper/lakekeeper \
      --notes-file <(awk -v t="## $TAG" 'index($0,t)==1{f=1;next} f&&/^## /{exit} f' "$NOTES")
    ```

6. **Refresh the versioned docs.** Replace the `X.Y.x` folder on the `docs` branch with the tag's docs, keeping the folder's `site_name`:

    ```bash
    VERSION=0.14.1        # the release just cut, without the leading v
    TAG="v$VERSION"
    MINOR="${VERSION%.*}.x"                                  # e.g. 0.14.x
    git fetch origin && git fetch --tags
    git worktree add ../lakekeeper-docs origin/docs -b "docs-$VERSION"
    rm -r "../lakekeeper-docs/$MINOR"
    mkdir "../lakekeeper-docs/$MINOR"
    git archive "$TAG" docs/docs docs/hooks docs/mkdocs.yml | tar -x -C "../lakekeeper-docs/$MINOR" --strip-components=1
    sed -i.bak -E "s|^(site_name:[[:space:]]+docs/).*|\1$MINOR|" "../lakekeeper-docs/$MINOR/mkdocs.yml" && rm "../lakekeeper-docs/$MINOR/mkdocs.yml.bak"
    git -C ../lakekeeper-docs add -A "$MINOR" && git -C ../lakekeeper-docs diff --cached --stat
    ```

    The diff should show only the docs changes backported since the last refresh. Commit the folder to the `docs` branch through a pull request. Merging it deploys the site.

### After the release

1. **Pin the next patch on the branch.** Commit `chore: Release as X.Y.(Z+1)` to `rel-X-Y`, changing only `"release-as"`. It opens no pull request until the next backport.
2. **Tell Lakekeeper Plus the tag commit.** A Plus patch release pins a Lakekeeper patch release of the same minor (see its `docs/releasing.md`); it needs `git rev-parse "$TAG^{commit}"`.

One-time repository setting: require branches to be up to date before merging (or use a merge queue) on `main` and `rel-*`. Without it a pull request can merge with a check result computed against an older base; the push run then fails after the fact.

## House style

Many readers do not speak English as their first language. Write so that they can follow without a dictionary.

- **Full sentences in plain English.** Short sentences, common words, active voice. No idioms and no internal terms (crate, trait or function names; "single-flight", "seam", "read-through"), except in a bullet written for people who build on the crates. Name things as the docs and the API name them.
- **One bullet per change, benefit first.** A short bold label, then one to three sentences: what the user can do now, or what no longer goes wrong. Inline only the single most important setting (env var, field or endpoint).
- **Compact: link the docs, don't repeat them.** A new feature gets one bullet and a link to its docs section; the details live there. If a feature has no docs yet, write them before the release instead of putting the details here. Link with absolute, versioned URLs (`https://docs.lakekeeper.io/docs/X.Y.x/<page>/#anchor`): for a minor release, the `X.Y.x` folder only exists after the versioned docs snapshot, and the site link check skips absolute URLs, so check each page and anchor by hand.
- **Breaking changes and upgrade notes say what to do.** Name who is affected and the concrete action. Say who is not affected when that is most readers ("Prebuilt binaries and images are not affected").
- Add `### Highlights` only when 2-3 changes genuinely stand out. Omit empty sections. Link the (public) PRs as Markdown links. Credit external contributors with `(thanks @handle)`.

Sections, in order: **Highlights · Features · Bug Fixes · Breaking Changes · Upgrade Notes**. The audit log block of a minor release goes last.

## What to leave out / collapse

The `CHANGELOG.md` PR list is raw input, not the release notes. Curate it:

- **Minor releases: compare against the newest patch of the previous minor.** Readers assume that `X.Y.0` contains everything from the newest `X.(Y-1).z` that exists on release day (0.14.0 contains 0.13.6). release-please diffs against the last release on `main`, so its list re-includes every PR that was backported to a `rel-*` branch. Leave those PRs out, and describe a later change to such a feature as the change since that patch. Add the patch's own section too if it was never written up. List the backports with `git log $(git describe --tags --abbrev=0 origin/main)..<newest-patch-tag>` and match them to `main` by PR number: cherry-picks get new SHAs, and a few carry no PR number.
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
