# Governed multimodal medallion for AI agents

**Thesis:** Lakekeeper governs a mixed-format medallion — Apache Iceberg *and*
Lance — under one catalog and one authorization model, and it enforces AI-agent
data access **at the credential-vending layer**. A denied agent receives no
storage credentials for that dataset, so it *physically cannot read those
objects*. The wall is in the catalog, not in application code you have to trust.

This example builds a small lakehouse from open-access museum images, then points
two AI agents at the Gold layer to show governance in action. You drive it from
**three Jupyter notebooks**, and a *human operator logs in interactively* (OAuth2
device code) to hand out access — no admin secret in sight.

```
 Open-access images (Met Museum, CC0)
        │  upload image OBJECTS via vended credentials
        ▼
 ┌─ RAW ── S3 objects (generic table) ┐   <id>.jpg objects — the governed landing zone
 │  namespace: raw                    │
 └───────────────┬─────────────────────┘
                 │  metadata + s3:// link to each object
                 ▼
 ┌─ BRONZE ── Iceberg ─────────────┐   clean metadata + image_uri (s3://…)
 │  namespace: bronze              │
 └───────────────┬──────────────────┘
                 │  vision LLM (Ollama): caption the image (object read via its s3:// link)
                 ▼
 ┌─ SILVER ── Iceberg ─────────────┐   structured attributes extracted from images
 │  namespace: silver              │
 └───────────────┬──────────────────┘
                 │  CLIP embeddings (open_clip, local; object read via its s3:// link)
                 ▼
 ┌─ GOLD ──── Lance (GENERIC TABLE) ┐   image embeddings for semantic search
 │  namespace: gold                 │   ← the asset the agents want
 └───────────────┬──────────────────┘
                 │  pylakekeeper: load(vended=True)
        ┌────────┴─────────┐
        ▼                  ▼
   analyst-agent      contractor-agent
   read on gold       read on raw+bronze+silver, DENIED gold
   → gets STS creds   → can read source data, blind to the embeddings
   → RAG answers      → DENIED at cred vending for Gold
```

Everything is Iceberg + Lance under **one** Lakekeeper catalog, governed by
**one** OpenFGA model. The governance boundary runs *through* the medallion,
per layer.

## The medallion, stage by stage

Each stage is a namespace in the one warehouse, written by the `PIPELINE` service
account through Lakekeeper-vended credentials. What actually happens:

| Stage | What it is | What happens | Governs via |
|---|---|---|---|
| **Raw** — `raw.images` | Generic table, `format="dataset"` — a *governed S3 location*, not a typed table | The source images are uploaded as plain **objects** (`<location>/<id>.jpg`). Lakekeeper catalogs the location and vends scoped creds; the pipeline writes the bytes with `pyarrow.fs`. The untouched landing zone. | vended STS creds on the raw location |
| **Bronze** — `bronze.artworks` | Iceberg table | The clean, queryable **metadata** (title, artist, department, theme, …) **plus `image_uri`** — the `s3://` link to each raw object. No pixels here; just a catalog of what landed. | Iceberg `select` |
| **Silver** — `silver.artwork_features` | Iceberg table | A local **vision LLM** (Ollama `moondream`) reads each image back via its `image_uri` and writes a one-line **caption** — an attribute extracted from the pixels that the source metadata never had. | Iceberg `select` |
| **Gold** — `gold.image_embeddings` | Lance generic table, `format="lance"` | Local **CLIP** embeds each image into a 512-d **vector**, written as a Lance dataset (vector + title/theme/caption/`image_uri`). This is what makes semantic search possible — the asset the agents want. | `lakekeeper_generic_table` `can_read_data` |
| **Agents** | `analyst` / `contractor` | Both run the identical `search_gallery` tool: CLIP-embed the query → ask Lakekeeper to **vend** Gold creds → vector-search Lance → an LLM writes the answer. Only the *grant* differs. | credential vending |

The single asymmetry: `analyst` is granted `select` on **gold**; `contractor` is
granted `select` on **raw + bronze + silver** but **not gold**. So the contractor
is a legitimate collaborator that can read the source images, metadata and
captions, yet Lakekeeper refuses to vend it credentials for the Gold embeddings —
it gets a 404 and physically cannot read them.

## Two tiers of identity — and why

| Principal | Auth | Role |
|---|---|---|
| **peter** | Device code (interactive human login) | Platform admin: bootstraps, builds the warehouse, grants everyone's access |
| **PIPELINE** | Client credentials (service account) | Autonomous ETL job that writes Raw/Bronze/Silver/Gold |
| **analyst-agent** | Client credentials (service account) | AI agent — granted read on **Gold** |
| **contractor-agent** | Client credentials (service account) | AI agent — read on **Raw/Bronze/Silver**, **denied Gold** |

A *person* uses the interactive **device-authorization grant** (RFC 8628): the
notebook prints a URL + code, you approve it in your browser, done — no secret in
the notebook. *Autonomous agents* have no human in the loop, so they use the
**client-credentials** grant. That's the correct split, and it's the point: the
governance lever peter pulls is one grant, and the agents inherit exactly the
access they were given.

## Why this shape

- **Images make Lance necessary.** Vector search over image embeddings is what
  Lance is for — it kills the "why not just use Iceberg for Gold too?" question.
- **The medallion spans three storage shapes, one catalog.** Raw is a governed
  object dataset (`format="dataset"`), Bronze/Silver are Iceberg, Gold is a Lance
  *generic table* — all in one warehouse under the same authz.
- **The governance payoff is real, not theater.** Generic tables vend scoped STS
  credentials and have a full per-principal OpenFGA type
  (`lakekeeper_generic_table`, action `can_read_data`). The contractor's
  `load(vended=True)` on Gold fails the authz check and returns no credentials —
  while the *same* contractor reads Raw/Bronze/Silver fine. The wall is at Gold.
- **Scope of the guarantee.** The control is *credentialed access to the stored Gold
  dataset*. This demo also grants the contractor the raw images and ships the CLIP
  model in the workbench, so a determined contractor could re-embed the raw images
  itself — governance controls access to the *precomputed* asset, not the ability to
  re-derive it from inputs a principal is allowed to see. (Deny `raw` too if that
  matters for your case.)

## Requirements

This example is **heavy** — it runs a local vision LLM, a local chat LLM, and CPU
PyTorch. Budget accordingly:

- **Docker or Podman** with Compose v2.
- **Disk: ~12 GB free.** The first `./up.sh` pulls several images (Ollama,
  Keycloak, Postgres ×2, aws-cli, Lakekeeper) **and builds the workbench image**
  (CPU `torch` + `open_clip` ≈ 3 GB, plus the CLIP weights ≈ 0.6 GB baked in so the
  agent never downloads them at runtime), then downloads two small local models:
  `moondream` ≈ 1.7 GB and `gemma2:2b` ≈ 1.6 GB.
- **RAM: ~8 GB** allocated to your Docker/Podman VM (catalog stack + CLIP + the
  small `gemma2:2b`). A larger chat model (e.g. `qwen2.5:7b`) needs ~5–6 GB more.
- **First run is slow** (image build + model pulls dominate); later runs are cached
  and fast.
- **Dataset:** the build notebook downloads **~40** small CC0 Met Museum thumbnails
  (a few MB total) and uploads them as **objects** to the governed `raw.images`
  dataset (Bronze links each by `s3://` URI) — no local `data/` folder. Change `N`
  in the ingest cell to adjust.

**Model size.** The chat model only writes the prose answer — retrieval (CLIP) and
the governance demo don't use it — so it's **small by default** (`gemma2:2b`, ~1.6 GB),
and the agent still works (falls back to listing the retrieved artworks) if it's
missing. For richer summaries, bump it via env:

```bash
CHAT_MODEL=qwen2.5:7b ./up.sh                       # ~4.7 GB, nicer prose
docker compose exec ollama ollama pull qwen2.5:7b
```

`VISION_MODEL` is overridable the same way. Override the catalog image with
`LAKEKEEPER_IMAGE=...` if you need a specific build.

## Quick start

**`./up.sh` is the single entry point** — it brings the whole stack up (you do
*not* also run `docker compose up`). Every notebook runs inside the docker network
(the `workbench` JupyterLab kernel), so vended credentials resolve and the
interactive login reaches Keycloak.

```bash
cd examples/agentic-medallion

# 1. Bring up everything: Lakekeeper, Postgres, OpenFGA, Keycloak, SeaweedFS,
#    JupyterLab (workbench) and Ollama. The FIRST run builds the workbench image
#    (torch/open_clip) — a few minutes. up.sh detects the browser/LAN host for you.
./up.sh                 # localhost — Docker Desktop / podman machine on your laptop
# ./up.sh 10.0.0.5      # remote Docker host: pass the IP/DNS your browser uses

# 2. Once it's up, pull the two small local models (vision for Silver, chat for the agent):
docker compose exec ollama ollama pull moondream
docker compose exec ollama ollama pull gemma2:2b
```

**3. Open JupyterLab → http://localhost:8888/lab/tree/notebooks** and work through `notebooks/` in
order — see [**Running the notebooks**](#running-the-notebooks) below for the
step-by-step (including the browser login in `00-setup`). The Lakekeeper console
is at http://localhost:8181. (`up.sh` prints these URLs when it finishes.)

> **Why `up.sh` and not just `docker compose up`?** Two things need a host-facing
> address that compose can't know:
>
> - **Device-code login** — peter approves a Keycloak URL in his *host* browser, so
>   `up.sh` sets `KEYCLOAK_BROWSER_URL` (defaults to `localhost:30080`).
> - **The S3 data plane** — the warehouse endpoint is signed into every request and
>   vended to clients, so it must be one URL that resolves from *both* the in-network
>   kernel and a host browser. `up.sh` detects the host **LAN IP** and sets
>   `S3_ENDPOINT` to it, and applies bucket CORS — so the notebooks read data *and*
>   the Lakekeeper UI can navigate the dataset files from your browser.
>
> Plain `docker compose --profile ml up -d --build` still works for the in-network
> notebook flow (endpoint defaults to `seaweedfs:8333`), but the UI can't reach data
> files and there's no interactive host override.

## Running the notebooks

Open **http://localhost:8888/lab/tree/notebooks** and work through the three notebooks in order,
running each cell top to bottom. The manual, human-in-the-loop steps are called
out below — the rest is "run the cell and read the output".

### 1 · `00-setup.ipynb` — governance setup (has an interactive login)

Run the cells top to bottom. One cell needs you:

- **The device-login cell** (`peter = mlib.device_login()`) **blocks** and prints
  a URL + code plus a clickable **Approve the login** button. Open it in your
  browser, sign in as **peter / `iceberg`**, and approve. The cell then unblocks
  and prints `logged in as peter`.
  *(Why a browser: the kernel runs in-network and can't show you a login screen;
  `up.sh` makes the printed URL host-reachable — see the note above.)*

The remaining cells run with no input and each print a status line: bootstrap
Lakekeeper, create the warehouse + `raw`/`bronze`/`silver`/`gold` namespaces,
provision the three service accounts, and apply the **per-layer grants** (analyst
→ gold; contractor → raw + bronze + silver, *not* gold). Those grants are the
whole point.

### 2 · `01-build-medallion.ipynb` — build the data (needs the two models)

Make sure you pulled `moondream` and `gemma2:2b` (step 2 of Quick start) first,
then run top to bottom. These are the slow cells:

- **Ingest → `raw.images`** — downloads ~40 CC0 Met images and uploads them as **objects**
  to the governed `raw.images` location (vended creds) — no local files
- **Bronze** — writes the clean metadata to Iceberg, including an `image_uri` (`s3://…`)
  link to each raw object
- **Silver** — the local vision model captions each image, reading the object back via its
  `s3://` link (one call per image, so it takes a few minutes)
- **Gold** — CLIP embeds each image (object read via its link; the **first** run downloads
  the CLIP weights unless they're baked into the image) and writes the vectors to the Lance
  dataset via vended credentials

The last cell reports how many embeddings were written.

### 3 · `02-agent-governed.ipynb` — governance in action

Run top to bottom and watch the two agents run the **identical** `search_gallery`
tool, differing only in their grant:

- the **analyst** cell → **ALLOWED**: creds vended, retrieves artworks, writes a
  RAG answer
- the **contractor** cell → **DENIED**: Lakekeeper returns **404 "no such table"**
  (it won't even admit Gold exists) — no creds are vended, so the data is unreachable
- the **source-layers cell** → shows the contractor *can* read `raw.images` and the
  Silver captions — proof the wall is at **Gold specifically**, not a blanket lockout
- the **chat widget** → type any query and ask *both* agents at once; the analyst
  returns artworks + thumbnails + a written answer, the contractor stays denied
- *(optional)* flip the lever live — grant the contractor `select` on Gold **in the
  Lakekeeper console** (warehouse `medallion` → namespace `gold` → Permissions →
  `service-account-contractor-agent`), or via the code cell; re-run the contractor
  cell and it now succeeds

The 404-not-403 is deliberate: Lakekeeper doesn't leak the existence of resources a
principal can't see.

### Seeing it in the Lakekeeper UI (optional)

Open the console at **http://localhost:8181**. You can browse the warehouse,
namespaces, tables, and the grants — and because `up.sh` set the S3 endpoint to
your LAN IP + applied CORS, you can navigate into the Iceberg dataset files too.

Model quality note: retrieval uses **CLIP image embeddings**, so search quality
holds regardless of the chat LLM. The vision captions (Silver) only feed the
agent's written answer — the tiny default `moondream` produces rough captions;
swap `VISION_MODEL=llama3.2-vision` (or `qwen2.5vl`) for much better ones.

## Reset / clean up

`./down.sh` tears the stack down and returns the example to a clean pre-run state
(fresh catalog + storage, generated `data/`, `.env`, checkpoints removed) so the
next `./up.sh` starts from scratch. It **keeps** the downloaded Ollama models by
default; add `--purge` to drop those too.

```bash
./down.sh            # reset, keep the ~6 GB of models
./down.sh --purge    # reset everything, including the models
```

## If your IP changes (laptop / Wi-Fi / hotspot)

`up.sh` bakes your host LAN IP into the warehouse's S3 endpoint (so a browser and
the in-network kernel share one endpoint). When your machine's IP changes — Wi-Fi
switch, phone hotspot, sleep/wake — that baked address goes dead and the notebooks
fail with `408` / `Connection refused` / `ShortTermCredentialError`.

Fix it **without a teardown** — re-point the warehouse at your current IP and keep
all your data:

```bash
./refresh-ip.sh      # detects the current IP, updates the warehouse endpoint in place
```

Then just re-run the failing cell. (It uses the `PIPELINE` account's `modify` right,
so no login prompt.) A full `./down.sh && ./up.sh` also works but rebuilds everything.

## Layout

```
up.sh                   bring the stack up (detects browser/LAN host, applies CORS)
refresh-ip.sh           re-point the warehouse at your current LAN IP (no teardown) after a network change
down.sh                 tear the stack down and reset to a clean pre-run state
docker-compose.yaml     stack: catalog + JupyterLab (workbench) + Ollama (ml profile)
Dockerfile.ml           the workbench image (JupyterLab + torch/open_clip/pyiceberg)
requirements-ml.txt     heavy deps (torch/open_clip/pyiceberg/lance/jupyterlab)
requirements-spike.txt  light deps (pylakekeeper/lance/requests) installed on top
keycloak/realm.json     realm: public `lakekeeper` client (device grant) + service accounts
seaweedfs-iam.json      SeaweedFS IAM + STS config (enables credential vending)
mlib.py                 config + device-code login, service-account tokens, grant helpers
icehelp.py              PyIceberg RestCatalog pointed at Lakekeeper (Bronze/Silver)
gt.py                   generic-table helpers: raw image OBJECTS + Gold Lance dataset (vended creds)
clip_util.py            local CLIP image/text embeddings
notebooks/00-setup.ipynb           peter (device code) governs the catalog
notebooks/01-build-medallion.ipynb raw objects → Bronze (s3:// links) → Silver → Gold (PIPELINE)
notebooks/02-agent-governed.ipynb  analyst vs contractor; governance in action
```

## Notes

- **Not for external compute.** Like the other Lakekeeper examples, the S3
  endpoint is only reachable inside the docker network. Run all clients (and the
  notebooks) via the in-network `workbench` kernel.
- **Demo secrets only.** The Keycloak client secrets in `keycloak/realm.json` and
  the storage keys are throwaway values — never reuse them.
