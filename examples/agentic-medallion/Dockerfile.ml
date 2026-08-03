# Workbench image: the JupyterLab kernel that runs every notebook — the human
# operator's setup + grants, the medallion pipeline (Silver vision extraction,
# Gold CLIP embeddings) and the retrieval agent.
#
# Built under the `ml` profile only; it carries the heavy ML deps (torch,
# open_clip) the pipeline and CLIP agent need.
FROM python:3.11-slim

# open_clip / torchvision image decoding needs libjpeg + libpng at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjpeg62-turbo libpng16-16 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

COPY requirements-spike.txt requirements-ml.txt ./
RUN pip install --no-cache-dir -r requirements-spike.txt -r requirements-ml.txt

# Quiet the HF Hub "unauthenticated request" notice (anonymous downloads are fine
# for these public weights). Set HF_TOKEN at runtime if you hit rate limits.
ENV HF_HUB_VERBOSITY=error

# Pre-download the CLIP weights (open_clip ViT-B/32, laion2b — ~600 MB) at BUILD
# time so the first Gold/agent run doesn't stall on a large HuggingFace fetch. Keep
# the model id + pretrained tag in sync with clip_util.py (MODEL_NAME / PRETRAINED).
RUN python -c "import open_clip; open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k')"

# Source is bind-mounted at /work by docker-compose.
