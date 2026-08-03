"""Local CLIP (open_clip, CPU) for cross-modal image/text embeddings.

Only imported inside the `workbench` (ML) image — never from the light `app`
container. Vectors are L2-normalized so Lance's default L2 nearest-neighbour
search ranks by cosine similarity.
"""
from __future__ import annotations

import functools
import io
from typing import Sequence

import numpy as np
import torch
from PIL import Image

MODEL_NAME = "ViT-B-32"
PRETRAINED = "laion2b_s34b_b79k"  # 512-dim embeddings (matches mlib.EMB_DIM)


@functools.lru_cache(maxsize=1)
def _load():
    import open_clip

    device = "cpu"
    model, _, preprocess = open_clip.create_model_and_transforms(
        MODEL_NAME, pretrained=PRETRAINED
    )
    model.eval().to(device)
    tokenizer = open_clip.get_tokenizer(MODEL_NAME)
    return model, preprocess, tokenizer, device


def _normalize(t: torch.Tensor) -> np.ndarray:
    t = t / t.norm(dim=-1, keepdim=True)
    return t.cpu().numpy().astype("float32")


def embed_images(paths: Sequence[str]) -> np.ndarray:
    model, preprocess, _, device = _load()
    batch = torch.stack([preprocess(Image.open(p).convert("RGB")) for p in paths]).to(device)
    with torch.no_grad():
        return _normalize(model.encode_image(batch))


def embed_images_bytes(images: Sequence[bytes]) -> np.ndarray:
    """Embed images given their raw bytes (e.g. read from the `raw.images` table)."""
    model, preprocess, _, device = _load()
    batch = torch.stack(
        [preprocess(Image.open(io.BytesIO(b)).convert("RGB")) for b in images]
    ).to(device)
    with torch.no_grad():
        return _normalize(model.encode_image(batch))


def embed_text(queries: Sequence[str]) -> np.ndarray:
    model, _, tokenizer, device = _load()
    tokens = tokenizer(list(queries)).to(device)
    with torch.no_grad():
        return _normalize(model.encode_text(tokens))
