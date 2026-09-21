"""Chat and embeddings behind one small interface, so the provider is a config choice.

Airgapped by default: Ollama, in this compose stack, with no API key anywhere. Point it
at Claude or any OpenAI-compatible endpoint (DeepSeek, vLLM, Together, a local
llama.cpp server) by setting env — nothing else changes.

    LLM_PROVIDER=ollama                      CHAT_MODEL=qwen2.5:3b     # default
    LLM_PROVIDER=anthropic  LLM_API_KEY=...  CHAT_MODEL=claude-sonnet-5
    LLM_PROVIDER=openai     LLM_API_KEY=...  CHAT_MODEL=deepseek-chat \\
                            LLM_BASE_URL=https://api.deepseek.com/v1

**The governance story does not depend on any of this.** The catalog decides access at
credential vending, before a model is involved at all, so swapping providers changes
nothing about what an agent may read. That is worth noticing: it is the opposite of a
memory service welded to one vendor's runtime.

One asymmetry to respect: the **chat** model can change freely, the **embedding** model
cannot. Vectors from different models are not comparable, so `MemoryStore` records the
model on the recall table at creation and refuses a mismatch — see `EmbeddingMismatch`.
"""

from __future__ import annotations

import os

import requests

PROVIDER = os.environ.get("LLM_PROVIDER", "ollama").lower()
CHAT_MODEL = os.environ.get("CHAT_MODEL", "qwen2.5:3b")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL") or ""
LLM_API_KEY = os.environ.get("LLM_API_KEY") or ""

TIMEOUT = 180


# --------------------------------------------------------------------------- chat


def chat(prompt: str, *, system: str | None = None, max_tokens: int = 400) -> str:
    """One turn of prose from the configured provider."""
    if PROVIDER == "ollama":
        return _ollama_chat(prompt, system, max_tokens)
    if PROVIDER == "anthropic":
        return _anthropic_chat(prompt, system, max_tokens)
    if PROVIDER in ("openai", "openai-compatible"):
        return _openai_chat(prompt, system, max_tokens)
    raise ValueError(f"unknown LLM_PROVIDER {PROVIDER!r} (ollama | anthropic | openai)")


def _ollama_chat(prompt: str, system: str | None, max_tokens: int) -> str:
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    r = requests.post(
        f"{OLLAMA_URL}/api/chat",
        json={
            "model": CHAT_MODEL,
            "messages": messages,
            "stream": False,
            "options": {"num_predict": max_tokens, "temperature": 0},
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return str(r.json()["message"]["content"]).strip()


def _anthropic_chat(prompt: str, system: str | None, max_tokens: int) -> str:
    body: dict = {
        "model": CHAT_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system
    r = requests.post(
        f"{LLM_BASE_URL or 'https://api.anthropic.com'}/v1/messages",
        headers={
            "x-api-key": LLM_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json=body,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return "".join(
        block.get("text", "") for block in r.json().get("content", [])
    ).strip()


def _openai_chat(prompt: str, system: str | None, max_tokens: int) -> str:
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    r = requests.post(
        f"{LLM_BASE_URL or 'https://api.openai.com/v1'}/chat/completions",
        headers={"Authorization": f"Bearer {LLM_API_KEY}"},
        json={
            "model": CHAT_MODEL,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0,
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return str(r.json()["choices"][0]["message"]["content"]).strip()


# --------------------------------------------------------------------- embeddings


class Embedder:
    """Turns text into vectors and names the model it used.

    Satisfies `pylakekeeper.agents.Embedder`. The `model` attribute is not decoration:
    it is recorded on the recall table at creation and checked on every use, so swapping
    embedders fails loudly instead of silently invalidating every stored vector.
    """

    def __init__(self, model: str | None = None) -> None:
        self.model = model or EMBED_MODEL

    def embed(self, texts):  # noqa: ANN001, ANN201 - duck-typed against the protocol
        return [_unit(self._one(t)) for t in texts]

    def _one(self, text: str) -> list[float]:
        if PROVIDER == "ollama":
            r = requests.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": self.model, "prompt": text},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            return [float(v) for v in r.json()["embedding"]]

        # Anthropic serves no embedding endpoint, so an OpenAI-compatible one is used
        # for both remote providers. Set LLM_BASE_URL/LLM_API_KEY accordingly.
        r = requests.post(
            f"{LLM_BASE_URL or 'https://api.openai.com/v1'}/embeddings",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json={"model": self.model, "input": text},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return [float(v) for v in r.json()["data"][0]["embedding"]]


def _unit(vector: list[float]) -> list[float]:
    """Scale to unit length.

    Lance ranks by L2 distance. On unit vectors that is monotonic with cosine distance
    and lands in [0, 2], so a printed score reads as a similarity. Raw `nomic-embed-text`
    vectors are unnormalised and produce distances in the hundreds, which look like a bug
    in a notebook even though the ranking is identical.
    """
    norm = sum(v * v for v in vector) ** 0.5
    return [v / norm for v in vector] if norm else vector


def describe() -> str:
    """One line naming what is actually answering, for the notebooks to print."""
    where = {"ollama": OLLAMA_URL, "anthropic": "api.anthropic.com"}.get(
        PROVIDER, LLM_BASE_URL or "openai-compatible endpoint"
    )
    return f"{PROVIDER}: chat={CHAT_MODEL}, embed={EMBED_MODEL} via {where}"
