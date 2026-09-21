"""Chat and embeddings behind one small interface, so the provider is a config choice.

Airgapped by default: Ollama, in this compose stack, with no API key anywhere. Point it
at Claude or any OpenAI-compatible endpoint (DeepSeek, vLLM, Together, a local
llama.cpp server) by setting env — nothing else changes.

    LLM_PROVIDER=ollama                      CHAT_MODEL=qwen2.5:3b     # default
    LLM_PROVIDER=openai     LLM_API_KEY=...  CHAT_MODEL=deepseek-chat \\
                            LLM_BASE_URL=https://api.deepseek.com/v1

Chat and embeddings are configured separately, because they are not always the same
service. `LLM_*` is the chat endpoint; `EMBED_*` is the embedding one. They only default
to each other where one service genuinely serves both APIs — an OpenAI-compatible host.
Anthropic serves no embedding endpoint at all, so it has to be told:

    LLM_PROVIDER=anthropic  LLM_API_KEY=sk-ant-...  CHAT_MODEL=claude-sonnet-5 \\
      EMBED_BASE_URL=https://api.openai.com/v1  EMBED_API_KEY=sk-...

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
# Embeddings, configured in their own right. Reusing the chat variables would mean
# pointing the chat client at an embeddings host to satisfy the embedder, which sends
# the chat provider's key somewhere it does not belong.
EMBED_BASE_URL = os.environ.get("EMBED_BASE_URL") or ""
EMBED_API_KEY = os.environ.get("EMBED_API_KEY") or ""

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


def _embedding_endpoint() -> tuple[str, str]:
    """Where embeddings live, and the credential for it.

    Explicit `EMBED_*` always wins. Otherwise the chat settings are reused only for an
    OpenAI-compatible provider, where one host really does serve both APIs. Anthropic
    serves no embedding endpoint, so rather than quietly posting an Anthropic key to
    `api.openai.com`, it asks to be told.
    """
    if EMBED_BASE_URL:
        return EMBED_BASE_URL.rstrip("/"), EMBED_API_KEY or LLM_API_KEY
    if PROVIDER in ("openai", "openai-compatible"):
        return (LLM_BASE_URL or "https://api.openai.com/v1").rstrip("/"), LLM_API_KEY
    raise RuntimeError(
        f"LLM_PROVIDER={PROVIDER} serves no embedding endpoint. Set EMBED_BASE_URL and "
        "EMBED_API_KEY to an OpenAI-compatible embeddings service, or leave EMBED_MODEL "
        "on Ollama. Not reusing LLM_BASE_URL: that is the chat endpoint, and pointing it "
        "at an embeddings host would send the chat provider's key there too."
    )


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

        base, key = _embedding_endpoint()
        r = requests.post(
            f"{base}/embeddings",
            headers={"Authorization": f"Bearer {key}"},
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
    chat_at = {"ollama": OLLAMA_URL, "anthropic": LLM_BASE_URL or "api.anthropic.com"}.get(
        PROVIDER, LLM_BASE_URL or "api.openai.com"
    )
    if PROVIDER == "ollama":
        embed_at = OLLAMA_URL
    else:
        try:
            embed_at = _embedding_endpoint()[0]
        except RuntimeError:
            embed_at = "UNSET — see EMBED_BASE_URL"
    return f"{PROVIDER}: chat={CHAT_MODEL} via {chat_at} · embed={EMBED_MODEL} via {embed_at}"
