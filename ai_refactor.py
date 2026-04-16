"""
===============================================================================
Project          : RK-Data-Scraping (AI Distributed Cloud Cluster Module)
File             : ai_refactor.py
Version          : 3.0.0
Description      : Resilient, state-managed pipeline that distributes product
                   SEO-optimisation tasks across cloud AI agents in parallel.
                   Features SQLite-backed state tracking for million-row scale,
                   a precise tiered backoff strategy, strict no-blank-save
                   validation, and token-efficient AI prompts.

                   Supports two operating modes:
                     Method 1 — Ollama Cloud (5 private API agents)
                     Method 2 — OllamaFreeAPI (all free community models)

Author           : Fattain Naime
Website          : https://iamnaime.info.bd
Laboratory       : Lab_0x4E
===============================================================================

Architecture overview
─────────────────────
  StateManager       — SQLite-backed (WAL mode, thread-local connections).
                       Tracks every processed Product Link so the pipeline is
                       fully resumable after any crash or manual stop.

  AIClient           — (Method 1) Encapsulates all HTTP logic for one API call
                       to the Ollama Cloud endpoint. Distinguishes transient
                       rate-limits (backoff & retry) from permanent quota/auth
                       failures (kill the agent).

  FreeAIClient       — (Method 2) Uses OllamaFreeAPI for model/server
                       discovery, then calls ollama.Client.generate() directly
                       with full control over system prompt, JSON format, and
                       timeout. Each free model runs as its own parallel agent.

  AgentWorker        — One thread per AI agent. Consumes task chunks from a
                       shared queue. On critical failure, re-queues unfinished
                       items so surviving agents can absorb the load.

  AIClusterPipeline  — Top-level orchestrator. Loads CSVs, builds the pending
                       list, fills the queue, and launches all workers.
                       Accepts pluggable AI client and agent list to support
                       both methods through a single pipeline.

Retry / backoff policy (rate-limit HTTP 429 only — Method 1)
─────────────────────────────────────────────────────────────
  Attempt 1 fails  →  wait  30 s  →  Attempt 2
  Attempt 2 fails  →  wait 120 s  →  Attempt 3
  Attempt 3 fails  →  wait 600 s  →  Attempt 4
  Attempt 4 fails  →  give up this product (row NOT written)

Retry policy (server/network failures — Method 2)
──────────────────────────────────────────────────
  Each attempt cycles through all available servers for the model
  (sorted by throughput). Up to 4 total attempts with the same
  tiered backoff as Method 1.

Quota / auth errors (HTTP 401, 402, 403, 503 + billing keywords)
─────────────────────────────────────────────────────────────────
  is_critical = True  →  worker terminates, unfinished items re-queued.

Output strictness
─────────────────
  A row is written to the output CSV ONLY when both new_name and
  new_description are non-empty strings returned by the AI. Any partial
  or failed result is silently dropped and logged.
"""

from __future__ import annotations

import csv
import html as html_lib
import json
import logging
import os
import queue
import re
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

import requests

# ============================================================
# OPTIONAL: OllamaFreeAPI + ollama client (Method 2)
# ============================================================

_FREE_API_AVAILABLE = False
try:
    from ollamafreeapi import OllamaFreeAPI
    from ollama import Client as OllamaClient
    _FREE_API_AVAILABLE = True
except ImportError:
    pass

# ============================================================
# CONFIGURATION — edit these values before running
# ============================================================

INPUT_CSV  = "Scraped_Products_Data.csv"
OUTPUT_CSV = "SEO_Optimized_Products.csv"
STATE_DB   = "pipeline_state.db"

# Number of products per task chunk. Each agent picks up one chunk at a time.
CHUNK_SIZE = 100

# Tiered backoff durations (seconds) applied on consecutive rate-limit failures.
# Index 0 → wait before attempt 2, index 1 → before attempt 3, index 2 → before attempt 4.
RATE_LIMIT_BACKOFF_SECONDS: list[int] = [30, 120, 600]

# Raw description is truncated to this length before sending to AI.
# Saves tokens on very long product descriptions without losing key content.
MAX_DESC_INPUT_CHARS: int = 2_000

# ⚠️  SECURITY: Do NOT commit real API keys to version control.
#     Use environment variables or a secrets manager in production.
#     Rotate the keys that were present in the original file immediately.
AI_AGENTS: list[dict] = [
    {
        "name": "qwen3-next:80b",
        "api_url": "https://ollama.com/api/chat",
        "api_key": os.environ.get("AGENT1", "YOUR_AGENT1_KEY"),
        "model": "qwen3-next:80b-cloud",
    },
    {
        "name": "gemma4:31b",
        "api_url": "https://ollama.com/api/chat",
        "api_key": os.environ.get("AGENT2", "YOUR_AGENT2_KEY"),
        "model": "gemma4:31b-cloud",
    },
    {
        "name": "nemotron-3-super",
        "api_url": "https://ollama.com/api/chat",
        "api_key": os.environ.get("AGENT3", "YOUR_AGENT3_KEY"),
        "model": "nemotron-3-super:cloud",
    },
    {
        "name": "kimi-k2.5",
        "api_url": "https://ollama.com/api/chat",
        "api_key": os.environ.get("AGENT4", "YOUR_AGENT4_KEY"),
        "model": "kimi-k2.5:cloud",
    },
    {
        "name": "minimax-m2.7",
        "api_url": "https://ollama.com/api/chat",
        "api_key": os.environ.get("AGENT5", "YOUR_AGENT5_KEY"),
        "model": "minimax-m2.7:cloud",
    },
]

# ============================================================
# AI SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT: str = (
    "You are an e-commerce SEO copywriter. "
    "Transform raw product data into optimised English sales copy.\n\n"
    "STRICT RULES — violating any rule makes the output invalid:\n"
    "1. TITLE: Under 65 characters. Title Case. Primary keyword included. No emojis.\n"
    "2. DESCRIPTION: Use AIDA or PAS copywriting framework. "
    "   Emphasise benefits over features. Sensory language encouraged.\n"
    "3. LANGUAGE: Always write in English, even if the input is in Bengali or another language.\n"
    "4. FORBIDDEN CONTENT: Never mention prices, costs, discounts, delivery, "
    "   shipping, Cash on Delivery (COD), payment methods, or ordering instructions.\n"
    "5. NO MARKDOWN: Do not use **, *, #, bullet points, or any markdown syntax.\n"
    "6. OUTPUT FORMAT: Return ONLY a valid JSON object — no preamble, no trailing "
    "   text, no markdown code fences (```json is forbidden).\n"
    '   Exact required schema: {"new_name": "...", "new_description": "..."}\n'
    "   Any content outside this JSON object will cause a parse failure and the "
    "   result will be discarded."
)

# ============================================================
# GRACEFUL SHUTDOWN — Ctrl+C support
# ============================================================

_shutdown_event = threading.Event()


def _handle_sigint(signum, frame):
    """Handle Ctrl+C: signal all workers to stop after finishing current task."""
    if not _shutdown_event.is_set():
        print("\n")
        logger.warning("🛑  Ctrl+C detected — finishing current tasks then shutting down gracefully...")
        logger.warning("🛑  Press Ctrl+C again to force-quit immediately.")
        _shutdown_event.set()
    else:
        logger.critical("🔴  Force quit! Pipeline stopped immediately.")
        raise SystemExit(1)


# ============================================================
# LOGGING — Emoji-enhanced, human-readable output
# ============================================================

class _EmojiFormatter(logging.Formatter):
    """Custom log formatter that prefixes each line with a level-appropriate emoji."""

    _LEVEL_EMOJI = {
        logging.DEBUG:    "🔍",
        logging.INFO:     "💬",
        logging.WARNING:  "⚠️ ",
        logging.ERROR:    "❌",
        logging.CRITICAL: "🚨",
    }

    def format(self, record: logging.LogRecord) -> str:
        emoji = self._LEVEL_EMOJI.get(record.levelno, "  ")
        record.emoji = emoji
        return super().format(record)


_handler = logging.StreamHandler()
_handler.setFormatter(
    _EmojiFormatter(
        fmt="%(emoji)s  %(asctime)s  [%(threadName)-22s]  %(message)s",
        datefmt="%H:%M:%S",
    )
)

logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger("ai_refactor")


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

_HTML_TAG_RE   = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")

_INVALID_LINK_VALUES: frozenset[str] = frozenset({"", "n/a", "none", "null", "#"})


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = html_lib.unescape(text)
    text = _HTML_TAG_RE.sub(" ", text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def truncate_text(text: str, max_chars: int) -> str:
    return text[:max_chars] if len(text) > max_chars else text


def clean_ai_markdown(text: str) -> str:
    return text.replace("**", "").replace("*", "").replace("# ", "").strip()


def is_valid_product_link(link: str) -> bool:
    return bool(link) and link.strip().lower() not in _INVALID_LINK_VALUES


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _free_api_server_sort_key(server: dict) -> float:
    """
    Sort key for ranking free-API servers by measured throughput (tokens/s).

    Mirrors the logic of `OllamaFreeAPI._select_best_server` (which exists
    only on the GitHub `main` branch, NOT in PyPI v0.1.4). Defensive about
    missing, non-numeric, or stringified performance data — anything that
    can't be parsed as a positive float goes last.

    Note: the live JSON data ships `tokens_per_second` as a *string*
    (e.g. "13.01"), not a number, so we must attempt a float() coercion
    rather than checking isinstance(..., (int, float)) directly.
    """
    perf = server.get("performance")
    if not isinstance(perf, dict):
        return -1.0
    tok_s = perf.get("tokens_per_second")
    if tok_s is None:
        return -1.0
    try:
        value = float(tok_s)
    except (TypeError, ValueError):
        return -1.0
    return value if value >= 0 else -1.0


def parse_ai_json(ai_raw: str, agent_name: str) -> Optional[dict]:
    """
    Shared JSON extraction and validation for both AI client methods.

    Extracts the first JSON object from `ai_raw`, parses it, and validates
    that both `new_name` and `new_description` are non-empty strings after
    markdown cleanup.

    Returns
    -------
    dict with keys "new_name" and "new_description" on success, None otherwise.
    """
    json_match = re.search(r"\{.*\}", ai_raw, re.DOTALL)
    if not json_match:
        logger.error(
            f"[{agent_name}] No JSON object found in AI reply. "
            f"Raw (first 300 chars): {ai_raw[:300]}"
        )
        return None

    try:
        parsed = json.loads(json_match.group(0))
    except json.JSONDecodeError as exc:
        logger.error(
            f"[{agent_name}] JSON parse error: {exc}. "
            f"Raw: {json_match.group(0)[:300]}"
        )
        return None

    new_name = clean_ai_markdown(parsed.get("new_name", ""))
    new_desc = clean_ai_markdown(parsed.get("new_description", ""))

    if not new_name or not new_desc:
        logger.warning(
            f"[{agent_name}] AI returned empty name or description — result discarded."
        )
        return None

    return {"new_name": new_name, "new_description": new_desc}


# ============================================================
# STATE MANAGER — SQLite-backed for million-row scale
# ============================================================

class StateManager:
    """
    Manages pipeline progress using an SQLite database.

    Each thread gets its own connection via threading.local() to avoid SQLite's
    "check_same_thread" restriction. A write-lock serialises INSERT/UPDATE
    operations so WAL mode handles concurrent reads without contention.

    Schema
    ──────
    meta(key TEXT PRIMARY KEY, value TEXT)
        Stores pipeline-level statistics (timestamp, counts).

    processed(product_link TEXT PRIMARY KEY, optimized_at TEXT)
        One row per successfully optimised product.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path     = db_path
        self._local      = threading.local()
        self._write_lock = threading.Lock()
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self) -> None:
        with self._write_lock:
            self._conn().executescript("""
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS processed (
                    product_link TEXT PRIMARY KEY,
                    optimized_at TEXT NOT NULL
                );
            """)
            self._conn().commit()

    def _insert_processed(self, product_link: str) -> None:
        self._conn().execute(
            "INSERT OR IGNORE INTO processed (product_link, optimized_at) VALUES (?, ?)",
            (product_link, utcnow_iso()),
        )
        self._conn().commit()

    def initialize_from_sources(self, source_csv: str, output_csv: str) -> None:
        """Sync the state DB from the existing output CSV on startup (idempotency guarantee)."""
        if not os.path.exists(output_csv):
            logger.info("📂  No previous output file found — starting fresh.")
            return

        logger.info("🔄  Syncing progress database from existing output file...")
        count = 0
        with open(output_csv, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                link = row.get("Product Link", "").strip()
                name = row.get("Product Name", "").strip()
                desc = row.get("Description", "").strip()
                if is_valid_product_link(link) and name and desc:
                    with self._write_lock:
                        self._insert_processed(link)
                    count += 1

        logger.info(f"✅  Loaded {count:,} previously optimised products from output file.")

    def is_processed(self, product_link: str) -> bool:
        cur = self._conn().execute(
            "SELECT 1 FROM processed WHERE product_link = ? LIMIT 1",
            (product_link,),
        )
        return cur.fetchone() is not None

    def mark_processed(self, product_link: str) -> None:
        with self._write_lock:
            self._insert_processed(product_link)

    def get_optimized_count(self) -> int:
        return self._conn().execute(
            "SELECT COUNT(*) FROM processed"
        ).fetchone()[0]

    def update_summary_meta(self, total: int, optimized: int) -> None:
        with self._write_lock:
            self._conn().executemany(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                [
                    ("last_edited_timestamp", utcnow_iso()),
                    ("total_products",        str(total)),
                    ("products_optimized",    str(optimized)),
                    ("pending_products",      str(max(0, total - optimized))),
                ],
            )
            self._conn().commit()

    def log_startup_summary(self, total_in_source: int) -> None:
        optimized = self.get_optimized_count()
        pending   = max(0, total_in_source - optimized)
        pct_done  = (optimized / total_in_source * 100) if total_in_source else 0
        logger.info("─" * 55)
        logger.info("📊  PIPELINE STATUS")
        logger.info(f"    📦  Total products in source  : {total_in_source:>10,}")
        logger.info(f"    ✅  Already optimised         : {optimized:>10,}  ({pct_done:.1f}%)")
        logger.info(f"    ⏳  Pending (to be processed) : {pending:>10,}")
        logger.info(f"    💾  State database            : {self.db_path}")
        logger.info("─" * 55)


# ============================================================
# AI CLIENT — Method 1: Ollama Cloud (HTTP calls, backoff, quota detection)
# ============================================================

_QUOTA_STATUS_CODES: frozenset[int] = frozenset({402, 503})
_AUTH_STATUS_CODES:  frozenset[int] = frozenset({401, 403})

_QUOTA_BODY_KEYWORDS: tuple[str, ...] = (
    "quota exceeded",
    "insufficient credits",
    "billing",
    "hard cap",
    "account suspended",
    "out of credits",
    "payment required",
)


class AIClient:
    """
    Method 1: Ollama Cloud — direct HTTP calls to ollama.com/api/chat.

    Handles all HTTP communication with a cloud AI endpoint.

    Return contract for `call()`:
    ──────────────────────────────
    (result: Optional[dict], is_critical: bool)

    result       — parsed {"new_name": ..., "new_description": ...} on success,
                   None on any failure.
    is_critical  — True if the agent should be permanently shut down.
    """

    def __init__(self) -> None:
        self._session = requests.Session()

    def call(
        self,
        agent: dict,
        raw_name: str,
        raw_desc: str,
    ) -> tuple[Optional[dict], bool]:
        clean_name = strip_html(raw_name)
        clean_desc = truncate_text(strip_html(raw_desc), MAX_DESC_INPUT_CHARS)

        user_prompt = (
            f"Product Name: {clean_name}\n"
            f"Description: {clean_desc}"
        )

        payload = {
            "model": agent["model"],
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            "format": "json",
            "options": {"temperature": 0.7},
            "stream": False,
        }
        headers = {
            "Authorization": f"Bearer {agent['api_key']}",
            "Content-Type": "application/json",
        }

        max_attempts = len(RATE_LIMIT_BACKOFF_SECONDS) + 1  # 4 total attempts

        for attempt in range(1, max_attempts + 1):
            # Respect shutdown signal — don't fire more requests if stopping
            if _shutdown_event.is_set():
                return None, False

            result, is_critical, is_rate_limited = self._execute_request(
                agent, payload, headers, raw_name, attempt
            )

            if result is not None or is_critical:
                return result, is_critical

            backoff_idx = attempt - 1
            if attempt < max_attempts:
                if is_rate_limited and backoff_idx < len(RATE_LIMIT_BACKOFF_SECONDS):
                    wait = RATE_LIMIT_BACKOFF_SECONDS[backoff_idx]
                    logger.warning(
                        f"[{agent['name']}] ⏳  Rate limited (attempt {attempt}/{max_attempts}). "
                        f"Waiting {wait}s before retry..."
                    )
                    # Sleep in short intervals so Ctrl+C is responsive
                    for _ in range(wait):
                        if _shutdown_event.is_set():
                            return None, False
                        time.sleep(1)
                else:
                    logger.warning(
                        f"[{agent['name']}] 🔁  Transient error (attempt {attempt}/{max_attempts}). "
                        "Retrying in 5s..."
                    )
                    for _ in range(5):
                        if _shutdown_event.is_set():
                            return None, False
                        time.sleep(1)
            else:
                logger.error(
                    f"[{agent['name']}] All {max_attempts} attempts failed "
                    f"for '{raw_name[:40]}'. Skipping this product."
                )

        return None, False

    def _execute_request(
        self,
        agent: dict,
        payload: dict,
        headers: dict,
        raw_name: str,
        attempt: int,
    ) -> tuple[Optional[dict], bool, bool]:
        try:
            response = self._session.post(
                agent["api_url"],
                headers=headers,
                json=payload,
                timeout=600,
            )
        except requests.exceptions.Timeout:
            logger.warning(
                f"[{agent['name']}] ⌛  Request timed out (attempt {attempt}) "
                f"— '{raw_name[:40]}'"
            )
            return None, False, False
        except requests.exceptions.RequestException as exc:
            logger.error(f"[{agent['name']}] 🌐  Network error (attempt {attempt}): {exc}")
            return None, False, False

        status = response.status_code

        if status in _AUTH_STATUS_CODES:
            logger.critical(
                f"[{agent['name']}] 🔑  Authentication failed (HTTP {status}). "
                "API key is invalid or revoked — agent permanently disabled."
            )
            return None, True, False

        if status in _QUOTA_STATUS_CODES:
            body_lower = response.text.lower()
            if any(kw in body_lower for kw in _QUOTA_BODY_KEYWORDS):
                logger.critical(
                    f"[{agent['name']}] 💳  Billing quota exceeded (HTTP {status}). "
                    f"Response: {response.text[:200]}  — "
                    "Top up credits and re-run. Agent permanently disabled."
                )
                return None, True, False
            logger.warning(
                f"[{agent['name']}] HTTP {status} (no billing keyword) — treating as transient."
            )
            return None, False, False

        if status == 429:
            logger.warning(
                f"[{agent['name']}] 🚦  Rate limited (HTTP 429) on attempt {attempt}."
            )
            return None, False, True

        if not response.ok:
            logger.error(
                f"[{agent['name']}] Unexpected HTTP {status} on attempt {attempt}. "
                f"Response: {response.text[:200]}"
            )
            return None, False, False

        try:
            body   = response.json()
            ai_raw = body.get("message", {}).get("content", "").strip()
        except (ValueError, AttributeError) as exc:
            logger.error(f"[{agent['name']}] Failed to decode JSON response: {exc}")
            return None, False, False

        result = parse_ai_json(ai_raw, agent["name"])
        if result is not None:
            return result, False, False
        return None, False, False


# ============================================================
# AI CLIENT — Method 2: OllamaFreeAPI (community free models)
# ============================================================

class FreeAIClient:
    """
    Method 2: OllamaFreeAPI — uses community-hosted free Ollama servers.

    Uses `OllamaFreeAPI` for model catalogue and server discovery, then calls
    `ollama.Client.generate()` directly for full control over:
      - system prompt  (sent as the `system` parameter)
      - format: "json" (instructs the model to output valid JSON)
      - num_predict    (token budget — raised to 2048 for SEO content)
      - timeout        (600s to match Method 1)

    Servers are tried in throughput-descending order (same logic as the
    library's internal `_select_best_server`). If all servers fail for a
    given attempt, the tiered backoff kicks in before the next attempt.

    Return contract for `call()`:
    ──────────────────────────────
    (result: Optional[dict], is_critical: bool)

    result       — parsed {"new_name": ..., "new_description": ...} on success,
                   None on any failure.
    is_critical  — Always False (free servers have no auth/billing concept).
    """

    def __init__(self, api: "OllamaFreeAPI") -> None:
        self._api = api

    def call(
        self,
        agent: dict,
        raw_name: str,
        raw_desc: str,
    ) -> tuple[Optional[dict], bool]:
        model_name = agent["model"]
        agent_name = agent["name"]

        clean_name = strip_html(raw_name)
        clean_desc = truncate_text(strip_html(raw_desc), MAX_DESC_INPUT_CHARS)

        user_prompt = (
            f"Product Name: {clean_name}\n"
            f"Description: {clean_desc}"
        )

        # Discover and rank servers for this model
        try:
            servers = self._api.get_model_servers(model_name)
        except Exception as exc:
            logger.error(f"[{agent_name}] Failed to discover servers: {exc}")
            return None, False

        if not servers:
            logger.error(f"[{agent_name}] No servers available for model '{model_name}'.")
            return None, False

        # Sort servers by measured throughput (descending); servers without
        # performance data go last. Inlined here — the private helper
        # `_api._select_best_server` only exists on ollamafreeapi's GitHub
        # `main` branch and is NOT in the PyPI release (v0.1.4), so calling
        # it directly would crash on most installations.
        servers = sorted(servers, key=_free_api_server_sort_key, reverse=True)

        max_attempts = len(RATE_LIMIT_BACKOFF_SECONDS) + 1  # 4 total attempts

        for attempt in range(1, max_attempts + 1):
            if _shutdown_event.is_set():
                return None, False

            # Try each server in throughput-descending order
            result = self._try_servers(
                servers, model_name, agent_name, user_prompt, attempt
            )

            if result is not None:
                return result, False

            # All servers failed this attempt — apply tiered backoff
            if attempt < max_attempts:
                backoff_idx = attempt - 1
                if backoff_idx < len(RATE_LIMIT_BACKOFF_SECONDS):
                    wait = RATE_LIMIT_BACKOFF_SECONDS[backoff_idx]
                else:
                    wait = 10
                logger.warning(
                    f"[{agent_name}] 🔁  All servers failed (attempt {attempt}/{max_attempts}). "
                    f"Waiting {wait}s before retry..."
                )
                for _ in range(wait):
                    if _shutdown_event.is_set():
                        return None, False
                    time.sleep(1)
            else:
                logger.error(
                    f"[{agent_name}] All {max_attempts} attempts exhausted "
                    f"for '{raw_name[:40]}'. Skipping this product."
                )

        return None, False

    def _try_servers(
        self,
        servers: list[dict],
        model_name: str,
        agent_name: str,
        user_prompt: str,
        attempt: int,
    ) -> Optional[dict]:
        """
        Iterate through servers (best-throughput first). Returns the first
        successful parsed result, or None if every server failed.
        """
        for server in servers:
            if _shutdown_event.is_set():
                return None

            server_url = server.get("url", "")
            if not server_url:
                continue

            try:
                # timeout belongs on the Client constructor (passed through
                # to httpx); ollama.Client.generate() does NOT accept a
                # `timeout` kwarg and will raise TypeError if one is passed.
                client = OllamaClient(host=server_url, timeout=600)
                response = client.generate(
                    model=model_name,
                    system=SYSTEM_PROMPT,
                    prompt=user_prompt,
                    format="json",
                    options={"temperature": 0.7, "num_predict": 2048},
                    stream=False,
                )
            except Exception as exc:
                logger.warning(
                    f"[{agent_name}] Server {server_url} failed "
                    f"(attempt {attempt}): {type(exc).__name__}: {str(exc)[:120]}"
                )
                continue

            # Extract response text — handle both dict and object forms
            ai_raw = ""
            if isinstance(response, dict):
                ai_raw = response.get("response", "").strip()
            else:
                ai_raw = getattr(response, "response", "")
                if not ai_raw:
                    try:
                        ai_raw = str(response)
                    except Exception:
                        pass
                ai_raw = ai_raw.strip()

            if not ai_raw:
                logger.warning(
                    f"[{agent_name}] Server {server_url} returned empty response "
                    f"(attempt {attempt})."
                )
                continue

            result = parse_ai_json(ai_raw, agent_name)
            if result is not None:
                return result

            # JSON parse failed — try next server (different server might
            # produce a cleaner response for the same model)

        return None


# ============================================================
# FREE API — Agent builder
# ============================================================

def build_free_api_agents() -> tuple[list[dict], "OllamaFreeAPI"]:
    """
    Instantiate OllamaFreeAPI, discover all available models, deduplicate,
    and return a list of agent dicts compatible with AgentWorker.

    Returns
    -------
    (agents, api_instance)
        agents       — list of {"name": ..., "model": ...} dicts (one per unique model)
        api_instance — the live OllamaFreeAPI instance (passed to FreeAIClient)
    """
    api = OllamaFreeAPI()
    all_models = api.list_models()

    # Deduplicate while preserving discovery order
    seen: set[str] = set()
    unique_models: list[str] = []
    for m in all_models:
        if m not in seen:
            seen.add(m)
            unique_models.append(m)

    agents = [{"name": model, "model": model} for model in unique_models]
    return agents, api


# ============================================================
# AGENT WORKER THREAD
# ============================================================

_SHUTDOWN_SENTINEL = object()


class AgentWorker(threading.Thread):
    """
    Worker thread bound to one AI agent.

    Lifecycle
    ─────────
    1. Block on queue.get() until a chunk arrives.
    2. Iterate the chunk, calling the AI client's call() for each product.
    3. On SUCCESS  → write the optimised row to the shared CSV; mark in state DB.
    4. On FAILURE  → log and skip (strict no-blank rule).
    5. On CRITICAL → re-queue remaining items; terminate this thread permanently.
    6. On SENTINEL or shutdown event → exit the loop cleanly.
    """

    def __init__(
        self,
        agent: dict,
        task_queue: queue.Queue,
        state_manager: StateManager,
        ai_client,
        csv_writer: csv.DictWriter,
        csv_file,
        csv_lock: threading.Lock,
        total_products: int,
        optimized_counter: list[int],
        counter_lock: threading.Lock,
    ) -> None:
        super().__init__(name=f"Agent-{agent['name']}", daemon=True)
        self.agent             = agent
        self.task_queue        = task_queue
        self.state             = state_manager
        self.client            = ai_client
        self.csv_writer        = csv_writer
        self.csv_file          = csv_file
        self.csv_lock          = csv_lock
        self.total_products    = total_products
        self.optimized_counter = optimized_counter
        self.counter_lock      = counter_lock

    def run(self) -> None:
        logger.info(f"🚀  Agent [{self.agent['name']}] started and ready for tasks.")

        while not _shutdown_event.is_set():
            try:
                chunk = self.task_queue.get(timeout=5)
            except queue.Empty:
                if _shutdown_event.is_set():
                    break
                logger.info(f"✅  Agent [{self.agent['name']}] — queue empty, shutting down.")
                break

            if chunk is _SHUTDOWN_SENTINEL:
                self.task_queue.task_done()
                logger.info(f"🏁  Agent [{self.agent['name']}] received shutdown signal. Stopping.")
                break

            try:
                agent_is_dead = self._process_chunk(chunk)
            finally:
                self.task_queue.task_done()

            if agent_is_dead:
                logger.critical(f"💀  Agent [{self.agent['name']}] permanently disabled after critical error.")
                break

        if _shutdown_event.is_set():
            logger.info(f"🛑  Agent [{self.agent['name']}] stopped gracefully.")
        else:
            logger.info(f"👋  Agent [{self.agent['name']}] finished all tasks.")

    def _process_chunk(self, chunk: list[tuple[int, dict]]) -> bool:
        """
        Process one chunk of (global_index, source_row) tuples.
        Returns True if the agent hit a critical error and should terminate.
        """
        logger.info(f"📋  Agent [{self.agent['name']}] — processing chunk of {len(chunk)} products.")

        for idx, (global_index, row) in enumerate(chunk):
            # Stop mid-chunk if shutdown was requested
            if _shutdown_event.is_set():
                remaining = chunk[idx:]
                if remaining:
                    self.task_queue.put(remaining)
                return False

            product_link = row.get("Product Link", "").strip()
            raw_name     = row.get("Product Name", "").strip()
            raw_desc     = row.get("Description",  "").strip()

            if not raw_name or raw_name.lower() in {"n/a", "none", "null", ""}:
                logger.warning(
                    f"[{self.agent['name']}] [{global_index}/{self.total_products}] "
                    f"Skipping — product has no name (link: {product_link[:50]})"
                )
                continue

            logger.info(
                f"[{self.agent['name']}] [{global_index}/{self.total_products}] "
                f"⚙️   Optimising: {raw_name[:50]}..."
            )

            ai_result, is_critical = self.client.call(self.agent, raw_name, raw_desc)

            if is_critical:
                remaining = chunk[idx:]
                if remaining:
                    logger.critical(
                        f"[{self.agent['name']}] 🔁  Re-queuing {len(remaining)} unfinished item(s) "
                        "for other agents."
                    )
                    self.task_queue.put(remaining)
                return True

            if ai_result:
                output_row = dict(row)
                output_row["Product Name"] = ai_result["new_name"]
                output_row["Description"]  = ai_result["new_description"]

                with self.csv_lock:
                    self.csv_writer.writerow(output_row)
                    self.csv_file.flush()

                self.state.mark_processed(product_link)

                with self.counter_lock:
                    self.optimized_counter[0] += 1
                    running_total = self.optimized_counter[0]

                logger.info(
                    f"[{self.agent['name']}] [{global_index}/{self.total_products}] "
                    f"✅  Done → '{ai_result['new_name'][:50]}' "
                    f"(Total so far: {running_total:,})"
                )
            else:
                logger.warning(
                    f"[{self.agent['name']}] [{global_index}/{self.total_products}] "
                    f"⏭️   Skipped — '{raw_name[:50]}' could not be optimised. Row not saved."
                )

        return False


# ============================================================
# PIPELINE ORCHESTRATOR
# ============================================================

class AIClusterPipeline:
    """
    Top-level orchestrator.

    Supports both Method 1 (Ollama Cloud) and Method 2 (OllamaFreeAPI)
    through pluggable ai_client and agents parameters.

    Run sequence
    ────────────
    1. Load and validate the source CSV.
    2. Initialise StateManager; sync it with the existing output CSV.
    3. Log the startup summary (how many done, how many pending).
    4. Build the pending list using O(log n) DB lookups.
    5. Distribute pending products as chunks into a shared queue.
    6. Open the output CSV in append mode; launch one AgentWorker per agent.
    7. Wait for all work to complete (queue.join() + thread.join()).
    8. Persist the final stats to the state DB and emit a completion summary.

    Parameters
    ──────────
    ai_client    — An object with a `call(agent, raw_name, raw_desc)` method.
                   Defaults to AIClient() (Method 1).
    agents       — List of agent dicts. Each must have at least a "name" key.
                   If None, uses AI_AGENTS with API key validation (Method 1).
    method_name  — Human-readable label for log output.
    """

    def __init__(
        self,
        ai_client=None,
        agents: Optional[list[dict]] = None,
        method_name: str = "Ollama Cloud",
    ) -> None:
        self.state       = StateManager(STATE_DB)
        self.ai_client   = ai_client if ai_client is not None else AIClient()
        self._agents     = agents    # None = use AI_AGENTS with key validation
        self.method_name = method_name
        self.task_queue: queue.Queue = queue.Queue()

    def run(self) -> None:
        # ----------------------------------------------------------
        # Step 1: Load source CSV
        # ----------------------------------------------------------
        if not os.path.exists(INPUT_CSV):
            logger.critical(
                f"Source file '{INPUT_CSV}' not found. "
                "Update INPUT_CSV to the correct path and try again."
            )
            return

        logger.info(f"📂  Loading source file: {INPUT_CSV}")
        with open(INPUT_CSV, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                logger.critical("Source CSV has no header row. Cannot continue.")
                return
            fieldnames: list[str] = list(reader.fieldnames)
            all_rows: list[dict]  = list(reader)

        total = len(all_rows)
        logger.info(f"📦  {total:,} products loaded from source file.")

        if total == 0:
            logger.warning("Source CSV is empty — nothing to do.")
            return

        # ----------------------------------------------------------
        # Step 2: Initialise state / sync from existing output CSV
        # ----------------------------------------------------------
        self.state.initialize_from_sources(INPUT_CSV, OUTPUT_CSV)

        # ----------------------------------------------------------
        # Step 3: Startup summary
        # ----------------------------------------------------------
        self.state.log_startup_summary(total)

        # ----------------------------------------------------------
        # Step 4: Build pending list
        # ----------------------------------------------------------
        pending: list[tuple[int, dict]] = []
        for idx, row in enumerate(all_rows, start=1):
            link = row.get("Product Link", "").strip()
            if not is_valid_product_link(link):
                logger.debug(f"Row {idx}: No valid Product Link — skipping.")
                continue
            if not self.state.is_processed(link):
                pending.append((idx, row))

        logger.info(f"⏳  Products to process this run: {len(pending):,}")

        if not pending:
            logger.info("🎉  All products are already optimised — nothing to do!")
            return

        # ----------------------------------------------------------
        # Step 5: Enqueue chunks
        # ----------------------------------------------------------
        for i in range(0, len(pending), CHUNK_SIZE):
            self.task_queue.put(pending[i : i + CHUNK_SIZE])

        chunk_count = self.task_queue.qsize()
        logger.info(
            f"📋  {chunk_count:,} chunk(s) queued "
            f"(up to {CHUNK_SIZE} products each)."
        )

        # ----------------------------------------------------------
        # Step 6: Resolve agents and launch workers
        # ----------------------------------------------------------
        if self._agents is not None:
            # Method 2 (or any externally-provided agent list):
            # no API-key validation needed — agents are pre-validated.
            valid_agents = self._agents
        else:
            # Method 1: filter out agents without a real API key.
            valid_agents = [
                a for a in AI_AGENTS
                if a.get("api_key") and not a["api_key"].startswith("YOUR_")
            ]

        if not valid_agents:
            logger.critical(
                "No agents available. "
                "For Method 1, set your API keys via environment variables (see .env). "
                "For Method 2, ensure ollamafreeapi is installed and has models. "
                "Aborting."
            )
            return

        # Log skipped agents (Method 1 only — when using AI_AGENTS)
        if self._agents is None:
            for skipped in [a for a in AI_AGENTS if a not in valid_agents]:
                logger.warning(
                    f"⚠️   Agent '{skipped['name']}' skipped — API key not set or is a placeholder."
                )

        file_is_new = not (
            os.path.exists(OUTPUT_CSV) and os.stat(OUTPUT_CSV).st_size > 0
        )

        # Snapshot the count BEFORE this run starts so we can compute
        # how many were processed specifically in this session.
        initial_optimized: int = self.state.get_optimized_count()

        optimized_counter: list[int] = [initial_optimized]
        counter_lock = threading.Lock()
        csv_lock     = threading.Lock()

        logger.info(f"🤖  Launching {len(valid_agents)} AI agent(s) [{self.method_name}]...")
        for a in valid_agents:
            logger.info(f"    🔹  {a['name']}")

        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as out_f:
            if file_is_new:
                out_f.write("\ufeff")  # UTF-8 BOM for Excel compatibility
                writer = csv.DictWriter(out_f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
            else:
                writer = csv.DictWriter(out_f, fieldnames=fieldnames, extrasaction="ignore")

            workers: list[AgentWorker] = []
            for agent in valid_agents:
                worker = AgentWorker(
                    agent=agent,
                    task_queue=self.task_queue,
                    state_manager=self.state,
                    ai_client=self.ai_client,
                    csv_writer=writer,
                    csv_file=out_f,
                    csv_lock=csv_lock,
                    total_products=total,
                    optimized_counter=optimized_counter,
                    counter_lock=counter_lock,
                )
                workers.append(worker)
                worker.start()

            self.task_queue.join()

            for worker in workers:
                worker.join()

        # ----------------------------------------------------------
        # Step 7: Final summary
        # ----------------------------------------------------------
        final_optimized    = optimized_counter[0]
        processed_this_run = final_optimized - initial_optimized
        skipped_this_run   = len(pending) - processed_this_run
        was_interrupted    = _shutdown_event.is_set()

        self.state.update_summary_meta(total, final_optimized)

        logger.info("─" * 55)
        if was_interrupted:
            logger.info("🛑  PIPELINE INTERRUPTED (Ctrl+C)")
        else:
            logger.info("🎉  PIPELINE COMPLETE")
        logger.info(f"    🏷️   Method used              : {self.method_name}")
        logger.info(f"    📦  Total source products    : {total:>10,}")
        logger.info(f"    ✅  Optimised this run       : {processed_this_run:>10,}")
        logger.info(f"    ⏭️   Skipped / failed         : {skipped_this_run:>10,}")
        logger.info(f"    📊  All-time optimised total : {final_optimized:>10,}")
        logger.info(f"    📄  Output file              : {OUTPUT_CSV}")
        logger.info(f"    💾  State database           : {STATE_DB}")
        logger.info("─" * 55)


# ============================================================
# ENTRY POINT — Method selection menu
# ============================================================

def _print_banner() -> None:
    """Print the startup banner."""
    logger.info("=" * 55)
    logger.info("🧠  AI SEO Pipeline  —  starting up")
    logger.info("💡  Press Ctrl+C at any time to stop gracefully.")
    logger.info("=" * 55)


def _select_method() -> int:
    """
    Display the method selection menu and return the user's choice.

    Returns 1 for Ollama Cloud, 2 for OllamaFreeAPI.
    """
    print()
    print("=" * 55)
    print("  SELECT AI METHOD")
    print("=" * 55)
    print()
    print("  [1]  Ollama Cloud  (private API agents)")
    print("       → Uses 5 cloud-hosted models via ollama.com/api/chat")
    print("       → Requires API keys set in .env")
    print()

    if _FREE_API_AVAILABLE:
        print("  [2]  OllamaFreeAPI  (free community models)")
        print("       → Uses all free open-source models in parallel")
        print("       → No API keys needed — powered by community servers")
    else:
        print("  [2]  OllamaFreeAPI  (⚠️  NOT AVAILABLE)")
        print("       → Install with: pip install ollamafreeapi")

    print()
    print("=" * 55)

    while True:
        try:
            choice = input("\n  Enter your choice (1 or 2): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

        if choice == "1":
            return 1
        elif choice == "2":
            if not _FREE_API_AVAILABLE:
                print(
                    "\n  ❌  OllamaFreeAPI is not installed."
                    "\n      Run: pip install ollamafreeapi"
                    "\n      Then restart this script."
                )
                continue
            return 2
        else:
            print("  ⚠️   Invalid input. Please enter 1 or 2.")


if __name__ == "__main__":
    # Register graceful Ctrl+C handler
    signal.signal(signal.SIGINT, _handle_sigint)

    _print_banner()

    method = _select_method()

    if method == 1:
        # ──────────────────────────────────────────────
        # Method 1: Ollama Cloud (original behaviour)
        # ──────────────────────────────────────────────
        logger.info("🔹  Method 1 selected: Ollama Cloud (private API agents)")
        pipeline = AIClusterPipeline(
            ai_client=AIClient(),
            agents=None,            # → triggers AI_AGENTS + key validation
            method_name="Ollama Cloud",
        )
        pipeline.run()

    elif method == 2:
        # ──────────────────────────────────────────────
        # Method 2: OllamaFreeAPI (community free models)
        # ──────────────────────────────────────────────
        logger.info("🔹  Method 2 selected: OllamaFreeAPI (free community models)")
        logger.info("🔄  Discovering available free models...")

        try:
            free_agents, api_instance = build_free_api_agents()
        except Exception as exc:
            logger.critical(f"Failed to initialise OllamaFreeAPI: {exc}")
            sys.exit(1)

        if not free_agents:
            logger.critical("OllamaFreeAPI returned no models. Cannot proceed.")
            sys.exit(1)

        logger.info(f"📦  Discovered {len(free_agents)} unique free model(s):")
        for a in free_agents:
            logger.info(f"    🔹  {a['name']}")

        pipeline = AIClusterPipeline(
            ai_client=FreeAIClient(api_instance),
            agents=free_agents,
            method_name="OllamaFreeAPI (Free Community Models)",
        )
        pipeline.run()
