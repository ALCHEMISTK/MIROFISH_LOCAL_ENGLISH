"""
LLM Client Wrapper
Unified OpenAI-format API calls
"""

import json
import re
import time
from typing import Optional, Dict, Any, List
from openai import OpenAI

from ..config import Config
from ..utils.logger import get_logger


logger = get_logger('mirofish.llm_client')


class LLMClient:
    """LLM Client"""

    MAX_RETRIES = 3
    RETRY_BASE_DELAY_SECONDS = 2.0
    JSON_PARSE_ATTEMPTS = 3

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.api_key = api_key or Config.LLM_API_KEY
        self.base_url = base_url or Config.LLM_BASE_URL
        self.model = model or Config.LLM_MODEL_NAME

        if not self.api_key:
            raise ValueError("LLM_API_KEY is not configured")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

    def _clean_content(self, content: Optional[str]) -> str:
        """Normalize model output into plain text."""
        if content is None:
            return ""
        # Some models may include <think> reasoning content in the response.
        return re.sub(r'<think>[\s\S]*?</think>', '', content).strip()

    def is_quota_exhausted_error(self, exc: Exception) -> bool:
        """Return True when the provider indicates a hard quota/usage limit."""
        text = str(exc).lower()
        quota_markers = (
            "weekly usage limit",
            "monthly usage limit",
            "daily usage limit",
            "session usage limit",
            "reached your usage limit",
            "upgrade for higher limits",
            "insufficient_quota",
            "quota exceeded",
        )
        return any(marker in text for marker in quota_markers)

    def _is_retryable_error(self, exc: Exception) -> bool:
        """Return True for transient transport/provider failures."""
        if self.is_quota_exhausted_error(exc):
            return False

        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int) and (status_code >= 500 or status_code in {408, 409, 429}):
            return True

        text = str(exc).lower()
        retryable_markers = (
            "internal server error",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "timed out",
            "timeout",
            "connection error",
            "connection reset",
            "could not connect",
            "temporarily unavailable",
            "rate limit",
            "too many requests",
            "empty response",
        )
        return any(marker in text for marker in retryable_markers)

    def _parse_json_response(self, response: str) -> Dict[str, Any]:
        """Parse JSON-mode output with some cleanup and repair."""
        cleaned_response = response.strip()
        cleaned_response = re.sub(r'^```(?:json)?\s*\n?', '', cleaned_response, flags=re.IGNORECASE)
        cleaned_response = re.sub(r'\n?```\s*$', '', cleaned_response)
        cleaned_response = cleaned_response.strip()

        if cleaned_response:
            start = cleaned_response.find('{')
            end = cleaned_response.rfind('}')
            if start != -1 and end != -1 and start < end:
                cleaned_response = cleaned_response[start:end + 1]

        try:
            return json.loads(cleaned_response)
        except json.JSONDecodeError:
            try:
                from json_repair import repair_json
                repaired = repair_json(cleaned_response, return_objects=True)
                if repaired:
                    return repaired
            except Exception:
                pass
            raise ValueError(f"Invalid JSON format returned by LLM: {cleaned_response}")

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2048,
        response_format: Optional[Dict] = None
    ) -> str:
        """
        Send a chat request

        Args:
            messages: List of messages
            temperature: Temperature parameter
            max_tokens: Maximum number of tokens
            response_format: Response format (e.g., JSON mode)

        Returns:
            Model response text
        """
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if response_format:
            kwargs["response_format"] = response_format

        last_error = None
        delay = self.RETRY_BASE_DELAY_SECONDS
        attempt = 0
        quota_wait_started_at = None
        quota_wait_logged_at = None

        while True:
            try:
                response = self.client.chat.completions.create(**kwargs)
                content = self._clean_content(response.choices[0].message.content)
                if content:
                    return content
                raise ValueError("LLM returned empty response")
            except Exception as exc:
                last_error = exc

                if self.is_quota_exhausted_error(exc) and Config.LLM_QUOTA_WAIT_ENABLED:
                    now = time.monotonic()
                    if quota_wait_started_at is None:
                        quota_wait_started_at = now

                    elapsed = now - quota_wait_started_at
                    max_wait = max(0.0, Config.LLM_QUOTA_MAX_WAIT_SECONDS)
                    if max_wait and elapsed >= max_wait:
                        raise TimeoutError(
                            f"LLM quota was not restored within {max_wait:.0f} seconds"
                        ) from exc

                    poll_seconds = max(1.0, Config.LLM_QUOTA_POLL_SECONDS)
                    if quota_wait_logged_at is None or (now - quota_wait_logged_at) >= poll_seconds - 0.1:
                        logger.warning(
                            "LLM quota exhausted for model %s; waiting %.0f seconds before retrying",
                            self.model,
                            poll_seconds
                        )
                        quota_wait_logged_at = now

                    time.sleep(poll_seconds)
                    # Quota refresh is external to the app, so do not consume the retry budget.
                    continue

                if attempt < self.MAX_RETRIES - 1 and self._is_retryable_error(exc):
                    time.sleep(delay)
                    delay *= 2
                    attempt += 1
                    continue
                raise

        raise last_error  # pragma: no cover

    def chat_json(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 2048
    ) -> Dict[str, Any]:
        """
        Send a chat request and return JSON

        Args:
            messages: List of messages
            temperature: Temperature parameter
            max_tokens: Maximum number of tokens

        Returns:
            Parsed JSON object
        """
        last_error: Optional[Exception] = None
        reminder = {
            "role": "user",
            "content": "Return only a valid JSON object. Do not include markdown, code fences, or extra explanation."
        }

        for attempt in range(self.JSON_PARSE_ATTEMPTS):
            response_format = {"type": "json_object"} if attempt < self.JSON_PARSE_ATTEMPTS - 1 else None
            current_messages = messages if attempt == 0 else [*messages, reminder]
            response = self.chat(
                messages=current_messages,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format=response_format
            )
            try:
                return self._parse_json_response(response)
            except ValueError as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error
        raise ValueError("LLM returned invalid JSON")
