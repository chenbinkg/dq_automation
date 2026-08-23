"""
Strands Agent factory for J&J DQ Automation.

Provides a reusable `strands.Agent` wired to:
  - `JNJClaudeGatewayModel` (jnj_strands_model.py) as the model provider, using the
    existing J&J GenAI Gateway model name / API key (no BedrockModel/AWS credentials
    required).
  - `MCPClient` (strands.tools.mcp) tools for the Atlassian (Jira) MCP server at
    https://atlassian-mcp.xena.dev/mcp/, authenticated via headers pulled from the
    Databricks secret scope (falls back to env vars / config.py locally).

This mirrors the Agent + MCPClient pattern in app.py, but swaps BedrockModel for
JNJClaudeGatewayModel and targets the Atlassian MCP server instead of the in-house
MongoDB/S3 MCP servers.

This module intentionally does not modify s1-s9; it only exposes a factory that
those scripts (or new callers) can opt into.

Usage:
    from strands_agent import diagnose_with_agent, atlassian_agent_session

    # One-shot
    answer = diagnose_with_agent("Find JEJQ tickets related to table jpubsdata.sales")

    # Reusable session (keeps MCP connection open across multiple calls)
    with atlassian_agent_session() as agent:
        result = agent("Search JEJQ tickets mentioning table X")
        print(result)
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, Optional

from jnj_strands_model import JNJClaudeGatewayModel

if TYPE_CHECKING:
    from strands import Agent as AgentType
    from strands.tools.mcp import MCPClient as MCPClientType

try:
    from databricks.sdk import WorkspaceClient

    dbutils = WorkspaceClient().dbutils
except Exception:
    dbutils = None

try:
    from strands import Agent
    from strands.tools.mcp import MCPClient
    from mcp.client.streamable_http import streamablehttp_client
except Exception as exc:  # pragma: no cover - optional dependency guard
    Agent = None  # type: ignore[assignment,misc]
    MCPClient = None  # type: ignore[assignment,misc]
    streamablehttp_client = None  # type: ignore[assignment]
    _IMPORT_ERROR: Optional[Exception] = exc
else:
    _IMPORT_ERROR = None

logger = logging.getLogger(__name__)

SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")


def _load_secret_or_default(key: str, default: Any = None) -> Any:
    if dbutils is None:
        return default
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        return default


# --- Atlassian MCP configuration (same secret/env names used by s8) ---
MCP_ATLASSIAN_URL = _load_secret_or_default(
    "mcp_atlassian_url", os.getenv("MCP_ATLASSIAN_URL", "https://atlassian-mcp.xena.dev/mcp/")
)
X_ATLASSIAN_JIRA_URL = _load_secret_or_default("x_atlassian_jira_url", os.getenv("X_ATLASSIAN_JIRA_URL"))
X_ATLASSIAN_JIRA_PERSONAL_TOKEN = _load_secret_or_default(
    "x_atlassian_jira_personal_token", os.getenv("X_ATLASSIAN_JIRA_PERSONAL_TOKEN")
)
X_ATLASSIAN_USERNAME = _load_secret_or_default("x_atlassian_username", os.getenv("X_ATLASSIAN_USERNAME"))
X_ATLASSIAN_READ_ONLY_MODE = _load_secret_or_default(
    "x_atlassian_read_only_mode", os.getenv("X_ATLASSIAN_READ_ONLY_MODE", "false")
)
X_ATLASSIAN_ENABLE_XRAY = _load_secret_or_default(
    "x_atlassian_enable_xray", os.getenv("X_ATLASSIAN_ENABLE_XRAY", "false")
)
# Confluence/Bitbucket are optional, but the MCP server may scope its advertised
# toolset to whichever service headers are present, so send them when available
# for parity with the working VS Code mcp.json config.
X_ATLASSIAN_CONFLUENCE_URL = _load_secret_or_default(
    "x_atlassian_confluence_url", os.getenv("X_ATLASSIAN_CONFLUENCE_URL")
)
X_ATLASSIAN_CONFLUENCE_PERSONAL_TOKEN = _load_secret_or_default(
    "x_atlassian_confluence_personal_token", os.getenv("X_ATLASSIAN_CONFLUENCE_PERSONAL_TOKEN")
)
X_ATLASSIAN_BITBUCKET_URL = _load_secret_or_default(
    "x_atlassian_bitbucket_url", os.getenv("X_ATLASSIAN_BITBUCKET_URL")
)
X_ATLASSIAN_BITBUCKET_PERSONAL_TOKEN = _load_secret_or_default(
    "x_atlassian_bitbucket_personal_token", os.getenv("X_ATLASSIAN_BITBUCKET_PERSONAL_TOKEN")
)

_RAW_MCP_HEADERS = {
    "X-Atlassian-Jira-Url": X_ATLASSIAN_JIRA_URL,
    "X-Atlassian-Jira-Personal-Token": X_ATLASSIAN_JIRA_PERSONAL_TOKEN,
    "X-Atlassian-Confluence-Url": X_ATLASSIAN_CONFLUENCE_URL,
    "X-Atlassian-Confluence-Personal-Token": X_ATLASSIAN_CONFLUENCE_PERSONAL_TOKEN,
    "X-Atlassian-Bitbucket-Url": X_ATLASSIAN_BITBUCKET_URL,
    "X-Atlassian-Bitbucket-Personal-Token": X_ATLASSIAN_BITBUCKET_PERSONAL_TOKEN,
    "X-Atlassian-Username": X_ATLASSIAN_USERNAME,
    "X-Atlassian-Read-Only-Mode": X_ATLASSIAN_READ_ONLY_MODE,
    "X-Atlassian-Enable-Xray": X_ATLASSIAN_ENABLE_XRAY,
}
# Drop unset headers so we don't send empty/None values to the MCP server.
MCP_HEADERS = {k: v for k, v in _RAW_MCP_HEADERS.items() if v}

DEFAULT_SYSTEM_PROMPT = (
    "You are a DQ automation assistant for J&J's data quality pipeline. "
    "You have access to Atlassian Jira tools via MCP to search, read, and diagnose "
    "JIRA tickets (JGPV data-quality tickets and JEJQ upstream ETL tickets). "
    "Use the available tools to gather evidence before answering, and cite ticket "
    "keys when referencing evidence. Be concise."
)


def _require_strands() -> None:
    if Agent is None or MCPClient is None or streamablehttp_client is None:
        raise ImportError(
            "The 'strands-agents' and 'mcp' packages are required for the Agent/MCP "
            "workflow. Install them (e.g. `pip install strands-agents mcp`) to use "
            "strands_agent.py."
        ) from _IMPORT_ERROR


def get_atlassian_mcp_client(tool_filters: Optional[Any] = None) -> "MCPClientType":
    """Create an MCPClient connected to the Atlassian MCP server over streamable HTTP.

    Args:
        tool_filters: Optional ``strands.tools.mcp.ToolFilters`` restricting which
            tools are loaded, e.g. ``{"allowed": ["jira_get_issue", "jira_search"]}``.
            Recommended for automated pipelines to keep write tools out of reach.
    """
    _require_strands()
    if not MCP_ATLASSIAN_URL:
        raise ValueError("MCP_ATLASSIAN_URL is not configured")

    return MCPClient(
        lambda: streamablehttp_client(MCP_ATLASSIAN_URL, headers=MCP_HEADERS),
        tool_filters=tool_filters,
    )


def get_model(**overrides: Any) -> JNJClaudeGatewayModel:
    """Build the J&J GenAI Gateway model provider used by the agent (Claude via api key)."""
    return JNJClaudeGatewayModel(**overrides)


def build_agent(
    mcp_client: "MCPClientType",
    system_prompt: Optional[str] = None,
    model: Optional[Any] = None,
    **agent_kwargs: Any,
) -> "AgentType":
    """Build a strands Agent wired to the given MCPClient's tools.

    Passes the MCPClient itself as a tool provider (rather than calling
    ``list_tools_sync()`` once) so the Agent's ToolRegistry paginates through
    *all* of the server's tools (``list_tools_sync()`` alone only returns a
    single page) and manages the client's start/stop lifecycle. Call
    ``agent.cleanup()`` when done with the agent to close the MCP connection,
    or use ``atlassian_agent_session()`` which does this for you.
    """
    _require_strands()

    return Agent(
        model=model or get_model(),
        tools=[mcp_client],
        system_prompt=system_prompt or DEFAULT_SYSTEM_PROMPT,
        **agent_kwargs,
    )


@contextmanager
def atlassian_agent_session(
    system_prompt: Optional[str] = None,
    model: Optional[Any] = None,
    **agent_kwargs: Any,
) -> Iterator["AgentType"]:
    """Context manager yielding an Agent with live Atlassian MCP tools.

    Example:
        with atlassian_agent_session() as agent:
            result = agent("Search JEJQ tickets mentioning table X")
    """
    mcp_client = get_atlassian_mcp_client()
    agent = build_agent(mcp_client, system_prompt=system_prompt, model=model, **agent_kwargs)
    try:
        yield agent
    finally:
        agent.cleanup()


def diagnose_with_agent(prompt: str, system_prompt: Optional[str] = None) -> str:
    """Convenience one-shot helper: run a prompt through the Atlassian-aware agent."""
    with atlassian_agent_session(system_prompt=system_prompt) as agent:
        result = agent(prompt)
    return str(result)


def list_registered_tools() -> list[str]:
    """Ground-truth tool names actually registered on the Agent (not an LLM summary).

    Use this instead of asking the agent in natural language to "list your tools",
    since the model may summarize/group a large tool list rather than enumerate it.
    """
    with atlassian_agent_session() as agent:
        names = sorted(agent.tool_names)
    return names


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    tool_names = list_registered_tools()
    print(f"Registered tools ({len(tool_names)}): {tool_names}")
    print(diagnose_with_agent("List the Atlassian tools you have access to and what they do."))
