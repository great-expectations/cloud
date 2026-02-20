# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the GX Cloud Agent repository - a Python-based agent that processes events from Great Expectations Cloud. The agent connects to RabbitMQ, receives events, and executes actions like running checkpoints, generating expectations, and listing data assets.

> **Important:** gx-agent and gx-runner are mutually exclusive. When running locally via
> Nexus, the `forceEnableAgent` LaunchDarkly flag controls which runs (`true` = agent, `false` = runner).
> Toggle at http://localhost:8765/ui.

## Development Commands

**Setup:**
- `poetry run invoke deps` - Install dependencies (runs `poetry sync --with dev`)
- `pre-commit install` - Set up pre-commit hooks
- Decrypt secrets: `assume dev && sops -d encrypted.env > .env`

> **Venv:** Uses Poetry (not a local `.venv`). Always prefix with `poetry run` — do not activate the venv manually.

**Testing:**
- `poetry run pytest` (with `--cov=great_expectations_cloud` by default)
- Tests marked with `@pytest.mark.unit` or `@pytest.mark.integration`

**Code Quality:**
- `poetry run invoke lint` - Lint with ruff (add `--check` to not auto-fix)
- `poetry run invoke fmt` - Format with ruff format (add `--check` to not auto-fix)
- `poetry run invoke type-check` - Run mypy type checking
- `poetry run ruff check . --fix` - Fix linting issues
- `poetry run ruff format .` - Format code

**Docker:**
- `poetry run invoke docker` - Build the Docker image
- `poetry run invoke docker --run` - Run the agent in Docker (requires .env file with credentials)
- `poetry run invoke docker --check` - Lint Dockerfile with hadolint

**Version Management:**
- `poetry run invoke version` - Print current version
- `poetry run invoke pre-release` - Bump pre-release version (YYYYMMDD.X.devY format)
- `poetry run invoke release` - Bump release version (YYYYMMDD.X format)

**Local Development:**
- `poetry run invoke start-supporting-services` - Start docker-compose services (DB, RabbitMQ, Mercury API)
- `poetry run gx-agent` - Run agent locally (requires `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID` env vars)
- `poetry run gx-agent --log-level DEBUG` - Run with debug logging

**All invoke tasks:** Run `poetry run invoke --list` to see available commands.

## Architecture

### Event-Driven System
The agent is an event-driven system that:
1. Connects to RabbitMQ queue via AMQP
2. Receives events from GX Cloud
3. Maps events to actions based on GX Core major version
4. Executes actions using CloudDataContext
5. Returns results to GX Cloud API

### Key Components

**Agent (`great_expectations_cloud/agent/agent.py`):**
- Main `GXAgent` class orchestrates event processing
- Manages RabbitMQ connection and message handling
- Validates organization_id to prevent cross-org data leaks
- Uses asyncio for message processing with ThreadPoolExecutor for actions

**Event Handler (`great_expectations_cloud/agent/event_handler.py`):**
- Core business logic mapping events to actions
- Version-specific event registration system via `register_event_action(version, event_type, action_class)`
- Actions are registered for specific GX Core major versions (e.g., "1" for v1.x.x)
- Falls back to `UnknownEventAction` for unrecognized events

**Actions (`great_expectations_cloud/agent/actions/`):**
- Each action is in a separate file
- All actions inherit from `AgentAction[EventT]` base class
- Must implement `run(event, id) -> ActionResult`
- Actions are registered at module import time via `register_event_action()`
- All actions imported in `actions/__init__.py` to ensure registration

**Models (`great_expectations_cloud/agent/models.py`):**
- Pydantic v2 models for events and data structures
- `EventBase` is base class for all events
- `DomainContext` encapsulates organization_id and workspace_id

### Configuration
- Uses Pydantic BaseSettings for environment variable validation
- Required env vars: `GX_CLOUD_ACCESS_TOKEN`, `GX_CLOUD_ORGANIZATION_ID`
- Optional: `GX_CLOUD_BASE_URL`, `AMQP_HOST_OVERRIDE`, `AMQP_PORT_OVERRIDE`
- **Important:** Do NOT use `os.environ` directly - use Pydantic BaseSettings models (enforced by ruff rule TID252)

## Gotchas

- **`.env` is never auto-loaded** — `invoke` tasks and `poetry run` manage the venv but don't source `.env`. For local runs or tests needing env vars: `. .env && poetry run pytest` or `. .env && invoke <task>`
- **No Makefile** — all task management is via `invoke`. Run `invoke --list` to see all available tasks.

## Important Patterns

### Adding a New Action
1. Create new file in `great_expectations_cloud/agent/actions/`
2. Define Event model in `models.py` (or use existing)
3. Create Action class inheriting from `AgentAction[YourEventType]`
4. Implement `run(event, id) -> ActionResult` method
5. Register action: `register_event_action("1", YourEvent, YourAction)` (for GX Core v1)
6. Import action in `actions/__init__.py` to trigger registration

### Code Style
- Strict mypy configuration (`strict = true`)
- Use `from __future__ import annotations` for forward references
- Line length: 100 characters
- Use pathlib over os.path operations (PTH rules)
- Timezone-aware datetimes required (DTZ rules)
- Banned: `os.environ`, `great_expectations.compatibility.*`

### Git Workflow
- **No co-authorship attribution** in commit messages (no `Co-Authored-By` lines)
- **No attribution footers** in PR descriptions (no "Generated with Claude Code" lines)

### PR Descriptions
Write in plain prose — no markdown headers, no structured sections like "## Summary" or "## Test Plan". Explain the **why** first (the problem or motivation), then briefly describe what changed. Use bullet points only when there are several distinct independent changes.

## Project Structure

```
great_expectations_cloud/
├── agent/
│   ├── actions/          # Event handlers (one per file)
│   ├── message_service/  # RabbitMQ client and subscriber
│   ├── agent.py          # Main GXAgent class
│   ├── cli.py            # CLI entry point (gx-agent command)
│   ├── config.py         # Environment variable configuration
│   ├── event_handler.py  # Event-to-action mapping
│   └── models.py         # Pydantic models for events/data
├── logging/              # Custom logging configuration
└── ...
tests/
├── agent/                # Unit tests for agent code
└── integration/          # Integration tests (require cloud services)
```

