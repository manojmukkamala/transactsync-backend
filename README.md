# TransactSync Backend

Backend agents that extract financial transactions and store them in the [TransactSync API](https://github.com/manojmukkamala/transactsync-api):

- **Email sync** — fetches new emails over IMAP, matches them against sender/subject rules, uses an LLM to extract transaction data, and saves it. Progress (last seen UID per folder) is tracked in the API.
- **Statement sync** — reads bank statement files, converts them to markdown, uses an LLM to extract transactions, and saves them. Progress (checkpoint per folder/file) is tracked in the API.

## Requirements

- Python 3.13 (managed with [uv](https://docs.astral.sh/uv/))
- An LLM server with an OpenAI-compatible API (Ollama, vLLM, LM Studio, ...)
- A running TransactSync API
- IMAP access for the email flow

## Setup

```bash
uv sync
```

Create a `.env` file (see Configuration below).

## Configuration

All configuration is via environment variables (loaded from `.env`):

| Variable            | Required | Description                                                          | Default                  |
| ------------------- | -------- | -------------------------------------------------------------------- | ------------------------ |
| `SOURCE`            | yes      | Data source: `email` or `statement`                                  | `email`                  |
| `API_HOST`          | yes      | TransactSync API base URL                                            | `http://127.0.0.1:8000`  |
| `API_HEADERS`       | no       | API headers as a JSON string (e.g. `{"x-api-key": "..."}`)           | —                        |
| `MODEL_HOST`        | no       | Base URL of an OpenAI-compatible LLM server                          | `http://localhost:11434/v1` |
| `MODEL_NAME`        | no       | LLM model name                                                       | `qwen3:8b`               |
| `MODEL_API_KEY`     | no       | LLM server API key (dummy key is used for local servers)             | —                        |
| `EMAIL_HOST`        | email    | IMAP server address                                                  | —                        |
| `EMAIL_PORT`        | email    | IMAP server port                                                     | `143`                    |
| `EMAIL_USERNAME`    | email    | Email account username                                               | —                        |
| `EMAIL_PASSWORD`    | email    | Email account password                                               | —                        |
| `EMAIL_FOLDER`      | no       | Email folder to process                                              | `INBOX`                  |
| `TRANSACTION_RULES` | email    | Path to the transaction rules YAML file                              | `/rules/transaction_rules.yaml` |
| `PROMPT_FILE`       | yes      | Path to the LLM prompt template                                      | `/rules/prompt.txt`      |
| `STATEMENT_FILE`    | statement| Path to a single statement file (alternative to `STATEMENT_FOLDER`)  | —                        |
| `STATEMENT_FOLDER`  | statement| Folder containing statement files (alternative to `STATEMENT_FILE`)  | `/data/statements`       |

Example rules and prompts are in `examples/`.

## Usage

```bash
# Extract transactions from new bank emails
uv run python main.py --source email

# Extract transactions from statement files
uv run python main.py --source statement
```

## Docker

Pre-built images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/manojmukkamala/transactsync-backend:2.1
docker run --env-file .env ghcr.io/manojmukkamala/transactsync-backend:2.1 --source email
```

The default source is `email`; pass `--source statement` (or any other `main.py` argument) to override it.

Or build from source:

```bash
docker build -t transactsync-backend .
docker run --env-file .env transactsync-backend --source email
```

Note: the already-published `:2.1` image predates the `ENTRYPOINT` split, so for that tag pass the full command instead, e.g. `docker run --env-file .env ghcr.io/manojmukkamala/transactsync-backend:2.1 uv run /workspace/main.py --source email`.

## Project structure

```
.
├── main.py                 # Entry point (email / statement)
├── app/
│   ├── email_sync.py       # IMAP fetch → rule match → LLM extract → save
│   ├── statement_sync.py   # File read → convert → LLM extract → save
│   └── utils/
│       ├── api_client.py   # TransactSync REST API client
│       ├── email_client.py # IMAP client
│       ├── llm_client.py   # OpenAI-compatible LLM client
│       ├── rule_parser.py  # Sender/subject rule matching
│       └── fs_client.py    # Statement file discovery
└── examples/               # Example prompts, rules, and seed data
```
