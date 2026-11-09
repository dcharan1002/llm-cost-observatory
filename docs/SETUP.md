# Phase 0 — Setup Guide

Follow these steps once. When everything is checked, Phase 0 is done.

## 1. Install the tools (all free)

| Tool | Why | Check it works |
|---|---|---|
| [Git](https://git-scm.com/downloads) | Version control | `git --version` |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Runs Redpanda, Dagster, etc. | `docker --version` and `docker compose version` |
| [Python 3.12](https://www.python.org/downloads/) | Pipeline + backend | `python3 --version` (Windows: `py --version`) |
| [Node.js LTS](https://nodejs.org/) | Next.js frontend | `node --version` |
| [VS Code](https://code.visualstudio.com/) | Editor | — |
| [Ollama](https://ollama.com/) *(optional, Phase 6)* | Free local LLM | `ollama --version` |

**Windows users:** install Docker Desktop with the WSL 2 backend, and run the project inside WSL (Ubuntu) so `make` works. Alternatively install `make` via `choco install make`.

## 2. Create the GitHub repository

1. On GitHub: **New repository** → name it `llm-cost-observatory`.
2. Set it to **Public** (GitHub Actions is free for public repos).
3. **Don't** add a README, .gitignore or license — this starter kit already has them.

## 3. Add the starter files and push

Unzip the starter kit, then from inside the `llm-cost-observatory` folder:

```bash
git init
git add .
git commit -m "chore: project scaffold (phase 0)"
git branch -M main
git remote add origin https://github.com/dcharan1002/llm-cost-observatory.git
git push -u origin main
```

## 4. Set up the Python environment

```bash
make setup
source .venv/bin/activate        # Windows (no WSL): .venv\Scripts\activate
```

This creates `.venv/`, installs dev tools, copies `.env.example` → `.env`, and creates the `data/` folder.

## 5. Personalize

- [ ] Replace `dcharan1002` in `README.md`
- [ ] Replace `[Your Name]` in `LICENSE`
- [ ] Add a repo description and topics on GitHub: `data-engineering`, `llm`, `finops`, `kafka`, `dbt`, `duckdb`, `dagster`, `fastapi`, `nextjs`

## ✅ Phase 0 checklist

- [ ] All tools installed and version commands work
- [ ] Docker Desktop is running
- [ ] Repo created and first commit pushed
- [ ] `make setup` finished without errors
- [ ] README personalized

**Next:** Phase 1 — synthetic data generator.
