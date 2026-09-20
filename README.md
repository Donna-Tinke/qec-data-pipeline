# Student infrastructure

This folder is the distributable student environment. It provides:

- MinIO object storage;
- PostgreSQL 16 and Adminer;
- JupyterLab and a pinned Python data-engineering environment;
- a fixed, network-free QEC dataset bundle;
- connection helpers, simple stage templates, and basic starter tests;
- the two-part assignment, source/Silver/ML-table guides, QEC primer, FAQ,
  rubric, and submission checklist.

Before starting, install Docker and Make and open a **Bash-compatible terminal**.
On Windows, install Bash through WSL 2 with Ubuntu if needed, and run the
assignment in the Ubuntu terminal. PowerShell alone does not support the
supplied commands. Follow the [Windows setup steps](assignment/getting-started.md#windows-use-wsl-bash-not-powershell), including Docker Desktop's WSL integration.

From this directory, run:

```bash
make bootstrap
```

On success, the command prints the three browser addresses. The first run
downloads container images and therefore takes longer. Use `make help` for
daily operations, verification, shutdown, and the explicit destructive reset
command.

The student handout starts at `assignment/README.md`; the coding workspace
starts at `starter/README.md`. The three assignment datasets are already
included, so students do not need Kaggle or other dataset-service credentials.
