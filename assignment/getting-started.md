# Getting started

## What is already provided

You receive:

- a reproducible local platform with object storage, PostgreSQL, a database
  browser, and JupyterLab;
- a network-free dataset release;
- Python dependencies and connection helpers;
- tested low-level readers for the supplied Stim `b8` and `01` representations;
- helpers that assign the course partitions, construct the documented model
  inputs, unpack bits, and calculate weighted logical-error rate;
- simple pipeline-stage templates and basic utility tests;
- the assignment brief, source, Parquet, and Silver-table guides, primer,
  Part II guide, rubric, FAQ, and checklist.

You do not receive the cleaning transformations, discovered relationships,
relational business schema, final pipeline, feature builders, or trained models.

## Requirements

- Docker Desktop or Docker Engine with Compose;
- at least 8 GB RAM available to Docker;
- approximately 5 GB free disk space;
- a Bash-compatible terminal (Windows students must install Bash if it is not already available); and
- GNU Make, or the ability to run the equivalent Docker Compose commands.

No Kaggle, IBM Quantum, or cloud account is required.

### Windows: use WSL Bash, not PowerShell

The supplied commands and Makefile assume a Bash-compatible environment. On
Windows, **PowerShell alone does not work for the supplied assignment commands**.
Bash is an additional setup requirement. The recommended route is Windows
Subsystem for Linux (WSL 2) with Ubuntu, which includes Bash. If you already
have Ubuntu running in WSL 2, skip the installation and check Docker integration:

1. Open PowerShell **as Administrator**, run `wsl --install`, and restart if
   Windows asks you to. See Microsoft's [WSL installation
   guide](https://learn.microsoft.com/windows/wsl/install).
2. Open the installed Ubuntu application. From this point onward, run the
   assignment commands in this Ubuntu/WSL terminal.
3. In Docker Desktop, enable the WSL 2 engine and integration for the installed
   distribution. See Docker's [WSL 2 backend
   guide](https://docs.docker.com/desktop/features/wsl/).
4. Install Make inside Ubuntu as shown below.

In the Ubuntu terminal, confirm that the tools are available before starting:

```bash
bash --version
make --version
docker compose version
```

Windows Terminal is a terminal application, not a shell. Select its Ubuntu/WSL
profile, rather than its PowerShell profile, when running this assignment.

Keeping the extracted assignment in the WSL Linux file system, for example
under `~/assignment-1/`, generally avoids Windows/Linux path and permission
problems. PowerShell is needed only for the initial WSL installation command.

### Installing Make

Check first by running `make --version` in a terminal. If the command is not
found:

- **macOS:** run `xcode-select --install`. Apple's Command Line Tools include
  `make`. See the [Apple developer tools
  instructions](https://developer.apple.com/xcode/resources/).
- **Windows/WSL Ubuntu:** open the Ubuntu terminal and run
  `sudo apt update && sudo apt install -y make`.
- **Ubuntu/Debian Linux:** run `sudo apt update && sudo apt install make`.
- **Fedora/RHEL Linux:** run `sudo dnf install make`.

If software installation is restricted, open the `Makefile` and run the
corresponding `docker compose` command shown for the target you need. GNU Make
documentation is available from the [GNU Make
manual](https://www.gnu.org/software/make/manual/).

The complete release contains exactly three read-only input objects, placed
in the `bronze` area under `bronze/source=qec_syndromes`,
`bronze/source=google_qec`, and `bronze/source=qasmbench`.

Part I moves that data through Bronze, Silver, Gold, and ML. Read the
architecture section of [the brief](brief.md) and the exact Silver contracts in
[silver-tables.md](silver-tables.md) before your first working session.

## First startup

In your Bash-compatible terminal, open the extracted `assignment-1/` folder
(or `infrastructure/` when working from the full repository):

```bash
make bootstrap
```

The first run downloads the platform images, so it can take several minutes.
The command verifies the supplied data bundle, starts the services, copies the
unchanged source files into the `bronze` zone, and checks the environment.

Open:

- JupyterLab: <http://localhost:8888/lab?token=quantum-course>
- object-store console: <http://localhost:9001>
- database browser: <http://localhost:8080>

Default local credentials are documented in `.env.example`. They are only for
the isolated course environment.

## Verify the starter

In JupyterLab, open a terminal:

```bash
make check
make inventory
make test
make train
```

`make run` and `make train` initially report that their orchestration is
unimplemented. Replacing both boundaries is part of the assignment.

## Everyday commands

From `infrastructure/`:

```bash
make ps
make logs
make verify
make down
```

`make down` retains your object-store and database volumes. The
`make reset-platform` command removes those volumes and therefore deletes the
generated `silver`, `gold`, and `ml` zones. It does not remove `bronze`, the
source archives, or your code.

## If a port is already in use

Copy `.env.example` to `.env` and change the host-side port, for example:

```text
POSTGRES_PORT=55432
JUPYTER_PORT=18888
```

Container-to-container connection values do not change. Restart the platform
after editing `.env`.

## Recommended first working session

1. Run the connection and starter tests.
2. List stored file paths and archive members without extracting everything.
3. Validate one syndrome sequence, one `b8` record, and one `01` file using
   tiny samples before attempting full processing.
4. Record source sizes, record counts, columns/data types, and possible IDs.
5. Create a discovery notebook and a plain Markdown decision log.
6. Review the six required Silver tables and write one sentence describing what
   a row represents in every planned Gold relation.
7. Implement one small end-to-end example: one record from `bronze` through
   `silver` and `gold` into `ml`.


## Recommended transition to Part II

1. Run Part I twice and record the hashes of the two `ml` tables.
2. Reconcile 75,598 syndrome examples to 70 million weighted observations and
   250,000 Google examples to the source shot counts.
3. Test example uniqueness, bit order, feature dimensions, and the supplied
   partition values.
4. Run the weighted prior baseline end to end and save its metrics.
5. Add one model at a time without changing the test split.
6. Run the raw-detector prototype only after the decoder-combination experiment
   works end to end.

Part II is a consumer test for the pipeline. You are not graded on reaching a
particular model score or beating a supplied decoder.
