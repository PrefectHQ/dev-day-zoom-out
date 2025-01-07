# The Summit Dev Day repo

This is the repository for Summit Dev Day January 2025.

## Setup

Use the Python virtual environment manager of your choice.
We suggest using [uv](https://docs.astral.sh/uv/) because it's fast and relatively quick to set up, but feel free to use your preferred tool.
The examples below show uv. 
Ensure you have exited out of any other virtual environments before following the instructions below.


### Download the repository and navigate to the directory

```bash
git clone https://github.com/PrefectHQ/dev-day-zoom-out.git
cd dev-day-zoom-out
```

### Install uv (if needed)

For macOS/Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh  
```

For Windows:

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

For troubleshooting, see the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation).

### Create and activate a new virtual environment

Create a virtual environment with Python 3.12.

```bash
uv venv --python 3.12
```

You should see a note that the virtual environment was created successfully and instructions for how to activate it. Follow the instructions to activate the virtual environment.

```bash
source .venv/bin/activate
```

You should see the virtual environment name in parentheses in your terminal prompt .

### Install Prefect and the other Python packages you'll use during Dev Day

```bash
uv pip install -r requirements.txt
```

## Connect to Prefect Cloud

If you don't already have a Prefect Cloud account, sign up for a free account at [app.prefect.cloud](https://app.prefect.cloud).

If your command line is not already authenticated with Prefect Cloud, authenticate with

```bash
prefect cloud login
```

If you have any issues connecting to Prefect Cloud, see the [Connect to Prefect Cloud docs](https://docs.prefect.io/v3/manage/cloud/connect-to-cloud).

Alternatively, you can use a self-hosted Prefect server instance for the first part of Dev Day. See the [instructions for self-hosting a Prefect server](https://docs.prefect.io/v3/manage/self-host).
