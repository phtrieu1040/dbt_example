# Local Prefect Setup Instructions

## Prerequisites
- Python 3.11.9 (using pyenv)
- Virtual environment `tevi_venv` already set up

## Setup Steps

1. **Activate the Virtual Environment**
   ```bash
   pyenv activate tevi_venv
   cd company_gitlab
   ```

2. **Verify Prefect Installation**
   ```bash
   pip list | grep prefect
   ```
   You should see prefect and related packages listed.

3. **Configure Prefect API URL**
   ```bash
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

4. **Start Prefect Server**
   ```bash
   prefect server start
   ```
   - The server will run on http://127.0.0.1:4200
   - You can view the dashboard at http://127.0.0.1:4200
   - API documentation is available at http://127.0.0.1:4200/docs

5. **Start Prefect Worker** (in a new terminal)
   ```bash
   pyenv activate tevi_venv
   cd company_gitlab
   prefect worker start -p 'default-agent-pool'
   ```

## Troubleshooting

1. **If port 4200 is already in use:**
   ```bash
   pkill -f "prefect server"
   ```
   Then try starting the server again.

2. **If 'prefect' command not found:**
   Make sure you're in the correct virtual environment:
   ```bash
   pyenv activate tevi_venv
   ```

3. **To stop the server:**
   Press `Ctrl+C` in the terminal where the server is running.

## Running Your First Flow

1. Make sure both the server and worker are running
2. Run your flow file:
   ```bash
   python flows/hello_telegram.py
   ```

## Important Notes
- Keep the server running while working with Prefect
- The worker needs to be running to execute your flows
- You can monitor flow runs in the Prefect dashboard
- Environment variables (like TELEGRAM_BOT_TOKEN) need to be set before running flows that use them 


source /Users/jacklucian/.pyenv/versions/3.11.9/envs/tevi_venv/bin/activate