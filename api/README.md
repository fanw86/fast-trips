# Transit Assignment API

REST API for running transit assignment simulations.

## Prerequisites

- Python 3.11+
- Virtual environment set up at `/home/wf/fast-trips/.venv`
- Required dependencies installed (fastapi, uvicorn, python-multipart)

## Starting the Server

### Method 1: Using uvicorn (Recommended)

From the fast-trips root directory:

```bash
cd /home/wf/fast-trips
.venv/bin/python -m uvicorn api.server:app --host 0.0.0.0 --port 8000
```

### Method 2: Direct Python execution

```bash
cd /home/wf/fast-trips
.venv/bin/python api/server.py
```

## Verifying the Server is Running

Once started, you should see:

```
INFO:     Started server process [XXXXX]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

Visit http://localhost:8000/docs to access the interactive API documentation.

## Stopping the Server

Press `CTRL+C` in the terminal where the server is running.

## Configuration

### Port Configuration

To run on a different port:

```bash
.venv/bin/python -m uvicorn api.server:app --host 0.0.0.0 --port 8080
```

### Run Directory

By default, uploaded scenarios are stored in `api/runs/`. To change this, set the environment variable:

```bash
export FASTTRIPS_API_RUN_DIR=/path/to/runs
.venv/bin/python -m uvicorn api.server:app --host 0.0.0.0 --port 8000
```

### Development Mode (Auto-reload)

For development, enable auto-reload when files change:

```bash
.venv/bin/python -m uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Main Endpoints

**Create a Run**
```
POST /runs
```

**List All Runs**
```
GET /runs
```

**Get Run Status**
```
GET /runs/{run_id}
```

**Get Run Logs**
```
GET /runs/{run_id}/log?log_type=info&lines=200
```

**List Result Files**
```
GET /runs/{run_id}/files
```

**Download a Specific Result File**
```
GET /runs/{run_id}/files/{file_path}
```

**Download All Results as ZIP**
```
GET /runs/{run_id}/download
```

**Stop a Run**
```
POST /runs/{run_id}/stop
```

**Upload Scenario (Zip File)**
```
POST /scenario/upload
```

**Get Scenario Status**
```
GET /scenario/{scenario_id}/status
```

## Testing the API

### Using the Interactive Docs

1. Start the server
2. Navigate to http://localhost:8000/docs
3. Click on any endpoint to expand it
4. Click "Try it out"
5. Fill in the required parameters
6. Click "Execute"

### Using curl

Upload a scenario:
```bash
curl -X POST "http://localhost:8000/scenario/upload" \
  -F "scenarioId=test_scenario" \
  -F "needFile=@/path/to/scenario.zip"
```

Check scenario status:
```bash
curl "http://localhost:8000/scenario/test_scenario/status"
```

### Using Postman or Other Tools

Any HTTP client can be used to test the API:
- Postman (https://www.postman.com/)
- Insomnia (https://insomnia.rest/)
- Thunder Client (VS Code extension)
- HTTPie (command-line tool)

## Troubleshooting

### ModuleNotFoundError: No module named 'api'

Make sure you run the server from the `/home/wf/fast-trips` directory (the parent of the api folder), not from within the api folder itself.

### Port Already in Use

If port 8000 is already in use, either:
- Stop the existing process using that port
- Run the server on a different port using `--port 8080`

### Import Errors

Ensure the virtual environment is activated and all dependencies are installed:

```bash
.venv/bin/pip install fastapi uvicorn python-multipart
```
