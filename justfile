set dotenv-load := false

# Generate .env secrets if missing, build image, start stack with local Postgres + Redis
local_up:
    #!/usr/bin/env bash
    set -euo pipefail
    python3 scripts/gen_local_env.py
    docker compose -f docker-compose.yml -f docker-compose.override.yml build
    docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
    echo ""
    echo "Airflow UI → http://localhost:8080  (check .env for admin password)"

# Run Airflow locally using the production image (Dockerfile.prod) + local Postgres/Redis
airflow:
    #!/usr/bin/env bash
    set -euo pipefail
    python3 scripts/gen_local_env.py
    VERSION=$(jq -r .adapter.version dwe-state.json)
    echo "Building from pondered/airflow-dwe:$VERSION"
    AIRFLOW_DWE_VERSION=$VERSION docker compose -f docker-compose.prod.yml -f docker-compose.override.yml up -d --build
    echo ""
    echo "Airflow UI → http://localhost:8080  (check .env for admin password)"

# Build Dockerfile.prod (version from dwe-state.json) and start production stack
prod_up:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION=$(jq -r .adapter.version dwe-state.json)
    echo "Building from pondered/airflow-dwe:$VERSION"
    AIRFLOW_DWE_VERSION=$VERSION docker compose -f docker-compose.prod.yml up -d --build

# Stop production stack
prod_down:
    docker compose -f docker-compose.prod.yml down

# Stop stack, keep volumes
down:
    docker compose -f docker-compose.yml -f docker-compose.override.yml down

# Stop stack and wipe all volumes (fresh start)
reset:
    docker compose -f docker-compose.yml -f docker-compose.override.yml down -v

# Tail logs from all services (or pass a service name: just logs scheduler)
logs service="":
    docker compose -f docker-compose.yml -f docker-compose.override.yml logs -f {{ service }}

# Rebuild the image without cache
rebuild:
    docker compose -f docker-compose.yml -f docker-compose.override.yml build --no-cache

# Open a shell in the worker container
shell:
    docker compose -f docker-compose.yml -f docker-compose.override.yml exec airflow-worker bash
