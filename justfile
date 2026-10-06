set dotenv-load := false

# Generate .env if missing, build image, start stack with local Postgres + Redis
local_up:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f .env ]; then
      echo "Creating .env with generated secrets..."
      FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
      JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
      {
        echo "AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}"
        echo "AIRFLOW__API_AUTH__JWT_SECRET=${JWT_SECRET}"
        echo "_AIRFLOW_WWW_USER_USERNAME=airflow"
        echo "_AIRFLOW_WWW_USER_PASSWORD=airflow"
        echo "AIRFLOW_UID=50000"
        echo ""
        echo "# Trino connection (local — no auth):"
        echo "# AIRFLOW_CONN_DWE=trino://user@localhost:8080/hive"
      } > .env
      echo ".env created."
    fi
    docker compose -f docker-compose.yml -f docker-compose.override.yml build
    docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
    echo ""
    echo "Airflow UI → http://localhost:8080  (airflow / airflow)"

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
