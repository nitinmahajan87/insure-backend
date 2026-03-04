#!/bin/bash
set -e

echo "⏳ Waiting for PostgreSQL to be ready..."
until python -c "
import os, psycopg2
url = os.environ.get('ALEMBIC_DATABASE_URL', 'postgresql://admin:password123@insurtech_db:5432/insurtech_gateway')
psycopg2.connect(url)
" 2>/dev/null; do
    echo "   PostgreSQL not ready — retrying in 2s..."
    sleep 2
done
echo "✅ PostgreSQL is ready."

echo "🔄 Running Alembic migrations..."
alembic upgrade head
echo "✅ Migrations complete."

echo "🚀 Starting FastAPI server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
