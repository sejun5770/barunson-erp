#!/bin/sh
set -e

# DB가 없으면 시드 DB로 초기화
if [ ! -f "$DATA_DIR/orders.db" ]; then
  echo "orders.db not found, initializing..."
  if [ -f "/app/barunson-database-reference/user/seed.db" ]; then
    echo "Using seed.db (products/vendors/users included)..."
    cp /app/barunson-database-reference/user/seed.db "$DATA_DIR/orders.db"
    echo "seed.db copied → orders.db"
  else
    echo "seed.db not found, running init_db.js..."
    node barunson-database-reference/user/init_db.js
  fi
fi

exec "$@"
