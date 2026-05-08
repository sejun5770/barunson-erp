FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi
COPY . .
RUN npx prisma generate 2>/dev/null || true
RUN npm run build --if-present
EXPOSE 3000
CMD if [ -f prisma/schema.prisma ]; then DB_URL="${DATABASE_URL:-file:/app/data/database.db}"; sed -i '/^\s*url\s*=/d' prisma/schema.prisma; npx prisma db push --url "$DB_URL" --accept-data-loss 2>/dev/null || true; if [ -f prisma/seed.sql ]; then apk add --no-cache sqlite 2>/dev/null; sqlite3 "$(echo $DB_URL | sed s/file://)" < prisma/seed.sql 2>/dev/null || true; fi; fi && npm start
