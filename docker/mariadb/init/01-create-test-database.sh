#!/usr/bin/env bash
set -euo pipefail

mariadb --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD}" <<SQL
CREATE DATABASE IF NOT EXISTS social_insights_test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON social_insights_test.* TO '${MARIADB_USER}'@'%';
FLUSH PRIVILEGES;
SQL
