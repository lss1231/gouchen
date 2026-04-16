#!/usr/bin/env python3
"""Generate and execute CREATE TABLE statements from schema JSON for Doris."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pymysql
from src.config import get_settings

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.json')


def doris_type(json_type: str) -> str:
    """Map JSON schema type to Doris type."""
    t = json_type.strip().lower()
    # Most types map directly; handle a few edge cases
    if t == 'text':
        return 'STRING'
    if t == 'timestamp':
        return 'DATETIME'
    return json_type.upper()


def escape_comment(text: str) -> str:
    return text.replace("'", "''")


def generate_ddl(table: dict) -> str:
    table_name = table['table_name']
    fields = table['fields']
    lines = []
    for f in fields:
        ft = doris_type(f['data_type'])
        comment = escape_comment(f.get('description', ''))
        lines.append(f"    `{f['field_name']}` {ft} COMMENT '{comment}'")
    cols = ",\n".join(lines)
    cn_name = escape_comment(table.get('table_cn_name', table_name))
    ddl = f"""CREATE TABLE `{table_name}` (
{cols}
)
DUPLICATE KEY(`{fields[0]['field_name']}`)
COMMENT '{cn_name}'
DISTRIBUTED BY HASH(`{fields[0]['field_name']}`) BUCKETS 1
PROPERTIES ('replication_num' = '1');
"""
    return ddl


def main():
    settings = get_settings()
    conn = pymysql.connect(
        host=settings.doris_host, port=settings.doris_port,
        user=settings.doris_user, password=settings.doris_password,
        database=settings.doris_database, charset='utf8mb4'
    )
    cursor = conn.cursor()

    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    for table in schema['tables']:
        table_name = table['table_name']
        print(f"Creating {table_name} ...")
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
            ddl = generate_ddl(table)
            cursor.execute(ddl)
            conn.commit()
            print(f"  [OK] {table_name}")
        except Exception as e:
            print(f"  [ERR] {table_name}: {e}")

    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
