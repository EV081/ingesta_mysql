import os
import sys
import time
import csv
from datetime import datetime
from typing import List, Optional

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

TABLES_ENV = os.getenv("TABLES", "")
TABLES: List[str] = [t.strip() for t in TABLES_ENV.split(",") if t.strip()]

CSV_SEP = os.getenv("CSV_SEP", ",")
CSV_QUOTE = os.getenv("CSV_QUOTE", "MINIMAL").upper()  
CSV_LINE_TERMINATOR = os.getenv("CSV_LINE_TERMINATOR", "\n")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "100000")) 
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/out")

# S3
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "")  
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

# Misc
TIMESTAMP = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def csv_quote_const(name: str):
    import csv as _csv
    return {
        "MINIMAL": _csv.QUOTE_MINIMAL,
        "ALL": _csv.QUOTE_ALL,
        "NONNUMERIC": _csv.QUOTE_NONNUMERIC,
        "NONE": _csv.QUOTE_NONE,
    }.get(name, _csv.QUOTE_MINIMAL)


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_engine():
    uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    return create_engine(
        uri,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"charset": "utf8mb4"},
    )


def table_exists(engine, table_name: str) -> bool:
    q = text("""
        SELECT COUNT(*) AS c
        FROM information_schema.tables
        WHERE table_schema = :db AND table_name = :tbl
    """)
    with engine.connect() as conn:
        r = conn.execute(q, {"db": MYSQL_DB, "tbl": table_name}).scalar()
        return (r or 0) > 0


def export_table_to_csv(engine, table_name: str, out_dir: str) -> str:
    """Stream de la tabla a CSV por chunks."""
    filename = f"{table_name}_{TIMESTAMP}.csv"
    out_path = os.path.join(out_dir, filename)

    quote = csv_quote_const(CSV_QUOTE)
    header_written = False
    row_count = 0

    query = f"SELECT * FROM `{table_name}`"
    for chunk in pd.read_sql(query, engine, chunksize=CHUNKSIZE):
        chunk.to_csv(
            out_path,
            mode="a",
            index=False,
            sep=CSV_SEP,
            quoting=quote,
            lineterminator=CSV_LINE_TERMINATOR,
            header=not header_written,
        )
        row_count += len(chunk)
        header_written = True

    print(f"[OK] {table_name} -> {out_path} ({row_count} filas)")
    return out_path


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_to_s3(local_path: str, bucket: str, prefix: Optional[str]) -> str:
    key = os.path.basename(local_path)
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    cli = s3_client()
    try:
        cli.upload_file(local_path, bucket, key)
    except (NoCredentialsError, PartialCredentialsError):
        print("[ERROR] Credenciales de AWS no encontradas o incompletas.", file=sys.stderr)
        raise
    except ClientError as e:
        print(f"[ERROR] Fallo subiendo a S3: {e}", file=sys.stderr)
        raise

    print(f"[OK] Subido a s3://{bucket}/{key}")
    return key


def main():
    if not MYSQL_DB or not MYSQL_USER or not MYSQL_PASSWORD or not TABLES:
        print(
            "Faltan variables de entorno obligatorias: MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD y TABLES.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not S3_BUCKET:
        print("Falta S3_BUCKET.", file=sys.stderr)
        sys.exit(1)

    ensure_output_dir(OUTPUT_DIR)
    engine = get_engine()

    exported_files = []
    for tbl in TABLES:
        if not table_exists(engine, tbl):
            print(f"[WARN] La tabla '{tbl}' no existe en {MYSQL_DB}. Se omite.")
            continue
        path = export_table_to_csv(engine, tbl, OUTPUT_DIR)
        exported_files.append(path)

    if not exported_files:
        print("[INFO] No se exportó ninguna tabla. Revisa nombres y permisos.", file=sys.stderr)
        sys.exit(2)

    for path in exported_files:
        upload_to_s3(path, S3_BUCKET, S3_PREFIX)

    print("[DONE] Ingesta completada.")


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    finally:
        dur = time.time() - start
        print(f"Duración total: {dur:.1f}s")
