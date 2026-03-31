"""Load transformed DataFrames into Oracle DB using oracledb (thin mode)."""

import logging

import oracledb
import pandas as pd
from config.settings import ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN

logger = logging.getLogger(__name__)


def _get_connection() -> oracledb.Connection:
    """Create an Oracle DB connection in thin mode."""
    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN,
    )


def _build_merge_sql(table: str, columns: list[str], key_col: str = "id") -> str:
    """Build an Oracle MERGE (upsert) statement.

    Merges on the key column; updates all other columns if matched,
    inserts a new row if not matched.
    """
    bind_cols = ", ".join(f":{c}" for c in columns)
    src_cols = ", ".join(f"src.{c}" for c in columns)
    tgt_cols = ", ".join(columns)
    update_set = ", ".join(
        f"tgt.{c} = src.{c}" for c in columns if c != key_col
    )

    sql = f"""
    MERGE INTO {table} tgt
    USING (
        SELECT {', '.join(f':{c} AS {c}' for c in columns)}
        FROM dual
    ) src
    ON (tgt.{key_col} = src.{key_col})
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({tgt_cols})
        VALUES ({src_cols})
    """
    return sql


def upsert_dataframe(
    df: pd.DataFrame, table: str, key_col: str = "id"
) -> int:
    """Upsert a DataFrame into an Oracle table.

    Args:
        df: The DataFrame to load.
        table: Target Oracle table name (e.g. "ACC_RFIS").
        key_col: Primary key column for MERGE ON clause.

    Returns:
        Number of rows processed.
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping Oracle load for %s", table)
        return 0

    # Columns already come uppercased from the transform step
    columns = list(df.columns)

    merge_sql = _build_merge_sql(table, columns, key_col)
    rows = df.where(pd.notna(df), None).to_dict("records")

    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(merge_sql, rows)
        conn.commit()
        logger.info("Upserted %d rows into %s", len(rows), table)
    finally:
        conn.close()

    return len(rows)
