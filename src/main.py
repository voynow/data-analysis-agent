import asyncio
import sqlite3
from typing import Any, Dict, List

from llm import get_completion


def get_table_metadata() -> List[Dict[str, Any]]:
    """
    Get metadata for all tables: name, row count, and column names

    :return: List of dictionaries, each containing table metadata
    """

    conn = sqlite3.connect("financial.db")
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    metadata = []
    for table in tables:
        if table == "sqlite_sequence":
            continue

        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]

        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall()]

        # Get example data
        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
        example_data = cursor.fetchall()

        metadata.append(
            {
                "table_name": table,
                "row_count": count,
                "columns": columns,
                "example_data": example_data,
            }
        )

    conn.close()
    return metadata


async def main():
    query = "What do you think about this database?"
    propmt = f"""# Role
You are a junior quant data analyst.
    
# Database
Heres some metadata about the database:
{get_table_metadata()}

# Task
A teammate has asked you to answer the following question:
{query}

Please answer the question in a concise manner."""

    response = await get_completion(
        message=propmt,
        model="gpt-4.1",
    )

    print(response)


if __name__ == "__main__":
    asyncio.run(main())
