#!/usr/bin/env python3
"""
Interactive DuckDB query tool for exploring the taxi data.
Usage: python scripts/query.py "SELECT * FROM yellow_taxi_materialized LIMIT 5"
"""

import sys
import duckdb
from pathlib import Path
from tabulate import tabulate

DB_PATH = Path("/app/data/taxi.duckdb")

def main():
    if not DB_PATH.exists():
        print("Database not found! Run setup_tables.py first.")
        sys.exit(1)
    
    con = duckdb.connect(str(DB_PATH))
    
    if len(sys.argv) > 1:
        # Run query from command line
        query = " ".join(sys.argv[1:])
        try:
            result = con.execute(query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            
            if rows:
                print(tabulate(rows, headers=columns, tablefmt="pretty"))
                print(f"\n({len(rows)} rows)")
            else:
                print("Query executed successfully. No rows returned.")
        except Exception as e:
            print(f"Error: {e}")
    else:
        # Interactive mode
        print("DuckDB Interactive Query Tool")
        print("=" * 50)
        print("Tables available:")
        tables = con.execute("SHOW TABLES").fetchall()
        for t in tables:
            print(f"  - {t[0]}")
        print("\nType your SQL query (or 'exit' to quit):")
        print()
        
        while True:
            try:
                query = input("duckdb> ").strip()
                if query.lower() in ('exit', 'quit', 'q'):
                    break
                if not query:
                    continue
                
                result = con.execute(query)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                
                if rows:
                    # Limit display for large results
                    display_rows = rows[:100]
                    print(tabulate(display_rows, headers=columns, tablefmt="pretty"))
                    if len(rows) > 100:
                        print(f"... showing 100 of {len(rows)} rows")
                    else:
                        print(f"({len(rows)} rows)")
                else:
                    print("OK")
                print()
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}\n")
    
    con.close()

if __name__ == "__main__":
    main()
