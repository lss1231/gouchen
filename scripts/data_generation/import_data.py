"""Main script to import all test data."""
import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine, text

from src.config import get_settings


def execute_sql_file(engine, filepath: str):
    """Execute SQL file.

    Args:
        engine: SQLAlchemy engine instance
        filepath: Path to SQL file
    """
    print(f"Executing SQL file: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Split by semicolon and execute each statement
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]

    with engine.connect() as conn:
        for i, statement in enumerate(statements, 1):
            # Skip DELIMITER statements and comments
            if statement.upper().startswith('DELIMITER'):
                continue
            if statement.startswith('--') or statement.startswith('/*'):
                continue

            try:
                conn.execute(text(statement))
                print(f"  Statement {i}/{len(statements)} executed")
            except Exception as e:
                print(f"  Warning: Statement {i} failed: {e}")
                # Continue with other statements

        conn.commit()

    print(f"Completed: {filepath}")


def import_mysql_data():
    """Import data to MySQL."""
    settings = get_settings()
    engine = create_engine(settings.mysql_url)

    print("=" * 60)
    print("Importing MySQL Data")
    print("=" * 60)

    # Execute SQL files (categories, dim_date, dim_region)
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'sql')
    sql_files = [
        '01_categories.sql',
        '02_dim_date.sql',
        '03_dim_region.sql',
    ]

    for sql_file in sql_files:
        filepath = os.path.join(sql_dir, sql_file)
        if os.path.exists(filepath):
            try:
                execute_sql_file(engine, filepath)
            except Exception as e:
                print(f"Error executing {sql_file}: {e}")
                raise
        else:
            print(f"Warning: SQL file not found: {filepath}")

    # Generate large tables
    print("\n" + "-" * 60)
    print("Generating users...")
    print("-" * 60)
    from scripts.data_generation.generate_users import main as gen_users
    gen_users()

    print("\n" + "-" * 60)
    print("Generating products...")
    print("-" * 60)
    from scripts.data_generation.generate_products import main as gen_products
    gen_products()

    print("\n" + "-" * 60)
    print("Generating orders...")
    print("-" * 60)
    from scripts.data_generation.generate_orders import main as gen_orders
    gen_orders()

    print("\n" + "=" * 60)
    print("MySQL Import Complete")
    print("=" * 60)


def import_doris_data():
    """Import data to Doris (from MySQL)."""
    print("=" * 60)
    print("Importing Doris Data")
    print("=" * 60)
    print("Note: This requires Doris tables to be created first")
    print("Doris data will be loaded from MySQL via INSERT INTO SELECT or ETL")
    print("")
    print("TODO: Implement Doris data import")
    print("This could be:")
    print("  1. INSERT INTO Doris SELECT * FROM MySQL (via FEDERATED or external table)")
    print("  2. Export MySQL to CSV, then Load into Doris")
    print("  3. Direct calculation for DWS/ADS layers")
    print("=" * 60)


def truncate_all_tables(engine):
    """Truncate all data tables before import.

    Args:
        engine: SQLAlchemy engine instance
    """
    print("Truncating existing data...")

    tables_to_truncate = [
        'order_items',
        'orders',
        'products',
        'users',
        'categories',
        'dim_date',
        'dim_region',
    ]

    with engine.connect() as conn:
        # Disable foreign key checks temporarily
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))

        for table in tables_to_truncate:
            try:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
                print(f"  Truncated: {table}")
            except Exception as e:
                print(f"  Warning: Could not truncate {table}: {e}")

        # Re-enable foreign key checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()

    print("Truncate complete.")


def main():
    parser = argparse.ArgumentParser(
        description='Import test data to MySQL and/or Doris databases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python import_data.py --target mysql     # Import only to MySQL
  python import_data.py --target doris     # Import only to Doris
  python import_data.py --target all       # Import to both (default)
  python import_data.py --target mysql --truncate  # Truncate before import
        """
    )
    parser.add_argument(
        '--target',
        choices=['mysql', 'doris', 'all'],
        default='all',
        help='Target database (default: all)'
    )
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate existing data before import (MySQL only)'
    )

    args = parser.parse_args()

    # MySQL import
    if args.target in ['mysql', 'all']:
        try:
            if args.truncate:
                settings = get_settings()
                engine = create_engine(settings.mysql_url)
                truncate_all_tables(engine)

            import_mysql_data()
        except Exception as e:
            print(f"\nError importing MySQL data: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    # Doris import
    if args.target in ['doris', 'all']:
        try:
            import_doris_data()
        except Exception as e:
            print(f"\nError importing Doris data: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print("\nAll imports completed successfully!")


if __name__ == '__main__':
    main()
