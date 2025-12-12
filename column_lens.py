"""
Script to generate queries for comparing dashboard columns with system tables
and identify unused columns in upstream ETL pipelines.
"""

import json
from collections import defaultdict

def generate_column_comparison_queries(results, output_format='sql'):
    """
    Generate queries to check system tables for columns used in dashboards.
    
    Args:
        results: Output from extract_expressions function
        output_format: 'sql' for SQL queries or 'python' for Python/Spark code
    
    Returns:
        Dictionary with queries and analysis information
    """
    
    queries = {
        'table_columns_query': None,
        'lineage_query': None,
        'comparison_analysis': None,
        'unused_columns_query': None
    }
    
    # Get unique tables from dashboard
    dashboard_tables = list(results['by_table'].keys())
    
    if output_format == 'sql':
        queries['table_columns_query'] = generate_system_table_sql(results)
        queries['lineage_query'] = generate_lineage_sql(dashboard_tables)
        queries['unused_columns_query'] = generate_unused_columns_sql(results)
        queries['comparison_analysis'] = generate_comparison_sql(results)
    else:  # python/spark
        queries['table_columns_query'] = generate_system_table_python(results)
        queries['lineage_query'] = generate_lineage_python(dashboard_tables)
        queries['unused_columns_query'] = generate_unused_columns_python(results)
    
    return queries


def generate_system_table_sql(results):
    """
    Generate SQL query to fetch all columns from system.information_schema.columns
    for tables used in the dashboard.
    """
    
    tables_by_catalog = defaultdict(lambda: defaultdict(list))
    
    # Group tables by catalog and schema
    for table_name in results['by_table'].keys():
        parts = table_name.split('.')
        if len(parts) == 3:
            catalog, schema, table = parts
            tables_by_catalog[catalog][schema].append(table)
        elif len(parts) == 2:
            schema, table = parts
            tables_by_catalog['hive_metastore'][schema].append(table)
        else:
            tables_by_catalog['hive_metastore']['default'].append(table_name)
    
    # Generate WHERE clause for filtering
    conditions = []
    for catalog, schemas in tables_by_catalog.items():
        for schema, tables in schemas.items():
            if len(tables) == 1:
                conditions.append(
                    f"(table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{tables[0]}')"
                )
            else:
                table_list = "', '".join(tables)
                conditions.append(
                    f"(table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name IN ('{table_list}'))"
                )
    
    where_clause = "\n    OR ".join(conditions)
    
    sql = f"""-- Query 1: Get all columns from tables used in dashboard
SELECT 
    table_catalog,
    table_schema,
    table_name,
    CONCAT_WS('.', table_catalog, table_schema, table_name) as full_table_name,
    column_name,
    data_type,
    ordinal_position,
    is_nullable,
    column_default
FROM system.information_schema.columns
WHERE 
    {where_clause}
ORDER BY 
    table_catalog, 
    table_schema, 
    table_name, 
    ordinal_position;
"""
    return sql


def generate_lineage_sql(dashboard_tables):
    """
    Generate SQL query to get upstream table lineage using system.access.table_lineage.
    """
    
    # Create table list for WHERE clause
    table_conditions = []
    for table in dashboard_tables:
        parts = table.split('.')
        if len(parts) == 3:
            catalog, schema, table_name = parts
            table_conditions.append(
                f"(target_table_catalog = '{catalog}' AND target_table_schema = '{schema}' AND target_table_name = '{table_name}')"
            )
    
    where_clause = "\n    OR ".join(table_conditions) if table_conditions else "1=1"
    
    sql = f"""-- Query 2: Get upstream table lineage
WITH dashboard_tables AS (
    SELECT DISTINCT
        target_table_catalog,
        target_table_schema,
        target_table_name,
        CONCAT_WS('.', target_table_catalog, target_table_schema, target_table_name) as target_table_full_name
    FROM system.access.table_lineage
    WHERE 
        {where_clause}
),
upstream_lineage AS (
    SELECT 
        dt.target_table_full_name as dashboard_table,
        tl.source_table_catalog,
        tl.source_table_schema,
        tl.source_table_name,
        CONCAT_WS('.', tl.source_table_catalog, tl.source_table_schema, tl.source_table_name) as upstream_table_full_name,
        tl.source_type
    FROM dashboard_tables dt
    INNER JOIN system.access.table_lineage tl
        ON dt.target_table_catalog = tl.target_table_catalog
        AND dt.target_table_schema = tl.target_table_schema
        AND dt.target_table_name = tl.target_table_name
)
SELECT DISTINCT
    dashboard_table,
    upstream_table_full_name,
    source_type
FROM upstream_lineage
ORDER BY dashboard_table, upstream_table_full_name;
"""
    return sql


def generate_unused_columns_sql(results):
    """
    Generate SQL to identify columns in tables that are NOT used in dashboard.
    """
    
    # Build column usage per table
    table_columns_used = {}
    for table_name, entries in results['by_table'].items():
        columns = set()
        for entry in entries:
            columns.update(entry['columns_used'])
        table_columns_used[table_name] = sorted(list(columns))
    
    # Generate query for each table
    queries = []
    
    for table_name, used_columns in table_columns_used.items():
        parts = table_name.split('.')
        if len(parts) == 3:
            catalog, schema, table = parts
        elif len(parts) == 2:
            catalog = 'hive_metastore'
            schema, table = parts
        else:
            catalog = 'hive_metastore'
            schema = 'default'
            table = table_name
        
        # Create NOT IN clause for used columns
        used_columns_str = "', '".join(used_columns)
        
        query = f"""
-- Table: {table_name}
-- Columns used in dashboard: {', '.join(used_columns)}
SELECT 
    '{table_name}' as table_name,
    column_name,
    data_type,
    ordinal_position
FROM system.information_schema.columns
WHERE table_catalog = '{catalog}'
    AND table_schema = '{schema}'
    AND table_name = '{table}'
    AND column_name NOT IN ('{used_columns_str}')
ORDER BY ordinal_position
"""
        queries.append(query)
    
    sql = """-- Query 3: Find unused columns in dashboard tables
-- Columns that exist in tables but are NOT referenced in any dashboard widget

""" + "\nUNION ALL\n".join(queries) + ";"
    
    return sql


def generate_comparison_sql(results):
    """
    Generate a comprehensive comparison query showing dashboard usage vs available columns.
    """
    
    # Create CTE with dashboard column usage
    table_columns_used = {}
    for table_name, entries in results['by_table'].items():
        columns = set()
        for entry in entries:
            columns.update(entry['columns_used'])
        table_columns_used[table_name] = sorted(list(columns))
    
    # Build UNION query for dashboard columns
    dashboard_columns_cte = []
    for table_name, columns in table_columns_used.items():
        parts = table_name.split('.')
        if len(parts) == 3:
            catalog, schema, table = parts
        elif len(parts) == 2:
            catalog = 'hive_metastore'
            schema, table = parts
        else:
            catalog = 'hive_metastore'
            schema = 'default'
            table = table_name
        
        for col in columns:
            dashboard_columns_cte.append(
                f"    SELECT '{catalog}' as catalog, '{schema}' as schema, '{table}' as table_name, '{col}' as column_name"
            )
    
    union_query = "\n    UNION ALL\n".join(dashboard_columns_cte)
    
    sql = f"""-- Query 4: Comprehensive comparison - Dashboard columns vs System tables
WITH dashboard_columns AS (
{union_query}
),
system_columns AS (
    SELECT 
        table_catalog as catalog,
        table_schema as schema,
        table_name,
        column_name,
        data_type,
        ordinal_position
    FROM system.information_schema.columns
    WHERE CONCAT_WS('.', table_catalog, table_schema, table_name) IN (
        SELECT DISTINCT CONCAT_WS('.', catalog, schema, table_name) 
        FROM dashboard_columns
    )
)
SELECT 
    CONCAT_WS('.', sc.catalog, sc.schema, sc.table_name) as full_table_name,
    sc.column_name,
    sc.data_type,
    sc.ordinal_position,
    CASE 
        WHEN dc.column_name IS NOT NULL THEN 'YES'
        ELSE 'NO'
    END as used_in_dashboard,
    CASE 
        WHEN dc.column_name IS NULL THEN 'UNUSED'
        ELSE 'USED'
    END as status
FROM system_columns sc
LEFT JOIN dashboard_columns dc
    ON sc.catalog = dc.catalog
    AND sc.schema = dc.schema
    AND sc.table_name = dc.table_name
    AND sc.column_name = dc.column_name
ORDER BY 
    full_table_name,
    status DESC,
    ordinal_position;
"""
    return sql


def generate_column_lists_for_sql(results):
    """
    Generate ready-to-copy column lists for SQL SELECT statements.
    Returns formatted strings that can be directly pasted into SQL.
    """
    
    column_lists = {}
    
    for table_name, entries in results['by_table'].items():
        # Get unique columns for this table
        columns = set()
        for entry in entries:
            columns.update(entry['columns_used'])
        
        sorted_columns = sorted(list(columns))
        
        # Generate different formats
        column_lists[table_name] = {
            'comma_separated': ', '.join(sorted_columns),
            'newline_separated': ',\n    '.join(sorted_columns),
            'backtick_comma': ', '.join([f'`{col}`' for col in sorted_columns]),
            'backtick_newline': ',\n    '.join([f'`{col}`' for col in sorted_columns]),
            'select_statement': f"SELECT\n    {',   '.join([f'`{col}`' for col in sorted_columns])}FROM {table_name}",
            'count': len(sorted_columns),
            'columns': sorted_columns
        }
    
    return column_lists


def generate_column_list_file(results, output_path):
    """
    Generate a text file with ready-to-copy column lists for each table.
    """
    
    column_lists = generate_column_lists_for_sql(results)
    
    with open(output_path, 'w') as f:
        f.write("="*80 + "\n")
        f.write("DASHBOARD COLUMNS - READY TO COPY FOR SQL\n")
        f.write("="*80 + "\n\n")
        
        for table_name, formats in sorted(column_lists.items()):
            f.write("\n" + "="*80 + "\n")
            f.write(f"TABLE: {table_name}\n")
            f.write(f"Column count: {formats['count']}\n")
            f.write("="*80 + "\n\n")
            
            # Format 1: Simple comma-separated
            f.write("-- Format 1: Comma-separated (inline)\n")
            f.write("-- Copy and paste after SELECT:\n")
            f.write(formats['comma_separated'] + "\n\n")
            
            # Format 2: Comma-separated with backticks
            f.write("-- Format 2: Comma-separated with backticks (inline)\n")
            f.write("-- Copy and paste after SELECT:\n")
            f.write(formats['backtick_comma'] + "\n\n")
            
            # Format 3: Newline-separated with backticks
            f.write("-- Format 3: One column per line with backticks (formatted)\n")
            f.write("-- Copy and paste after SELECT:\n")
            f.write(formats['backtick_newline'] + "\n\n")
            
            # Format 4: Complete SELECT statement
            f.write("-- Format 4: Complete SELECT statement\n")
            f.write("-- Ready to execute:\n")
            f.write(formats['select_statement'] + ";\n\n")
            
            # Format 5: Array format for Python/Spark
            f.write("-- Format 5: Python/Spark list\n")
            f.write("-- For use in PySpark select():\n")
            columns_str = ', '.join([f'"{col}"' for col in formats['columns']])
            f.write(f"columns = [{columns_str}]\n")
            f.write(f"df.select(columns)\n\n")
            
            f.write("-"*80 + "\n\n")


def generate_system_table_python(results):
    """
    Generate Python/PySpark code to fetch columns from system tables.
    """
    
    tables = list(results['by_table'].keys())
    tables_str = "', '".join(tables)
    
    code = f"""# Fetch all columns from dashboard tables using PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Dashboard tables
dashboard_tables = ['{tables_str}']

# Query system.information_schema.columns
system_columns_df = spark.sql('''
    SELECT 
        CONCAT_WS('.', table_catalog, table_schema, table_name) as full_table_name,
        column_name,
        data_type,
        ordinal_position
    FROM system.information_schema.columns
    WHERE CONCAT_WS('.', table_catalog, table_schema, table_name) IN ({{tables}})
    ORDER BY full_table_name, ordinal_position
'''.format(tables="'" + "', '".join(dashboard_tables) + "'"))

# Display results
display(system_columns_df)

# Get columns by table
columns_by_table = {{}}
for row in system_columns_df.collect():
    table = row.full_table_name
    if table not in columns_by_table:
        columns_by_table[table] = []
    columns_by_table[table].append(row.column_name)

print("Columns found in system tables:")
for table, columns in columns_by_table.items():
    print(f"\\n{{table}}: {{len(columns)}} columns")
    print(f"  {{', '.join(columns)}}")
"""
    return code


def generate_lineage_python(dashboard_tables):
    """
    Generate Python/PySpark code for lineage query.
    """
    
    tables_str = "', '".join(dashboard_tables)
    
    code = f"""# Query upstream table lineage using PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Query table lineage
lineage_df = spark.sql('''
    SELECT DISTINCT
        CONCAT_WS('.', target_table_catalog, target_table_schema, target_table_name) as dashboard_table,
        CONCAT_WS('.', source_table_catalog, source_table_schema, source_table_name) as upstream_table,
        source_type
    FROM system.access.table_lineage
    WHERE CONCAT_WS('.', target_table_catalog, target_table_schema, target_table_name) 
        IN ('{{tables}}')
    ORDER BY dashboard_table, upstream_table
'''.format(tables="', '".join(['{tables_str}'])))

display(lineage_df)
"""
    return code


def generate_unused_columns_python(results):
    """
    Generate Python code to identify unused columns.
    """
    
    # Build column usage mapping
    table_columns = {}
    for table_name, entries in results['by_table'].items():
        columns = set()
        for entry in entries:
            columns.update(entry['columns_used'])
        table_columns[table_name] = list(columns)
    
    code = f"""# Identify unused columns in dashboard tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.getOrCreate()

# Dashboard column usage
dashboard_column_usage = {table_columns}

# Get all columns from system tables
all_columns = []
for table_name, used_columns in dashboard_column_usage.items():
    df = spark.sql(f'''
        SELECT 
            '{{table_name}}' as table_name,
            column_name,
            data_type
        FROM system.information_schema.columns
        WHERE CONCAT_WS('.', table_catalog, table_schema, table_name) = '{{table_name}}'
    ''')
    
    # Mark columns as used or unused
    df = df.withColumn('used_in_dashboard', 
        when(col('column_name').isin(used_columns), 'YES').otherwise('NO'))
    
    all_columns.append(df)

# Union all results
unused_columns_df = all_columns[0]
for df in all_columns[1:]:
    unused_columns_df = unused_columns_df.union(df)

# Filter to show only unused columns
unused_only = unused_columns_df.filter(col('used_in_dashboard') == 'NO')

print("\\nUnused columns by table:")
display(unused_only.orderBy('table_name', 'column_name'))

# Summary statistics
summary = unused_columns_df.groupBy('table_name', 'used_in_dashboard').count()
print("\\nSummary:")
display(summary)
"""
    return code


def save_queries_to_files(queries, output_dir='./output'):
    """
    Save generated queries to separate SQL/Python files.
    """
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save SQL queries
    with open(f'{output_dir}/01_table_columns.sql', 'w') as f:
        f.write(queries['table_columns_query'])
    
    with open(f'{output_dir}/02_lineage.sql', 'w') as f:
        f.write(queries['lineage_query'])
    
    with open(f'{output_dir}/03_unused_columns.sql', 'w') as f:
        f.write(queries['unused_columns_query'])
    
    with open(f'{output_dir}/04_comparison_analysis.sql', 'w') as f:
        f.write(queries['comparison_analysis'])
    
    print(f"Queries saved to {output_dir}/")


# Main execution function
def analyze_dashboard_columns(dashboard_json_path, output_format='sql'):
    """
    Complete workflow: Extract columns from dashboard and generate comparison queries.
    
    Args:
        dashboard_json_path: Path to dashboard export JSON file
        output_format: 'sql' or 'python'
    """
    
    # Import the extraction function (assumes it's available)
    from extract_expressions import extract_expressions
    
    # Load dashboard JSON
    with open(dashboard_json_path, 'r') as f:
        dashboard_data = json.load(f)
    
    # Extract expressions
    print("Extracting columns from dashboard...")
    results = extract_expressions(dashboard_data)
    
    # Generate queries
    print(f"Generating {output_format.upper()} queries...")
    queries = generate_column_comparison_queries(results, output_format)
    
    # Save to files
    save_queries_to_files(queries)
    
    # Print summary
    print("\n" + "="*80)
    print("ANALYSIS SUMMARY")
    print("="*80)
    print(f"Tables analyzed: {len(results['by_table'])}")
    print(f"Total columns used in dashboard: {len(results['all_columns'])}")
    print(f"\nGenerated queries:")
    print("  1. Table columns query - Get all columns from system tables")
    print("  2. Lineage query - Get upstream dependencies")
    print("  3. Unused columns query - Find columns not used in dashboard")
    print("  4. Comparison analysis - Full comparison view")
    
    return results, queries


if __name__ == "__main__":
    import argparse
    import sys
    
    # Set up command line argument parser
    parser = argparse.ArgumentParser(
        description='Extract only the utilised columns from Databricks dashboard to optimise your SQL queries and ETL pipelines.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic usage with SQL output
  python column_lens.py dashboard_export.json
  
  # Generate Python/PySpark code instead
  python column_lens.py dashboard_export.json --format python
  
  # Specify custom output directory
  python column_lens.py dashboard_export.json --output ./my_queries
  
  # Generate both SQL and Python versions
  python column_lens.py dashboard_export.json --format both
        '''
    )
    
    parser.add_argument(
        'dashboard_json',
        help='Path to the dashboard export JSON file'
    )
    
    parser.add_argument(
        '-f', '--format',
        choices=['sql', 'python', 'both'],
        default='sql',
        help='Output format: sql, python, or both (default: sql)'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='./output',
        help='Output directory for generated queries (default: ./output)'
    )
    
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Print queries to console only, do not save to files'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate input file exists
    import os
    if not os.path.exists(args.dashboard_json):
        print(f"Error: File not found: {args.dashboard_json}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Import the extraction function
        from extract_expressions import extract_expressions
        
        # Load dashboard JSON
        print(f"Loading dashboard from: {args.dashboard_json}")
        with open(args.dashboard_json, 'r') as f:
            dashboard_data = json.load(f)
        
        # Extract expressions
        print("Extracting columns from dashboard...")
        results = extract_expressions(dashboard_data)
        
        # Generate queries based on format
        formats_to_generate = ['sql', 'python'] if args.format == 'both' else [args.format]
        
        for fmt in formats_to_generate:
            print(f"\nGenerating {fmt.upper()} queries...")
            queries = generate_column_comparison_queries(results, fmt)
            
            if not args.no_save:
                # Create format-specific subdirectory
                output_dir = os.path.join(args.output, fmt) if args.format == 'both' else args.output
                
                # Save queries
                os.makedirs(output_dir, exist_ok=True)
                
                if fmt == 'sql':
                    with open(f'{output_dir}/01_table_columns.sql', 'w') as f:
                        f.write(queries['table_columns_query'])
                    
                    with open(f'{output_dir}/02_lineage.sql', 'w') as f:
                        f.write(queries['lineage_query'])
                    
                    with open(f'{output_dir}/03_unused_columns.sql', 'w') as f:
                        f.write(queries['unused_columns_query'])
                    
                    with open(f'{output_dir}/04_comparison_analysis.sql', 'w') as f:
                        f.write(queries['comparison_analysis'])
                    
                    # Generate the column lists file
                    column_lists_path = f'{output_dir}/00_COLUMNS_TO_COPY.txt'
                    generate_column_list_file(results, column_lists_path)
                    
                    print(f"✓ SQL queries saved to {output_dir}/")
                    print(f"✓ Ready-to-copy column lists saved to {column_lists_path}")
                
                else:  # python
                    with open(f'{output_dir}/01_table_columns.py', 'w') as f:
                        f.write(queries['table_columns_query'])
                    
                    with open(f'{output_dir}/02_lineage.py', 'w') as f:
                        f.write(queries['lineage_query'])
                    
                    with open(f'{output_dir}/03_unused_columns.py', 'w') as f:
                        f.write(queries['unused_columns_query'])
                    
                    # Also generate column lists for Python
                    column_lists_path = f'{output_dir}/00_COLUMNS_TO_COPY.txt'
                    generate_column_list_file(results, column_lists_path)
                    
                    print(f"✓ Python scripts saved to {output_dir}/")
                    print(f"✓ Ready-to-copy column lists saved to {column_lists_path}")
            
            else:
                # Print to console
                print("\n" + "="*80)
                print(f"QUERY 1: TABLE COLUMNS ({fmt.upper()})")
                print("="*80)
                print(queries['table_columns_query'])
                
                print("\n" + "="*80)
                print(f"QUERY 2: LINEAGE ({fmt.upper()})")
                print("="*80)
                print(queries['lineage_query'])
        
        # Print summary
        print("\n" + "="*80)
        print("ANALYSIS SUMMARY")
        print("="*80)
        print(f"Dashboard file: {args.dashboard_json}")
        print(f"Tables analyzed: {len(results['by_table'])}")
        print(f"Unique columns used: {len(results['all_columns'])}")
        print(f"Total expressions: {len(results['all_expressions'])}")
        print(f"Total filters: {len(results['all_filters'])}")
        
        print("\nTables in dashboard:")
        for table in sorted(results['by_table'].keys()):
            column_count = len(set(col for entry in results['by_table'][table] for col in entry['columns_used']))
            print(f"  - {table} ({column_count} columns used)")
        
        if not args.no_save:
            print(f"\n✓ All queries saved to: {args.output}")
            print("\nNext steps:")
            print("  1. Run the generated SQL queries in Databricks SQL")
            print("  2. Review unused columns in query 03_unused_columns.sql")
            print("  3. Check upstream lineage in query 02_lineage.sql")
            print("  4. Optimize ETL pipelines by removing unused columns")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find file: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in dashboard file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)