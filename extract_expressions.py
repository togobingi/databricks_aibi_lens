import json
import re
from collections import defaultdict

def extract_expressions(dashboard_json):
    """
    Extract all expressions from a Databricks dashboard JSON export.
    Returns a dictionary with widget names and their expressions.
    """
    
    # Parse JSON if string, otherwise use as-is
    if isinstance(dashboard_json, str):
        data = json.loads(dashboard_json)
    else:
        data = dashboard_json
    
    # First, build a mapping of dataset IDs to table names
    dataset_mapping = build_dataset_mapping(data)
    
    results = {
        'by_widget': [],
        'by_table': defaultdict(list),
        'all_columns': set(),
        'all_expressions': [],
        'all_filters': [],
        'dataset_mapping': dataset_mapping
    }
    
    # Iterate through pages
    for page in data.get('pages', []):
        page_name = page.get('displayName', 'Unknown Page')
        
        # Iterate through widgets in layout
        for layout_item in page.get('layout', []):
            widget = layout_item.get('widget', {})
            widget_name = widget.get('name', 'Unknown Widget')
            widget_title = None
            
            # Get widget title from spec if available
            if 'spec' in widget and 'frame' in widget.get('spec', {}):
                widget_title = widget['spec']['frame'].get('title', widget_name)
            
            # Extract queries
            for query_item in widget.get('queries', []):
                query = query_item.get('query', {})
                dataset_id = query.get('datasetName', 'Unknown Dataset')
                table_name = dataset_mapping.get(dataset_id, dataset_id)
                
                # Extract fields with expressions
                for field in query.get('fields', []):
                    field_name = field.get('name', 'Unknown Field')
                    expression = field.get('expression')
                    
                    if expression:
                        # Parse column names from expression
                        columns = extract_columns_from_expression(expression)
                        
                        entry = {
                            'page': page_name,
                            'widget': widget_title or widget_name,
                            'dataset_id': dataset_id,
                            'table_name': table_name,
                            'field_name': field_name,
                            'expression': expression,
                            'columns_used': list(columns),
                            'type': 'field'
                        }
                        
                        results['by_widget'].append(entry)
                        results['by_table'][table_name].append(entry)
                        results['all_columns'].update(columns)
                        results['all_expressions'].append(expression)
                
                # Extract filters
                for filter_item in query.get('filters', []):
                    filter_expression = filter_item.get('expression')
                    
                    if filter_expression:
                        # Parse column names from filter expression
                        columns = extract_columns_from_expression(filter_expression)
                        
                        entry = {
                            'page': page_name,
                            'widget': widget_title or widget_name,
                            'dataset_id': dataset_id,
                            'table_name': table_name,
                            'filter_expression': filter_expression,
                            'columns_used': list(columns),
                            'type': 'filter'
                        }
                        
                        results['by_widget'].append(entry)
                        results['by_table'][table_name].append(entry)
                        results['all_columns'].update(columns)
                        results['all_filters'].append(filter_expression)
    
    # Convert set to sorted list for easier reading
    results['all_columns'] = sorted(list(results['all_columns']))
    results['by_table'] = dict(results['by_table'])
    
    return results


def build_dataset_mapping(data):
    """
    Build a mapping from dataset ID to actual table name from queryLines.
    """
    mapping = {}
    
    for dataset in data.get('datasets', []):
        dataset_id = dataset.get('name')
        query_lines = dataset.get('queryLines', [])
        
        if dataset_id and query_lines:
            # Join all query lines
            full_query = ' '.join(query_lines)
            
            # Extract table name from SELECT ... FROM table_name pattern
            table_name = extract_table_name(full_query)
            
            if table_name:
                mapping[dataset_id] = table_name
            else:
                # Fallback to displayName if table name not found
                mapping[dataset_id] = dataset.get('displayName', dataset_id)
    
    return mapping


def extract_table_name(query):
    """
    Extract table name from SQL query.
    Handles patterns like:
    - SELECT * FROM table_name
    - select * from schema.table
    - SELECT ... FROM catalog.schema.table
    """
    # Pattern to match FROM clause with optional catalog/schema
    pattern = r'FROM\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*)'
    
    match = re.search(pattern, query, re.IGNORECASE)
    
    if match:
        return match.group(1)
    
    return None


def extract_columns_from_expression(expression):
    """
    Extract column names from SQL-like expressions.
    Looks for patterns like `column_name`.
    """
    import re
    
    # Pattern to match backtick-enclosed column names
    pattern = r'`([^`]+)`'
    matches = re.findall(pattern, expression)
    
    return set(matches)


def print_summary(results):
    """
    Print a formatted summary of the extraction results.
    """
    print("=" * 80)
    print("DASHBOARD EXPRESSION EXTRACTION SUMMARY")
    print("=" * 80)
    
    # Count filters vs fields
    filters_count = sum(1 for entry in results['by_widget'] if entry['type'] == 'filter')
    fields_count = sum(1 for entry in results['by_widget'] if entry['type'] == 'field')
    
    print(f"\nTotal unique columns found: {len(results['all_columns'])}")
    print(f"Total expressions found: {len(results['all_expressions'])}")
    print(f"Total filters found: {len(results['all_filters'])}")
    print(f"Total tables: {len(results['by_table'])}")
    print(f"Total widget items: {len(results['by_widget'])} ({fields_count} fields, {filters_count} filters)")
    
    print("\n" + "=" * 80)
    print("DATASET ID TO TABLE NAME MAPPING")
    print("=" * 80)
    for dataset_id, table_name in sorted(results['dataset_mapping'].items()):
        print(f"  {dataset_id} -> {table_name}")
    
    print("\n" + "=" * 80)
    print("ALL COLUMNS USED")
    print("=" * 80)
    for col in results['all_columns']:
        print(f"  - {col}")
    
    print("\n" + "=" * 80)
    print("COLUMNS BY TABLE")
    print("=" * 80)
    for table, entries in sorted(results['by_table'].items()):
        columns = set()
        for entry in entries:
            columns.update(entry['columns_used'])
        print(f"\n{table}:")
        for col in sorted(columns):
            print(f"  - {col}")
    
    print("\n" + "=" * 80)
    print("SAMPLE EXPRESSIONS BY WIDGET (First 10)")
    print("=" * 80)
    for i, entry in enumerate(results['by_widget'][:10]):
        print(f"\n{i+1}. Widget: {entry['widget']}")
        print(f"   Page: {entry['page']}")
        print(f"   Table: {entry['table_name']}")
        print(f"   Type: {entry['type'].upper()}")
        
        if entry['type'] == 'field':
            print(f"   Field: {entry['field_name']}")
            print(f"   Expression: {entry['expression']}")
        else:  # filter
            print(f"   Filter: {entry['filter_expression']}")
        
        print(f"   Columns: {', '.join(entry['columns_used'])}")
    
    print("\n" + "=" * 80)
    print("FILTER EXPRESSIONS")
    print("=" * 80)
    filter_entries = [e for e in results['by_widget'] if e['type'] == 'filter']
    for i, entry in enumerate(filter_entries[:10]):
        print(f"\n{i+1}. Widget: {entry['widget']}")
        print(f"   Table: {entry['table_name']}")
        print(f"   Filter: {entry['filter_expression']}")
        print(f"   Columns: {', '.join(entry['columns_used'])}")


# Example usage:
if __name__ == "__main__":
    # Load your JSON file
    with open('aibi_dashboard.json', 'r') as f:
        dashboard_data = json.load(f)
    
    # Extract expressions
    results = extract_expressions(dashboard_data)
    
    # Print summary
    print_summary(results)
    
    # Optionally save to JSON file
    with open('extracted_expressions.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n\nFull results saved to 'extracted_expressions.json'")