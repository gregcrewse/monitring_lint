import json
import csv
import os
import re
from pathlib import Path

def extract_column_lineage(manifest_path, catalog_path, output_dir="lineage_output"):
    """
    Extract column lineage information from dbt manifest and catalog files.
    
    Args:
        manifest_path: Path to dbt manifest.json
        catalog_path: Path to dbt catalog.json
        output_dir: Directory to save output files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load manifest and catalog files
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    with open(catalog_path, 'r') as f:
        catalog = json.load(f)
    
    # Extract nodes (models) from manifest
    nodes = manifest.get('nodes', {})
    
    # Get column information from catalog
    model_columns = {}
    for node_name, node in catalog.get('nodes', {}).items():
        if node.get('metadata', {}).get('type') in ('table', 'view'):
            columns = node.get('columns', {})
            model_columns[node_name] = list(columns.keys())
    
    # Initialize lineage dictionary
    column_lineage = {}
    
    # Process each model
    for node_name, node in nodes.items():
        if node.get('resource_type') == 'model':
            # Get compiled SQL
            compiled_sql = node.get('compiled_sql', '')
            if not compiled_sql:
                continue
            
            # Get model columns
            if node_name not in model_columns:
                continue
            
            model_cols = model_columns[node_name]
            
            # Get dependencies
            depends_on = node.get('depends_on', {}).get('nodes', [])
            
            # For each dependency, check which columns are referenced
            for dep_node in depends_on:
                if dep_node not in model_columns:
                    continue
                
                dep_cols = model_columns[dep_node]
                
                # Simple heuristic: check if a column name from dependency appears in SQL
                for col in dep_cols:
                    # Add basic column name pattern matching
                    # This is simplified; a proper SQL parser would be better
                    col_pattern = r'\b' + re.escape(col) + r'\b'
                    if re.search(col_pattern, compiled_sql):
                        # Record lineage
                        if node_name not in column_lineage:
                            column_lineage[node_name] = {}
                        
                        # Find which columns in this model likely use the dependent column
                        for model_col in model_cols:
                            # Another simple heuristic
                            col_usage_pattern = r'\b' + re.escape(model_col) + r'\s*=.*\b' + re.escape(col) + r'\b'
                            col_in_select = r'(?:select|,)\s*.*\b' + re.escape(col) + r'\b.*\s+as\s+\b' + re.escape(model_col) + r'\b'
                            
                            if re.search(col_usage_pattern, compiled_sql, re.IGNORECASE) or \
                               re.search(col_in_select, compiled_sql, re.IGNORECASE):
                                if model_col not in column_lineage[node_name]:
                                    column_lineage[node_name][model_col] = {}
                                
                                if dep_node not in column_lineage[node_name][model_col]:
                                    column_lineage[node_name][model_col][dep_node] = []
                                
                                column_lineage[node_name][model_col][dep_node].append(col)
    
    # Export lineage to JSON
    json_output_path = os.path.join(output_dir, "column_lineage.json")
    with open(json_output_path, 'w') as f:
        json.dump(column_lineage, f, indent=2)
    
    # Export to CSV for easier analysis
    csv_output_path = os.path.join(output_dir, "column_lineage.csv")
    with open(csv_output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Model", "Column", "Upstream Model", "Upstream Column"])
        
        for model, columns in column_lineage.items():
            for column, upstreams in columns.items():
                for upstream_model, upstream_columns in upstreams.items():
                    for upstream_column in upstream_columns:
                        writer.writerow([
                            model.split('.')[-1],  # Extract model name
                            column,
                            upstream_model.split('.')[-1],  # Extract upstream model name
                            upstream_column
                        ])
    
    # Generate downstream lineage CSV
    downstream_csv_path = os.path.join(output_dir, "downstream_lineage.csv")
    with open(downstream_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Model", "Column", "Downstream Model", "Downstream Column"])
        
        # Invert the lineage to show downstream relationships
        downstream_lineage = {}
        for model, columns in column_lineage.items():
            for column, upstreams in columns.items():
                for upstream_model, upstream_columns in upstreams.items():
                    for upstream_column in upstream_columns:
                        if upstream_model not in downstream_lineage:
                            downstream_lineage[upstream_model] = {}
                        
                        if upstream_column not in downstream_lineage[upstream_model]:
                            downstream_lineage[upstream_model][upstream_column] = []
                        
                        downstream_lineage[upstream_model][upstream_column].append((model, column))
        
        # Write to CSV
        for model, columns in downstream_lineage.items():
            for column, downstreams in columns.items():
                for downstream in downstreams:
                    downstream_model, downstream_column = downstream
                    writer.writerow([
                        model.split('.')[-1],  # Extract model name
                        column,
                        downstream_model.split('.')[-1],  # Extract downstream model name
                        downstream_column
                    ])
    
    print(f"Column lineage exported to {json_output_path}")
    print(f"CSV lineage exported to {csv_output_path}")
    print(f"Downstream lineage exported to {downstream_csv_path}")
    
    return column_lineage

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract column lineage from dbt")
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--catalog", required=True, help="Path to dbt catalog.json")
    parser.add_argument("--output", default="lineage_output", help="Output directory")
    
    args = parser.parse_args()
    
    extract_column_lineage(args.manifest, args.catalog, args.output)
