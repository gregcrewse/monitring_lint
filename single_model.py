import json
import csv
import os
import re
from pathlib import Path

def extract_model_column_lineage(manifest_path, catalog_path, target_model=None, output_dir="lineage_output"):
    """
    Extract column lineage information for a specific model from dbt manifest and catalog files.
    
    Args:
        manifest_path: Path to dbt manifest.json
        catalog_path: Path to dbt catalog.json
        target_model: Target model name (without project/schema prefixes)
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
    
    # Find the full node name for the target model
    target_node_name = None
    if target_model:
        for node_name in nodes:
            # Check if the node name ends with the target model name
            if node_name.split('.')[-1] == target_model:
                target_node_name = node_name
                break
        
        if not target_node_name:
            print(f"Model '{target_model}' not found in the manifest.")
            return {}
    
    # Initialize lineage dictionary
    upstream_lineage = {}
    downstream_lineage = {}
    
    # First pass: build the complete column-to-column lineage
    all_column_lineage = {}
    
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
                    col_pattern = r'\b' + re.escape(col) + r'\b'
                    if re.search(col_pattern, compiled_sql):
                        # Find which columns in this model likely use the dependent column
                        for model_col in model_cols:
                            # Another simple heuristic
                            col_usage_pattern = r'\b' + re.escape(model_col) + r'\s*=.*\b' + re.escape(col) + r'\b'
                            col_in_select = r'(?:select|,)\s*.*\b' + re.escape(col) + r'\b.*\s+as\s+\b' + re.escape(model_col) + r'\b'
                            
                            if re.search(col_usage_pattern, compiled_sql, re.IGNORECASE) or \
                               re.search(col_in_select, compiled_sql, re.IGNORECASE):
                                # Record upstream lineage
                                if node_name not in all_column_lineage:
                                    all_column_lineage[node_name] = {}
                                
                                if model_col not in all_column_lineage[node_name]:
                                    all_column_lineage[node_name][model_col] = {}
                                
                                if dep_node not in all_column_lineage[node_name][model_col]:
                                    all_column_lineage[node_name][model_col][dep_node] = []
                                
                                if col not in all_column_lineage[node_name][model_col][dep_node]:
                                    all_column_lineage[node_name][model_col][dep_node].append(col)
    
    # If we have a target model, extract its lineage
    if target_node_name:
        # Get upstream lineage (recursive)
        def get_upstream_lineage(node_name, column_name, visited=None):
            if visited is None:
                visited = set()
            
            # Avoid circular references
            key = (node_name, column_name)
            if key in visited:
                return {}
            
            visited.add(key)
            
            result = {}
            
            # Check if this node and column have upstream dependencies
            if node_name in all_column_lineage and column_name in all_column_lineage[node_name]:
                for upstream_node, upstream_columns in all_column_lineage[node_name][column_name].items():
                    if upstream_node not in result:
                        result[upstream_node] = {}
                    
                    for upstream_col in upstream_columns:
                        result[upstream_node][upstream_col] = get_upstream_lineage(upstream_node, upstream_col, visited)
            
            return result
        
        # Build upstream lineage for each column in the target model
        if target_node_name in model_columns:
            upstream_lineage[target_node_name] = {}
            
            for column in model_columns[target_node_name]:
                upstream_lineage[target_node_name][column] = get_upstream_lineage(target_node_name, column)
        
        # Build downstream lineage
        def get_downstream_lineage(node_name, column_name, visited=None):
            if visited is None:
                visited = set()
            
            # Avoid circular references
            key = (node_name, column_name)
            if key in visited:
                return {}
            
            visited.add(key)
            
            result = {}
            
            # Look for models that depend on this node and column
            for downstream_node, columns in all_column_lineage.items():
                for downstream_col, upstreams in columns.items():
                    for upstream_node, upstream_cols in upstreams.items():
                        if upstream_node == node_name and column_name in upstream_cols:
                            if downstream_node not in result:
                                result[downstream_node] = {}
                            
                            if downstream_col not in result[downstream_node]:
                                result[downstream_node][downstream_col] = get_downstream_lineage(downstream_node, downstream_col, visited)
            
            return result
        
        # Build downstream lineage for each column in the target model
        if target_node_name in model_columns:
            downstream_lineage[target_node_name] = {}
            
            for column in model_columns[target_node_name]:
                downstream_lineage[target_node_name][column] = get_downstream_lineage(target_node_name, column)
    
    # Export lineage to JSON
    model_name = target_model if target_model else "all_models"
    
    upstream_json_path = os.path.join(output_dir, f"{model_name}_upstream_lineage.json")
    with open(upstream_json_path, 'w') as f:
        json.dump(upstream_lineage, f, indent=2)
    
    downstream_json_path = os.path.join(output_dir, f"{model_name}_downstream_lineage.json")
    with open(downstream_json_path, 'w') as f:
        json.dump(downstream_lineage, f, indent=2)
    
    # Export to CSV for easier analysis
    # For upstream lineage
    def write_upstream_csv(lineage, csv_path):
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Model", "Column", "Upstream Model", "Upstream Column", "Depth"])
            
            def process_upstream(model, column, upstream_model, upstream_column, depth=1):
                writer.writerow([
                    model.split('.')[-1],
                    column,
                    upstream_model.split('.')[-1],
                    upstream_column,
                    depth
                ])
                
                next_upstreams = lineage[model][column][upstream_model][upstream_column]
                for next_up_model, next_up_cols in next_upstreams.items():
                    for next_up_col in next_up_cols:
                        process_upstream(upstream_model, upstream_column, next_up_model, next_up_col, depth+1)
            
            for model, columns in lineage.items():
                for column, upstreams in columns.items():
                    for upstream_model, upstream_cols in upstreams.items():
                        for upstream_column in upstream_cols:
                            process_upstream(model, column, upstream_model, upstream_column)
    
    # For downstream lineage
    def write_downstream_csv(lineage, csv_path):
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Model", "Column", "Downstream Model", "Downstream Column", "Depth"])
            
            def process_downstream(model, column, downstream_model, downstream_column, depth=1):
                writer.writerow([
                    model.split('.')[-1],
                    column,
                    downstream_model.split('.')[-1],
                    downstream_column,
                    depth
                ])
                
                next_downstreams = lineage[model][column][downstream_model][downstream_column]
                for next_down_model, next_down_cols in next_downstreams.items():
                    for next_down_col in next_down_cols:
                        process_downstream(downstream_model, downstream_column, next_down_model, next_down_col, depth+1)
            
            for model, columns in lineage.items():
                for column, downstreams in columns.items():
                    for downstream_model, downstream_cols in downstreams.items():
                        for downstream_column in downstream_cols:
                            process_downstream(model, column, downstream_model, downstream_column)
    
    # Create flattened CSV files for easier analysis
    upstream_csv_path = os.path.join(output_dir, f"{model_name}_upstream_lineage.csv")
    with open(upstream_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Model", "Column", "Upstream Model", "Upstream Column"])
        
        # Flatten the nested structure
        def flatten_upstream(model, column, upstreams, writer):
            for upstream_model, upstream_cols in upstreams.items():
                for upstream_col in upstream_cols:
                    writer.writerow([
                        model.split('.')[-1],
                        column,
                        upstream_model.split('.')[-1],
                        upstream_col
                    ])
                    
                    # Process next level of upstream
                    next_upstreams = upstream_lineage[model][column][upstream_model][upstream_col]
                    if next_upstreams:
                        flatten_upstream(upstream_model, upstream_col, next_upstreams, writer)
        
        for model, columns in upstream_lineage.items():
            for column, upstreams in columns.items():
                flatten_upstream(model, column, upstreams, writer)
    
    downstream_csv_path = os.path.join(output_dir, f"{model_name}_downstream_lineage.csv")
    with open(downstream_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Model", "Column", "Downstream Model", "Downstream Column"])
        
        # Flatten the nested structure
        def flatten_downstream(model, column, downstreams, writer):
            for downstream_model, downstream_cols in downstreams.items():
                for downstream_col in downstream_cols:
                    writer.writerow([
                        model.split('.')[-1],
                        column,
                        downstream_model.split('.')[-1],
                        downstream_col
                    ])
                    
                    # Process next level of downstream
                    next_downstreams = downstream_lineage[model][column][downstream_model][downstream_col]
                    if next_downstreams:
                        flatten_downstream(downstream_model, downstream_col, next_downstreams, writer)
        
        for model, columns in downstream_lineage.items():
            for column, downstreams in columns.items():
                flatten_downstream(model, column, downstreams, writer)
    
    print(f"Upstream lineage exported to {upstream_json_path} and {upstream_csv_path}")
    print(f"Downstream lineage exported to {downstream_json_path} and {downstream_csv_path}")
    
    return {
        "upstream": upstream_lineage,
        "downstream": downstream_lineage
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract column lineage from dbt")
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--catalog", required=True, help="Path to dbt catalog.json")
    parser.add_argument("--model", help="Target model name (without project/schema prefixes)")
    parser.add_argument("--output", default="lineage_output", help="Output directory")
    
    args = parser.parse_args()
    
    extract_model_column_lineage(args.manifest, args.catalog, args.model, args.output)
