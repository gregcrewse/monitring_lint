#!/usr/bin/env python3
import argparse
import os
import subprocess
import tempfile

def find_sql_file(filename: str) -> str:
    """Find SQL file in current directory and subdirectories."""
    # Remove .sql extension if provided
    base_name = filename.replace('.sql', '')
    
    # Search patterns to try
    patterns = [f"{base_name}.sql", base_name]
    
    # Walk through directories
    for root, _, files in os.walk('.'):
        for pattern in patterns:
            for file in files:
                if pattern == file:
                    return os.path.abspath(os.path.join(root, file))
    
    return None

def create_config_file() -> str:
    """Create a temporary config file with the specified SQLFluff rules."""
    config_content = """[sqlfluff]
dialect = redshift 
templater = dbt
max_line_length = 120
exclude_rules = L029,L031,L034
sql_file_exts = .sql,.sql.jinja2,.dml,.ddl

[sqlfluff:indentation]
indented_joins = false
indented_using_on = true
template_blocks_indent = false
indent_unit = tab

[sqlfluff:templater:dbt]
project_dir = .
profile = ''

[sqlfluff:templater:jinja]
apply_dbt_builtins = true

[sqlfluff:layout:type:comma]
line_position = leading

[sqlfluff:layout:spacing]
spacing_before_parenthesis = 0
spacing_after_parenthesis = 0
spacing_within_parenthesis = 0
tab_space_size = 4
line_spacing = 0
spacing_before_comma = 1
spacing_after_comma = 0

[sqlfluff:rules:convention.comparison_operator]
preferred_comparison_operator = !=

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = lower

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower
ignore_words = []

[sqlfluff:rules:capitalisation.functions]
extended_capitalisation_policy = lower

[sqlfluff:rules:aliasing.table]
force_explicit = True

[sqlfluff:rules:aliasing.column]
aliasing = explicit

[sqlfluff:rules:layout.select_targets]
line_position = single
spacing_after = 0

[sqlfluff:rules:layout.spacing]
spacing_before_parenthesis = 0
spacing_after_parenthesis = 0
spacing_within_parenthesis = 0
line_spacing = 0
treat_multiple_whitespace = True
allow_mixed_indentation = False

[sqlfluff:rules:references.from]
force_from = True

[sqlfluff:rules:structure.subquery]
forbid_subquery_in = both

[sqlfluff:rules:ambiguous.column_references]
group_by_policy = consistent

[sqlfluff:rules:convention.quoted_literals]
preferred_quoted_literal_style = consistent

[sqlfluff:rules:layout.long_lines]
ignore_comment_lines = True
ignore_comment_clauses = True

[sqlfluff:rules:layout.align_expressions]
align_to = left

[sqlfluff:rules:layout.operators]
operator_new_lines = before

[sqlfluff:rules:convention.select_trailing_comma]
select_clause_trailing_comma = forbid
"""
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix='.sqlfluff')
    with os.fdopen(fd, 'w') as f:
        f.write(config_content)
    
    return path

def run_sqlfluff(filename: str, config_path: str, project_dir: str) -> bool:
    """Run SQLFluff fix on the specified file."""
    try:
        print(f"Running SQLFluff on {filename}")
        print(f"Using config: {config_path}")
        print(f"Project directory: {project_dir}")
        
        result = subprocess.run(
            [
                "sqlfluff", 
                "fix", 
                filename,
                "--config", 
                config_path,
                "--verbose"
            ],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        print("SQLFluff output:")
        print(result.stdout)
        
        if result.stderr:
            print("Errors:")
            print(result.stderr)
            return False
        
        return True
    except Exception as e:
        print(f"❌ Error running SQLFluff: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="SQLFluff Linter for dbt models")
    parser.add_argument("filename", help="SQL file name (with or without path)")
    parser.add_argument("--project-dir", help="dbt project directory (default: current directory)", default=os.getcwd())
    args = parser.parse_args()

    # Find the SQL file
    file_path = find_sql_file(args.filename)
    if not file_path:
        print(f"❌ Error: Could not find SQL file '{args.filename}'")
        return

    print(f"Found SQL file: {file_path}")
    
    # Create temporary config file
    config_path = create_config_file()
    try:
        # Run SQLFluff
        if run_sqlfluff(file_path, config_path, args.project_dir):
            print(f"✅ Successfully linted: {file_path}")
        else:
            print(f"❌ SQLFluff encountered issues with: {file_path}")
    finally:
        # Clean up temporary config file
        os.remove(config_path)

if __name__ == "__main__":
    main()
