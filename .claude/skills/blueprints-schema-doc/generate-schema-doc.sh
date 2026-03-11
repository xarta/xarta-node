#!/bin/bash

# blueprints-schema-doc/generate-schema-doc.sh
# Generate Blueprints database schema documentation in markdown

set -e

DB_PATH="/opt/blueprints/data/db/blueprints.db"

if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database not found at $DB_PATH"
    exit 1
fi

OUTPUT_SCHEMA="blueprints-schema.md"
OUTPUT_DIAGRAM="blueprints-schema-diagram.md"

python3 << 'PYTHON_EOF'
import sqlite3
import json
import sys
from datetime import datetime

DB_PATH = "/opt/blueprints/data/db/blueprints.db"

def get_schema():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [t[0] for t in cursor.fetchall()]
    
    schema = {}
    for table_name in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        table_info = {
            'columns': [],
            'primary_key': None,
        }
        
        for col_id, col_name, col_type, not_null, default_val, pk in columns:
            col_info = {
                'name': col_name,
                'type': col_type,
                'not_null': not_null,
                'default': default_val,
                'pk': pk,
            }
            table_info['columns'].append(col_info)
            if pk:
                table_info['primary_key'] = col_name
        
        schema[table_name] = table_info
    
    conn.close()
    return schema, tables

def infer_relationships(schema, tables):
    """Infer foreign key relationships based on column naming patterns"""
    relationships = []
    seen = set()
    
    for source_table, source_info in schema.items():
        for col in source_info['columns']:
            col_name = col['name']
            
            # Skip primary keys themselves
            if col['pk']:
                continue
            
            # Look for patterns like "target_table_id" or "table_id"
            for target_table in tables:
                target_pk = schema[target_table]['primary_key']
                if not target_pk:
                    continue
                
                matched = False
                
                # Pattern 1: "table_id" (e.g., "node_id" matches "nodes")
                if col_name == f"{target_table[:-1]}_id" and target_table.endswith('s'):
                    matched = True
                
                # Pattern 2: "table_id" for irregular plurals (e.g., "sync_queue.target_node_id" -> "nodes")
                elif col_name.endswith(f"_{target_table[:-1]}_id") and target_table.endswith('s'):
                    matched = True
                
                # Pattern 3: Exact match (e.g., "service_id" in services table itself -> self-ref, skip)
                elif col_name == f"{source_table[:-1]}_id" and source_table == target_table:
                    # Skip self-references to primary key
                    if col_name != f"{target_table[:-1]}_id":
                        matched = False
                
                # Pattern 4: Parent references (e.g., "parent_machine_id" in "machines")
                elif col_name.startswith("parent_") and col_name == f"parent_{target_table[:-1]}_id" and target_table.endswith('s'):
                    matched = True
                
                if matched:
                    rel_key = (source_table, col_name, target_table, target_pk)
                    if rel_key not in seen:
                        seen.add(rel_key)
                        relationships.append({
                            'source': source_table,
                            'source_col': col_name,
                            'target': target_table,
                            'target_col': target_pk,
                        })
    
    return relationships

def generate_markdown_tables(schema, tables, relationships):
    """Generate markdown tables for each table"""
    md = []
    md.append("# Blueprints Database Schema\n\n")
    md.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
    md.append("---\n\n")
    
    # Build relationship summary
    if relationships:
        md.append("## Relationships Summary\n\n")
        for rel in relationships:
            md.append(f"- `{rel['source']}.{rel['source_col']}` → `{rel['target']}.{rel['target_col']}`\n")
        md.append("\n---\n\n")
    
    for table_name in tables:
        if table_name == 'sqlite_sequence':
            continue
        
        info = schema[table_name]
        md.append(f"## `{table_name}`\n\n")
        
        # Build table markdown
        md.append("| Column | Type | Null? | Default | Notes |\n")
        md.append("|--------|------|:-----:|---------|-------|\n")
        
        for col in info['columns']:
            col_name = col['name']
            col_type = col['type']
            nullable = "✓" if col['not_null'] == 0 else "✗"
            default = f"`{col['default']}`" if col['default'] else "—"
            
            notes = []
            if col['pk']:
                notes.append("**PK**")
            
            # Check for inferred foreign keys
            for rel in relationships:
                if rel['source'] == table_name and rel['source_col'] == col_name:
                    notes.append(f"→ `{rel['target']}({rel['target_col']})`")
            
            notes_str = " | ".join(notes) if notes else "—"
            md.append(f"| `{col_name}` | `{col_type}` | {nullable} | {default} | {notes_str} |\n")
        
        md.append("\n")
    
    return "".join(md)

def generate_mermaid_diagram(schema, tables, relationships):
    """Generate Mermaid ER diagram"""
    mermaid = []
    mermaid.append("# Blueprints Database Relationships\n")
    mermaid.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
    mermaid.append("```mermaid\nerDiagram\n")
    
    # Add all tables and their columns
    for table_name in tables:
        if table_name == 'sqlite_sequence':
            continue
        
        info = schema[table_name]
        mermaid.append(f"    {table_name} {{\n")
        
        for col in info['columns']:
            col_name = col['name']
            col_type = col['type']
            
            # Add type hint
            type_hint = ""
            if col['pk']:
                type_hint = " PK"
            
            # Check if it's a foreign key
            is_fk = False
            for rel in relationships:
                if rel['source'] == table_name and rel['source_col'] == col_name:
                    is_fk = True
                    break
            
            if is_fk:
                type_hint += " FK"
            
            mermaid.append(f"        {col_type} {col_name}{type_hint}\n")
        
        mermaid.append("    }\n")
    
    # Add relationships with proper Mermaid syntax
    for rel in relationships:
        source = rel['source']
        target = rel['target']
        source_col = rel['source_col']
        
        # Use proper ER diagram syntax: source ||--o{ target
        # Many source records reference one target record
        mermaid.append(f"    {source} ||--o{{ {target} : uses\n")
    
    mermaid.append("```\n")
    
    return "".join(mermaid)

# Main execution
try:
    schema, tables = get_schema()
    relationships = infer_relationships(schema, tables)
    
    # Generate markdown tables
    markdown_content = generate_markdown_tables(schema, tables, relationships)
    with open("blueprints-schema.md", "w") as f:
        f.write(markdown_content)
    print("✓ Generated: blueprints-schema.md")
    
    # Generate Mermaid diagram
    mermaid_content = generate_mermaid_diagram(schema, tables, relationships)
    with open("blueprints-schema-diagram.md", "w") as f:
        f.write(mermaid_content)
    print("✓ Generated: blueprints-schema-diagram.md")
    
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

PYTHON_EOF
