#!/usr/bin/env python3
"""
MongoDB Schema Visualizer

Generates visual representations of MongoDB schema recommendations:
- Mermaid ER diagrams
- Interactive HTML visualization
- Console-based tree view

Usage:
    python visualize_schema.py --input analysis.json --output schema.html
    python visualize_schema.py --input analysis.json --format mermaid
"""

import argparse
import json
from pathlib import Path
from typing import Any


def generate_mermaid_diagram(schema_data: dict) -> str:
    """Generate a Mermaid ER diagram from schema data."""
    lines = ["erDiagram"]
    
    collections = schema_data.get("target_schema", {}).get("collections", [])
    recommendations = schema_data.get("recommendations", [])
    
    # Build collection definitions
    for collection in collections:
        name = collection.get("name", "unknown")
        lines.append(f"    {name} {{")
        
        for field in collection.get("fields", []):
            field_name = field.get("name", "")
            field_type = field.get("type", "string")
            is_key = field.get("is_key", False)
            
            key_marker = "PK" if is_key else ""
            lines.append(f"        {field_type} {field_name} {key_marker}")
        
        # Add embedded documents as nested fields
        for embedded in collection.get("embedded_documents", []):
            emb_name = embedded.get("name", "")
            lines.append(f"        array {emb_name}_list")
        
        lines.append("    }")
    
    # Add relationships
    for rec in recommendations:
        parent = rec.get("parent_table", "")
        child = rec.get("child_table", "")
        decision = rec.get("decision", "").lower()
        
        if decision == "embed":
            lines.append(f"    {parent} ||--o{{ {child} : embeds")
        elif decision == "reference":
            lines.append(f"    {parent} ||--o{{ {child} : references")
    
    return "\n".join(lines)


def generate_html_visualization(schema_data: dict, analysis_data: dict | None = None) -> str:
    """Generate an interactive HTML visualization."""
    
    collections = schema_data.get("target_schema", {}).get("collections", [])
    recommendations = schema_data.get("recommendations", [])
    
    # Build collection cards HTML
    collection_cards = ""
    for collection in collections:
        name = collection.get("name", "unknown")
        
        # Fields
        fields_html = ""
        for field in collection.get("fields", []):
            field_name = field.get("name", "")
            field_type = field.get("type", "string")
            is_key = field.get("is_key", False)
            
            key_badge = '<span class="badge key">PK</span>' if is_key else ""
            fields_html += f"""
                <div class="field">
                    <span class="field-name">{field_name}</span>
                    <span class="field-type">{field_type}</span>
                    {key_badge}
                </div>
            """
        
        # Embedded documents
        embedded_html = ""
        for embedded in collection.get("embedded_documents", []):
            emb_name = embedded.get("name", "")
            emb_source = embedded.get("source_table", "")
            is_array = embedded.get("is_array", True)
            
            emb_fields = ""
            for f in embedded.get("fields", []):
                emb_fields += f'<div class="emb-field">{f.get("name")}: {f.get("type")}</div>'
            
            array_badge = '<span class="badge array">[]</span>' if is_array else ""
            embedded_html += f"""
                <div class="embedded">
                    <div class="embedded-header">
                        <span class="embedded-name">{emb_name}</span>
                        {array_badge}
                        <span class="embedded-source">from {emb_source}</span>
                    </div>
                    <div class="embedded-fields">{emb_fields}</div>
                </div>
            """
        
        # References
        refs_html = ""
        for ref in collection.get("references", []):
            refs_html += f'<div class="reference">→ {ref}</div>'
        
        collection_cards += f"""
            <div class="collection-card">
                <div class="collection-header">
                    <h3>{name}</h3>
                    <span class="source-tables">{', '.join(collection.get('source_tables', []))}</span>
                </div>
                <div class="fields-section">
                    <h4>Fields</h4>
                    {fields_html}
                </div>
                {"<div class='embedded-section'><h4>Embedded Documents</h4>" + embedded_html + "</div>" if embedded_html else ""}
                {"<div class='references-section'><h4>References</h4>" + refs_html + "</div>" if refs_html else ""}
            </div>
        """
    
    # Build recommendations table
    rec_rows = ""
    for rec in recommendations:
        decision = rec.get("decision", "").upper()
        confidence = rec.get("confidence", 0) * 100
        
        decision_class = {
            "EMBED": "decision-embed",
            "REFERENCE": "decision-reference",
            "SEPARATE": "decision-separate"
        }.get(decision, "")
        
        reasoning = "<br>".join(rec.get("reasoning", []))
        warnings = "<br>".join(rec.get("warnings", []))
        
        rec_rows += f"""
            <tr>
                <td>{rec.get("parent_table", "")}</td>
                <td>{rec.get("child_table", "")}</td>
                <td><span class="decision {decision_class}">{decision}</span></td>
                <td>{confidence:.0f}%</td>
                <td class="reasoning">{reasoning}</td>
                <td class="warnings">{warnings}</td>
            </tr>
        """
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MongoDB Schema Visualization</title>
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        header {{
            background: linear-gradient(135deg, #2c3e50, #3498db);
            color: white;
            padding: 2rem;
            margin-bottom: 2rem;
            border-radius: 12px;
        }}
        
        header h1 {{
            font-size: 2rem;
            margin-bottom: 0.5rem;
        }}
        
        header p {{
            opacity: 0.9;
        }}
        
        .section {{
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        
        .section h2 {{
            color: #2c3e50;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #eee;
        }}
        
        /* Collections Grid */
        .collections-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 1.5rem;
        }}
        
        .collection-card {{
            background: #fafbfc;
            border: 1px solid #e1e4e8;
            border-radius: 8px;
            padding: 1rem;
        }}
        
        .collection-header {{
            border-bottom: 1px solid #eee;
            padding-bottom: 0.75rem;
            margin-bottom: 0.75rem;
        }}
        
        .collection-header h3 {{
            color: #0366d6;
            font-size: 1.1rem;
        }}
        
        .source-tables {{
            font-size: 0.8rem;
            color: #666;
        }}
        
        .fields-section h4,
        .embedded-section h4,
        .references-section h4 {{
            font-size: 0.9rem;
            color: #666;
            margin: 0.75rem 0 0.5rem;
        }}
        
        .field {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.25rem 0;
            font-size: 0.9rem;
        }}
        
        .field-name {{
            font-weight: 500;
            color: #24292e;
        }}
        
        .field-type {{
            color: #6f42c1;
            font-family: monospace;
            font-size: 0.85rem;
        }}
        
        .badge {{
            font-size: 0.7rem;
            padding: 0.15rem 0.4rem;
            border-radius: 4px;
            font-weight: 600;
        }}
        
        .badge.key {{
            background: #ffeaa7;
            color: #856404;
        }}
        
        .badge.array {{
            background: #81ecec;
            color: #00695c;
        }}
        
        .embedded {{
            background: #e8f4fd;
            border: 1px solid #bee5eb;
            border-radius: 6px;
            padding: 0.75rem;
            margin-bottom: 0.5rem;
        }}
        
        .embedded-header {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 0.5rem;
        }}
        
        .embedded-name {{
            font-weight: 600;
            color: #0c5460;
        }}
        
        .embedded-source {{
            font-size: 0.75rem;
            color: #666;
        }}
        
        .emb-field {{
            font-size: 0.8rem;
            color: #555;
            padding: 0.1rem 0;
        }}
        
        .reference {{
            font-size: 0.9rem;
            color: #e83e8c;
            padding: 0.25rem 0;
        }}
        
        /* Recommendations Table */
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        
        th, td {{
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        
        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }}
        
        .decision {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.8rem;
        }}
        
        .decision-embed {{
            background: #d4edda;
            color: #155724;
        }}
        
        .decision-reference {{
            background: #cce5ff;
            color: #004085;
        }}
        
        .decision-separate {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .reasoning {{
            font-size: 0.85rem;
            color: #666;
            max-width: 300px;
        }}
        
        .warnings {{
            font-size: 0.85rem;
            color: #dc3545;
            max-width: 200px;
        }}
        
        /* Mermaid Diagram */
        .mermaid-container {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 1rem;
            overflow-x: auto;
        }}
        
        pre.mermaid-code {{
            background: #2d2d2d;
            color: #f8f8f2;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem;
        }}
        
        /* Stats */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }}
        
        .stat-card {{
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: 700;
            color: #2c3e50;
        }}
        
        .stat-label {{
            font-size: 0.85rem;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🍃 MongoDB Schema Design</h1>
            <p>Recommended schema based on access pattern analysis</p>
        </header>
        
        <div class="section">
            <h2>📊 Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{len(collections)}</div>
                    <div class="stat-label">Collections</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{len([r for r in recommendations if r.get('decision', '').lower() == 'embed'])}</div>
                    <div class="stat-label">Embedded</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{len([r for r in recommendations if r.get('decision', '').lower() == 'reference'])}</div>
                    <div class="stat-label">Referenced</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>📁 Collections</h2>
            <div class="collections-grid">
                {collection_cards}
            </div>
        </div>
        
        <div class="section">
            <h2>💡 Recommendations</h2>
            <table>
                <thead>
                    <tr>
                        <th>Parent</th>
                        <th>Child</th>
                        <th>Decision</th>
                        <th>Confidence</th>
                        <th>Reasoning</th>
                        <th>Warnings</th>
                    </tr>
                </thead>
                <tbody>
                    {rec_rows}
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📐 ER Diagram (Mermaid)</h2>
            <p style="margin-bottom: 1rem; color: #666;">Copy this code to <a href="https://mermaid.live" target="_blank">mermaid.live</a> to view the diagram:</p>
            <pre class="mermaid-code">{generate_mermaid_diagram(schema_data)}</pre>
        </div>
    </div>
</body>
</html>
    """
    
    return html


def generate_tree_view(schema_data: dict) -> str:
    """Generate a console-friendly tree view."""
    lines = ["MongoDB Schema Design", "=" * 50, ""]
    
    collections = schema_data.get("target_schema", {}).get("collections", [])
    
    for i, collection in enumerate(collections):
        is_last = i == len(collections) - 1
        prefix = "└── " if is_last else "├── "
        child_prefix = "    " if is_last else "│   "
        
        name = collection.get("name", "unknown")
        lines.append(f"{prefix}📁 {name}")
        
        # Fields
        fields = collection.get("fields", [])
        embedded = collection.get("embedded_documents", [])
        references = collection.get("references", [])
        
        for j, field in enumerate(fields):
            is_last_field = j == len(fields) - 1 and not embedded and not references
            field_prefix = "└── " if is_last_field else "├── "
            
            field_name = field.get("name", "")
            field_type = field.get("type", "string")
            is_key = field.get("is_key", False)
            
            key_marker = " 🔑" if is_key else ""
            lines.append(f"{child_prefix}{field_prefix}{field_name}: {field_type}{key_marker}")
        
        # Embedded documents
        for j, emb in enumerate(embedded):
            is_last_emb = j == len(embedded) - 1 and not references
            emb_prefix = "└── " if is_last_emb else "├── "
            emb_child = "    " if is_last_emb else "│   "
            
            emb_name = emb.get("name", "")
            lines.append(f"{child_prefix}{emb_prefix}📎 {emb_name}[] (embedded)")
            
            for k, f in enumerate(emb.get("fields", [])):
                is_last_f = k == len(emb.get("fields", [])) - 1
                f_prefix = "└── " if is_last_f else "├── "
                lines.append(f"{child_prefix}{emb_child}{f_prefix}{f.get('name')}: {f.get('type')}")
        
        # References
        for j, ref in enumerate(references):
            is_last_ref = j == len(references) - 1
            ref_prefix = "└── " if is_last_ref else "├── "
            lines.append(f"{child_prefix}{ref_prefix}→ {ref} (reference)")
        
        lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Visualize MongoDB schema recommendations")
    parser.add_argument(
        "--input", "-i",
        type=Path,
        required=True,
        help="Input JSON file from schema-travels analyze"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output file path"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["html", "mermaid", "tree"],
        default="html",
        help="Output format (default: html)"
    )
    
    args = parser.parse_args()
    
    # Load input
    with open(args.input) as f:
        data = json.load(f)
    
    # Generate output
    if args.format == "html":
        output = generate_html_visualization(data)
        suffix = ".html"
    elif args.format == "mermaid":
        output = generate_mermaid_diagram(data)
        suffix = ".mmd"
    else:
        output = generate_tree_view(data)
        suffix = ".txt"
    
    # Write or print
    if args.output:
        output_path = args.output
    else:
        output_path = args.input.with_suffix(suffix)
    
    if args.format == "tree" and not args.output:
        print(output)
    else:
        output_path.write_text(output)
        print(f"✓ Generated: {output_path}")


if __name__ == "__main__":
    main()
