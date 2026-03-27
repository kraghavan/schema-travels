#!/usr/bin/env python3
"""
Schema Visualizer (MongoDB + DynamoDB)

Generates visual representations of schema recommendations:
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


# =============================================================================
# Detection
# =============================================================================

def detect_target_db(data: dict) -> str:
    """Detect if the schema is MongoDB or DynamoDB."""
    target_schema = data.get("target_schema", {})
    
    # Check for DynamoDB markers
    if target_schema.get("metadata", {}).get("dynamodb_design"):
        return "dynamodb"
    if target_schema.get("metadata", {}).get("design_mode"):
        return "dynamodb"
    
    # Check for MongoDB markers
    if target_schema.get("collections"):
        return "mongodb"
    
    # Default based on target_db field
    return data.get("target_db", "mongodb")


# =============================================================================
# MongoDB Visualization
# =============================================================================

def generate_mermaid_diagram_mongodb(schema_data: dict) -> str:
    """Generate a Mermaid ER diagram from MongoDB schema data."""
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


def generate_html_visualization_mongodb(schema_data: dict) -> str:
    """Generate an interactive HTML visualization for MongoDB."""
    
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
    {get_common_styles()}
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
            <pre class="mermaid-code">{generate_mermaid_diagram_mongodb(schema_data)}</pre>
        </div>
    </div>
</body>
</html>
    """
    
    return html


def generate_tree_view_mongodb(schema_data: dict) -> str:
    """Generate a console-friendly tree view for MongoDB."""
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


# =============================================================================
# DynamoDB Visualization
# =============================================================================

def generate_mermaid_diagram_dynamodb(schema_data: dict) -> str:
    """Generate a Mermaid ER diagram from DynamoDB schema data."""
    lines = ["erDiagram"]
    
    metadata = schema_data.get("target_schema", {}).get("metadata", {})
    design = metadata.get("dynamodb_design", {})
    design_mode = design.get("design_mode", metadata.get("design_mode", "unknown"))
    
    if design_mode == "single_table":
        # Single-table design - show entities
        entities = design.get("entities", [])
        table_name = design.get("table_name", "MainTable")
        
        lines.append(f"    {table_name} {{")
        lines.append("        string PK PK")
        lines.append("        string SK SK")
        lines.append("    }")
        
        for entity in entities:
            name = entity.get("name", "Entity")
            lines.append(f"    {name} {{")
            lines.append(f"        string PK \"{entity.get('pk_pattern', '')}\"")
            lines.append(f"        string SK \"{entity.get('sk_pattern', '')}\"")
            for attr in entity.get("attributes", [])[:5]:
                lines.append(f"        string {attr}")
            lines.append("    }")
            lines.append(f"    {table_name} ||--o{{ {name} : contains")
        
        # GSIs
        for gsi in design.get("gsis", []):
            gsi_name = gsi.get("name", "GSI")
            lines.append(f"    {gsi_name} {{")
            lines.append(f"        string {gsi.get('pk_attribute', 'GSIPK')} PK")
            if gsi.get("sk_attribute"):
                lines.append(f"        string {gsi.get('sk_attribute', 'GSISK')} SK")
            lines.append("    }")
            lines.append(f"    {table_name} ||--|| {gsi_name} : indexes")
    else:
        # Multi-table design
        tables = design.get("tables", [])
        
        for table in tables:
            table_name = table.get("table_name", "Table")
            pk = table.get("partition_key", "id")
            sk = table.get("sort_key")
            
            lines.append(f"    {table_name} {{")
            lines.append(f"        string {pk} PK")
            if sk:
                lines.append(f"        string {sk} SK")
            lines.append("    }")
            
            # GSIs for this table
            for gsi in table.get("gsis", []):
                gsi_name = f"{table_name}_{gsi.get('name', 'GSI')}"
                lines.append(f"    {gsi_name} {{")
                lines.append(f"        string {gsi.get('pk_attribute', 'GSIPK')} PK")
                lines.append("    }")
                lines.append(f"    {table_name} ||--|| {gsi_name} : indexes")
    
    return "\n".join(lines)


def generate_html_visualization_dynamodb(schema_data: dict) -> str:
    """Generate an interactive HTML visualization for DynamoDB."""
    
    metadata = schema_data.get("target_schema", {}).get("metadata", {})
    design = metadata.get("dynamodb_design", {})
    design_mode = design.get("design_mode", metadata.get("design_mode", "unknown"))
    confidence = metadata.get("confidence", design.get("confidence", 0))
    rationale = metadata.get("rationale", design.get("rationale", ""))
    
    # Design mode badge
    mode_color = "#28a745" if design_mode == "single_table" else "#007bff"
    mode_label = "SINGLE-TABLE" if design_mode == "single_table" else "MULTI-TABLE"
    
    # Build content based on design mode
    if design_mode == "single_table":
        content_html = _build_single_table_html(design)
    else:
        content_html = _build_multi_table_html(design)
    
    # GSI summary
    all_gsis = design.get("gsis", [])
    if design_mode != "single_table":
        for table in design.get("tables", []):
            all_gsis.extend(table.get("gsis", []))
    
    gsi_html = ""
    for gsi in all_gsis:
        proj_type = gsi.get("projection_type", "ALL")
        proj_color = {"ALL": "#28a745", "INCLUDE": "#ffc107", "KEYS_ONLY": "#6c757d"}.get(proj_type, "#6c757d")
        
        gsi_html += f"""
            <div class="gsi-card">
                <div class="gsi-header">
                    <span class="gsi-name">{gsi.get('name', 'GSI')}</span>
                    <span class="badge" style="background: {proj_color}; color: white;">{proj_type}</span>
                </div>
                <div class="gsi-keys">
                    <div>PK: <code>{gsi.get('pk_attribute', '-')}</code></div>
                    <div>SK: <code>{gsi.get('sk_attribute', '-') or '-'}</code></div>
                </div>
                <div class="gsi-pattern">{gsi.get('access_pattern', '')}</div>
            </div>
        """
    
    # Warnings
    warnings_html = ""
    for warning in design.get("warnings", []):
        warnings_html += f'<div class="warning-item">⚠️ {warning}</div>'
    
    # Clusters (for debugging/info)
    clusters = design.get("clusters", [])
    cluster_count = len(clusters)
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DynamoDB Schema Visualization</title>
    {get_common_styles()}
    {get_dynamodb_styles()}
</head>
<body>
    <div class="container">
        <header class="dynamodb-header">
            <h1>⚡ DynamoDB Schema Design</h1>
            <p>Recommended schema based on access pattern analysis</p>
        </header>
        
        <div class="section">
            <h2>📊 Design Summary</h2>
            <div class="design-summary">
                <div class="design-mode" style="border-color: {mode_color};">
                    <span class="mode-badge" style="background: {mode_color};">{mode_label}</span>
                    <div class="confidence">Confidence: {confidence:.0%}</div>
                </div>
                <div class="rationale">{rationale}</div>
            </div>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{len(design.get('tables', design.get('entities', [])))}</div>
                    <div class="stat-label">{"Entities" if design_mode == "single_table" else "Tables"}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{len(all_gsis)}</div>
                    <div class="stat-label">GSIs</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{cluster_count}</div>
                    <div class="stat-label">Access Clusters</div>
                </div>
            </div>
        </div>
        
        {content_html}
        
        <div class="section">
            <h2>🔍 Global Secondary Indexes</h2>
            <div class="gsi-grid">
                {gsi_html if gsi_html else '<p class="no-data">No GSIs detected</p>'}
            </div>
        </div>
        
        {"<div class='section warnings-section'><h2>⚠️ Warnings</h2>" + warnings_html + "</div>" if warnings_html else ""}
        
        <div class="section">
            <h2>📐 ER Diagram (Mermaid)</h2>
            <p style="margin-bottom: 1rem; color: #666;">Copy this code to <a href="https://mermaid.live" target="_blank">mermaid.live</a> to view the diagram:</p>
            <pre class="mermaid-code">{generate_mermaid_diagram_dynamodb(schema_data)}</pre>
        </div>
    </div>
</body>
</html>
    """
    
    return html


def _build_single_table_html(design: dict) -> str:
    """Build HTML for single-table design."""
    entities = design.get("entities", [])
    table_name = design.get("table_name", "MainTable")
    pk = design.get("partition_key", "PK")
    sk = design.get("sort_key", "SK")
    
    entity_cards = ""
    for entity in entities:
        attrs_html = "".join([f'<span class="attr">{a}</span>' for a in entity.get("attributes", [])[:8]])
        
        entity_cards += f"""
            <div class="entity-card">
                <div class="entity-header">
                    <span class="entity-name">{entity.get('name', 'Entity')}</span>
                    <span class="source-table">from {entity.get('source_table', '')}</span>
                </div>
                <div class="entity-keys">
                    <div class="key-pattern">
                        <span class="key-label">PK:</span>
                        <code>{entity.get('pk_pattern', '')}</code>
                    </div>
                    <div class="key-pattern">
                        <span class="key-label">SK:</span>
                        <code>{entity.get('sk_pattern', '')}</code>
                    </div>
                </div>
                <div class="entity-attrs">
                    {attrs_html}
                </div>
            </div>
        """
    
    return f"""
        <div class="section">
            <h2>🗄️ Single Table: {table_name}</h2>
            <div class="table-keys">
                <div class="primary-key">
                    <span class="key-name">Partition Key:</span>
                    <code>{pk}</code> (String)
                </div>
                <div class="primary-key">
                    <span class="key-name">Sort Key:</span>
                    <code>{sk}</code> (String)
                </div>
            </div>
            <h3>Entity Patterns</h3>
            <div class="entities-grid">
                {entity_cards}
            </div>
        </div>
    """


def _build_multi_table_html(design: dict) -> str:
    """Build HTML for multi-table design."""
    tables = design.get("tables", [])
    
    table_cards = ""
    for table in tables:
        gsi_badges = ""
        for gsi in table.get("gsis", []):
            gsi_badges += f'<span class="gsi-badge">{gsi.get("name", "GSI")}</span>'
        
        table_cards += f"""
            <div class="table-card">
                <div class="table-header">
                    <span class="table-name">{table.get('table_name', 'Table')}</span>
                </div>
                <div class="table-keys">
                    <div class="key-row">
                        <span class="key-label">PK:</span>
                        <code>{table.get('partition_key', 'id')}</code>
                    </div>
                    {"<div class='key-row'><span class='key-label'>SK:</span><code>" + table.get('sort_key', '') + "</code></div>" if table.get('sort_key') else ""}
                </div>
                <div class="table-gsis">
                    {gsi_badges if gsi_badges else '<span class="no-gsi">No GSIs</span>'}
                </div>
            </div>
        """
    
    return f"""
        <div class="section">
            <h2>🗄️ DynamoDB Tables ({len(tables)})</h2>
            <div class="tables-grid">
                {table_cards}
            </div>
        </div>
    """


def generate_tree_view_dynamodb(schema_data: dict) -> str:
    """Generate a console-friendly tree view for DynamoDB."""
    metadata = schema_data.get("target_schema", {}).get("metadata", {})
    design = metadata.get("dynamodb_design", {})
    design_mode = design.get("design_mode", metadata.get("design_mode", "unknown"))
    confidence = metadata.get("confidence", design.get("confidence", 0))
    
    lines = [
        "DynamoDB Schema Design",
        "=" * 50,
        f"Mode: {design_mode.upper().replace('_', '-')}",
        f"Confidence: {confidence:.0%}",
        ""
    ]
    
    if design_mode == "single_table":
        table_name = design.get("table_name", "MainTable")
        lines.append(f"📦 {table_name}")
        lines.append(f"│   PK: {design.get('partition_key', 'PK')} (String)")
        lines.append(f"│   SK: {design.get('sort_key', 'SK')} (String)")
        lines.append("│")
        lines.append("├── 📋 Entities")
        
        entities = design.get("entities", [])
        for i, entity in enumerate(entities):
            is_last = i == len(entities) - 1
            prefix = "│   └── " if is_last else "│   ├── "
            child_prefix = "│       " if is_last else "│   │   "
            
            lines.append(f"{prefix}🔹 {entity.get('name', 'Entity')}")
            lines.append(f"{child_prefix}PK: {entity.get('pk_pattern', '')}")
            lines.append(f"{child_prefix}SK: {entity.get('sk_pattern', '')}")
            lines.append(f"{child_prefix}Source: {entity.get('source_table', '')}")
        
        lines.append("│")
        lines.append("└── 🔍 GSIs")
        
        gsis = design.get("gsis", [])
        for i, gsi in enumerate(gsis):
            is_last = i == len(gsis) - 1
            prefix = "    └── " if is_last else "    ├── "
            child_prefix = "        " if is_last else "    │   "
            
            lines.append(f"{prefix}{gsi.get('name', 'GSI')}")
            lines.append(f"{child_prefix}PK: {gsi.get('pk_attribute', '')}")
            lines.append(f"{child_prefix}SK: {gsi.get('sk_attribute', '-')}")
            lines.append(f"{child_prefix}Projection: {gsi.get('projection_type', 'ALL')}")
    else:
        # Multi-table
        tables = design.get("tables", [])
        for i, table in enumerate(tables):
            is_last = i == len(tables) - 1
            prefix = "└── " if is_last else "├── "
            child_prefix = "    " if is_last else "│   "
            
            lines.append(f"{prefix}📦 {table.get('table_name', 'Table')}")
            lines.append(f"{child_prefix}PK: {table.get('partition_key', 'id')}")
            if table.get("sort_key"):
                lines.append(f"{child_prefix}SK: {table.get('sort_key')}")
            
            gsis = table.get("gsis", [])
            if gsis:
                lines.append(f"{child_prefix}GSIs: {', '.join(g.get('name', 'GSI') for g in gsis)}")
            lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# Common Styles
# =============================================================================

def get_common_styles() -> str:
    """Get common CSS styles."""
    return """
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        header {
            background: linear-gradient(135deg, #2c3e50, #3498db);
            color: white;
            padding: 2rem;
            margin-bottom: 2rem;
            border-radius: 12px;
        }
        
        header h1 {
            font-size: 2rem;
            margin-bottom: 0.5rem;
        }
        
        header p {
            opacity: 0.9;
        }
        
        .section {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        .section h2 {
            color: #2c3e50;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #eee;
        }
        
        .section h3 {
            color: #34495e;
            margin: 1.5rem 0 1rem;
            font-size: 1.1rem;
        }
        
        /* Collections/Tables Grid */
        .collections-grid, .tables-grid, .entities-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 1.5rem;
        }
        
        .collection-card, .table-card, .entity-card {
            background: #fafbfc;
            border: 1px solid #e1e4e8;
            border-radius: 8px;
            padding: 1rem;
        }
        
        .collection-header, .table-header, .entity-header {
            border-bottom: 1px solid #eee;
            padding-bottom: 0.75rem;
            margin-bottom: 0.75rem;
        }
        
        .collection-header h3, .table-name, .entity-name {
            color: #0366d6;
            font-size: 1.1rem;
            font-weight: 600;
        }
        
        .source-tables, .source-table {
            font-size: 0.8rem;
            color: #666;
        }
        
        /* Stats */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }
        
        .stat-card {
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            color: #2c3e50;
        }
        
        .stat-label {
            font-size: 0.85rem;
            color: #666;
        }
        
        /* Code */
        code {
            background: #f1f3f5;
            padding: 0.2rem 0.4rem;
            border-radius: 4px;
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem;
        }
        
        pre.mermaid-code {
            background: #2d2d2d;
            color: #f8f8f2;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem;
        }
        
        /* Table */
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }
        
        th, td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        
        th {
            background: #f8f9fa;
            font-weight: 600;
            color: #2c3e50;
        }
        
        .badge {
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .badge.key {
            background: #ffc107;
            color: #333;
        }
        
        .decision {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.8rem;
        }
        
        .decision-embed {
            background: #d4edda;
            color: #155724;
        }
        
        .decision-reference {
            background: #cce5ff;
            color: #004085;
        }
        
        .no-data {
            color: #666;
            font-style: italic;
        }
    </style>
    """


def get_dynamodb_styles() -> str:
    """Get DynamoDB-specific CSS styles."""
    return """
    <style>
        .dynamodb-header {
            background: linear-gradient(135deg, #232f3e, #ff9900) !important;
        }
        
        .design-summary {
            display: flex;
            align-items: center;
            gap: 1.5rem;
            margin-bottom: 1.5rem;
            padding: 1rem;
            background: #f8f9fa;
            border-radius: 8px;
        }
        
        .design-mode {
            border-left: 4px solid;
            padding-left: 1rem;
        }
        
        .mode-badge {
            display: inline-block;
            padding: 0.4rem 0.8rem;
            border-radius: 6px;
            color: white;
            font-weight: 700;
            font-size: 0.9rem;
        }
        
        .confidence {
            margin-top: 0.5rem;
            font-size: 0.9rem;
            color: #666;
        }
        
        .rationale {
            color: #555;
            flex: 1;
        }
        
        .table-keys, .entity-keys {
            margin: 0.75rem 0;
        }
        
        .primary-key, .key-row {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin: 0.25rem 0;
        }
        
        .key-name, .key-label {
            font-weight: 600;
            color: #555;
            min-width: 100px;
        }
        
        .key-pattern {
            margin: 0.25rem 0;
        }
        
        .entity-attrs {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-top: 0.75rem;
        }
        
        .attr {
            background: #e9ecef;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.8rem;
            color: #495057;
        }
        
        /* GSIs */
        .gsi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1rem;
        }
        
        .gsi-card {
            background: #f8f9fa;
            border: 1px solid #e1e4e8;
            border-radius: 8px;
            padding: 1rem;
        }
        
        .gsi-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
        }
        
        .gsi-name {
            font-weight: 600;
            color: #ff9900;
        }
        
        .gsi-keys {
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }
        
        .gsi-pattern {
            font-size: 0.85rem;
            color: #666;
            font-style: italic;
        }
        
        .gsi-badge {
            display: inline-block;
            background: #fff3cd;
            color: #856404;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            margin-right: 0.25rem;
        }
        
        .no-gsi {
            color: #999;
            font-size: 0.85rem;
        }
        
        .table-gsis {
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid #eee;
        }
        
        /* Warnings */
        .warnings-section {
            background: #fff8e1;
            border-left: 4px solid #ffc107;
        }
        
        .warning-item {
            padding: 0.5rem 0;
            border-bottom: 1px solid #ffe082;
        }
        
        .warning-item:last-child {
            border-bottom: none;
        }
    </style>
    """


# =============================================================================
# Router Functions
# =============================================================================

def generate_mermaid_diagram(schema_data: dict) -> str:
    """Route to appropriate Mermaid generator."""
    target = detect_target_db(schema_data)
    if target == "dynamodb":
        return generate_mermaid_diagram_dynamodb(schema_data)
    return generate_mermaid_diagram_mongodb(schema_data)


def generate_html_visualization(schema_data: dict) -> str:
    """Route to appropriate HTML generator."""
    target = detect_target_db(schema_data)
    if target == "dynamodb":
        return generate_html_visualization_dynamodb(schema_data)
    return generate_html_visualization_mongodb(schema_data)


def generate_tree_view(schema_data: dict) -> str:
    """Route to appropriate tree view generator."""
    target = detect_target_db(schema_data)
    if target == "dynamodb":
        return generate_tree_view_dynamodb(schema_data)
    return generate_tree_view_mongodb(schema_data)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Visualize MongoDB/DynamoDB schema recommendations")
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
    
    # Detect target DB
    target = detect_target_db(data)
    print(f"Detected target: {target.upper()}")
    
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
