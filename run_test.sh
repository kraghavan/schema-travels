#!/bin/bash
# =============================================================================
# Schema Travels - Complete Test Run
# =============================================================================
# This script:
# 1. Generates synthetic PostgreSQL query logs (10K-50K queries)
# 2. Runs access pattern analysis
# 3. Generates MongoDB schema recommendations
# 4. Creates visualizations
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          Schema Travels - Complete Test Run                  ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Configuration
QUERIES=${1:-10000}
PATTERN=${2:-ecommerce}
OUTPUT_DIR="./test_run_$(date +%Y%m%d_%H%M%S)"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Queries to generate: $QUERIES"
echo "  Workload pattern: $PATTERN"
echo "  Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR/logs"

# =============================================================================
# Step 1: Generate Synthetic Workload
# =============================================================================
echo -e "${GREEN}Step 1: Generating synthetic workload...${NC}"

python tools/generate_workload.py \
    --queries $QUERIES \
    --pattern $PATTERN \
    --output "$OUTPUT_DIR/logs" \
    --seed 42

echo ""

# =============================================================================
# Step 2: Run Analysis
# =============================================================================
echo -e "${GREEN}Step 2: Running access pattern analysis...${NC}"

# Use the extended schema
SCHEMA_FILE="examples/ecommerce_extended_schema.sql"
if [ ! -f "$SCHEMA_FILE" ]; then
    SCHEMA_FILE="examples/ecommerce_schema.sql"
fi

schema-travels analyze \
    --logs-dir "$OUTPUT_DIR/logs" \
    --schema-file "$SCHEMA_FILE" \
    --db-type postgres \
    --target mongodb \
    --output "$OUTPUT_DIR/analysis.json"

echo ""

# =============================================================================
# Step 3: Generate Visualizations
# =============================================================================
echo -e "${GREEN}Step 3: Generating visualizations...${NC}"

# HTML visualization
python tools/visualize_schema.py \
    --input "$OUTPUT_DIR/analysis.json" \
    --output "$OUTPUT_DIR/schema_visualization.html" \
    --format html

# Mermaid diagram
python tools/visualize_schema.py \
    --input "$OUTPUT_DIR/analysis.json" \
    --output "$OUTPUT_DIR/schema_diagram.mmd" \
    --format mermaid

# Tree view
python tools/visualize_schema.py \
    --input "$OUTPUT_DIR/analysis.json" \
    --format tree > "$OUTPUT_DIR/schema_tree.txt"

echo ""

# =============================================================================
# Summary
# =============================================================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Test run complete!${NC}"
echo ""
echo -e "${YELLOW}Output files:${NC}"
echo "  📄 $OUTPUT_DIR/analysis.json          - Full analysis results"
echo "  🌐 $OUTPUT_DIR/schema_visualization.html - Interactive visualization"
echo "  📊 $OUTPUT_DIR/schema_diagram.mmd     - Mermaid ER diagram"
echo "  🌳 $OUTPUT_DIR/schema_tree.txt        - Text tree view"
echo "  📝 $OUTPUT_DIR/logs/postgresql.log    - Generated query logs"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Open schema_visualization.html in your browser"
echo "  2. Copy schema_diagram.mmd contents to https://mermaid.live"
echo "  3. Review analysis.json for detailed metrics"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Open HTML in browser (macOS)
if command -v open &> /dev/null; then
    echo "Opening visualization in browser..."
    open "$OUTPUT_DIR/schema_visualization.html"
fi
