"""Main CLI entry point for Schema Travels."""

import json
import logging
import sys
import uuid
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from schema_travels import __version__
from schema_travels.config import get_settings, APIKeyNotConfiguredError
from schema_travels.collector import PostgresLogParser, MySQLLogParser, SchemaParser
from schema_travels.analyzer import PatternAnalyzer, MutationAnalyzer
from schema_travels.recommender import ClaudeAdvisor, SchemaGenerator, generate_rewrites
from schema_travels.recommender.models import TargetDatabase
from schema_travels.recommender.cache import compute_input_hash, get_cache, CacheMode
# v2.0.0: DynamoDB imports
from schema_travels.recommender.dynamodb_models import DesignMode
from schema_travels.recommender.dynamodb_output import DynamoDBOutputFormatter
from schema_travels.simulator import MigrationSimulator, SimulationConfig
from schema_travels.persistence import Database, AnalysisRepository

console = Console()
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@click.group()
@click.version_option(version=__version__)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def cli(verbose: bool) -> None:
    """Schema Travels - SQL to NoSQL Migration Analyzer.

    Analyze your database access patterns and get recommendations
    for optimal NoSQL schema design.
    """
    setup_logging(verbose)


@cli.command()
@click.option(
    "--logs-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Directory containing database query logs",
)
@click.option(
    "--schema-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="SQL schema file (DDL)",
)
@click.option(
    "--db-type",
    type=click.Choice(["postgres", "mysql"]),
    default="postgres",
    help="Source database type",
)
@click.option(
    "--target",
    type=click.Choice(["mongodb", "dynamodb"]),
    default="mongodb",
    help="Target database type",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    help="Output file for results (JSON)",
)
@click.option(
    "--use-ai/--no-ai",
    default=True,
    help="Use Claude AI for recommendations",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Bypass recommendation cache and get fresh AI analysis",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Clear all cached recommendations before running",
)
@click.option(
    "--cache-mode",
    type=click.Choice(["relaxed", "strict"]),
    default="relaxed",
    help="Cache mode: 'relaxed' ignores small log changes, 'strict' invalidates on any change",
)
@click.option(
    "--min-confidence",
    type=float,
    default=None,
    help="Only show recommendations at or above this confidence threshold (0.0-1.0)",
)
@click.option(
    "--show-rewrites",
    is_flag=True,
    default=False,
    help="Display SQL → MongoDB query rewrite examples for each recommendation",
)
# v2.0.0: DynamoDB-specific options
@click.option(
    "--dynamodb-mode",
    type=click.Choice(["auto", "single", "multi"]),
    default="auto",
    help="DynamoDB design mode: 'auto' decides based on access patterns, 'single' forces single-table, 'multi' forces multi-table",
)
@click.option(
    "--dynamodb-output",
    type=click.Choice(["json", "terraform", "nosql_workbench"]),
    default=None,
    help="DynamoDB output format (default: json). Use 'terraform' for IaC or 'nosql_workbench' for AWS tool import",
)
def analyze(
    logs_dir: Path,
    schema_file: Path,
    db_type: str,
    target: str,
    output: Path | None,
    use_ai: bool,
    no_cache: bool,
    clear_cache: bool,
    cache_mode: str,
    min_confidence: float | None,
    show_rewrites: bool,
    dynamodb_mode: str,
    dynamodb_output: str | None,
) -> None:
    """Analyze database access patterns and generate recommendations.

    Parses query logs and schema to identify hot joins, mutation patterns,
    and co-access patterns. Generates recommendations for NoSQL schema design.
    
    Cache modes:
    
    \b
    - relaxed (default): Ignores small log changes. Cache invalidates only when
      schema changes or access patterns significantly change (new joins, tables
      flip from read-heavy to write-heavy).
    
    \b
    - strict: Any change in query counts invalidates cache. Use when you want
      fresh recommendations for every data change.
    
    Use --no-cache to bypass cache entirely for one run.
    Use --clear-cache to invalidate all cached recommendations.
    
    DynamoDB modes (v2.0.0):
    
    \b
    - auto (default): Analyzes access patterns to decide between single-table
      and multi-table design. Uses >70% co-access threshold.
    
    \b
    - single: Forces single-table design. Best when tables are frequently
      accessed together (high co-access).
    
    \b
    - multi: Forces multi-table design. Best when tables are accessed
      independently or have different scaling requirements.
    """
    analysis_id = str(uuid.uuid4())[:8]
    target_db = TargetDatabase(target)
    
    # v2.0.0: Parse DynamoDB mode
    dynamo_design_mode = {
        "auto": DesignMode.AUTO,
        "single": DesignMode.SINGLE_TABLE,
        "multi": DesignMode.MULTI_TABLE,
    }.get(dynamodb_mode, DesignMode.AUTO)
    
    # Handle cache clearing
    cache = get_cache()
    if clear_cache:
        count = cache.invalidate_all()
        console.print(f"[yellow]Cleared {count} cached recommendations[/yellow]")

    console.print(Panel.fit(
        f"[bold blue]Schema Travels Analysis[/bold blue]\n"
        f"Analysis ID: {analysis_id}",
        title="Starting Analysis",
    ))

    # Initialize repository
    repo = AnalysisRepository()
    repo.create_analysis(
        analysis_id=analysis_id,
        source_db_type=db_type,
        target_db_type=target,
        logs_dir=str(logs_dir),
        schema_file=str(schema_file),
    )

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            # Parse schema
            task = progress.add_task("Parsing schema...", total=None)
            schema_parser = SchemaParser(dialect=db_type)
            schema = schema_parser.parse_file(schema_file)
            progress.update(task, completed=True)
            console.print(f"  Found {len(schema.tables)} tables, {len(schema.foreign_keys)} relationships")

            # Parse logs
            task = progress.add_task("Parsing query logs...", total=None)
            if db_type == "postgres":
                log_parser = PostgresLogParser(logs_dir)
            else:
                log_parser = MySQLLogParser(logs_dir)

            queries = log_parser.parse()
            progress.update(task, completed=True)
            console.print(f"  Parsed {len(queries)} queries")

            # Analyze patterns
            task = progress.add_task("Analyzing access patterns...", total=None)
            analyzer = PatternAnalyzer(schema)
            result = analyzer.analyze(queries, source_db_type=db_type)
            result.analysis_id = analysis_id
            progress.update(task, completed=True)
            
            # v2.0.0: Run MutationAnalyzer for SELECT clause extraction (DynamoDB GSI optimization)
            mutation_analyzer = None
            if target_db == TargetDatabase.DYNAMODB:
                task = progress.add_task("Analyzing SELECT patterns for GSI optimization...", total=None)
                mutation_analyzer = MutationAnalyzer()
                mutation_analyzer.analyze(queries)
                progress.update(task, completed=True)
                console.print(f"  Tracked {len(mutation_analyzer.selected_columns)} tables with SELECT patterns")

            # Save analysis result
            repo.save_analysis_result(result)

            # Get recommendations
            recommendations = []
            cache_used = False
            valid_recs = []  # Initialize here for use later
            dynamodb_review = None  # v2.0.1: AI review for DynamoDB
            
            # v2.0.1: Different AI flow for DynamoDB vs MongoDB
            if target_db == TargetDatabase.DYNAMODB:
                # DynamoDB: Local design + optional AI review
                if use_ai:
                    settings = get_settings()
                    if settings.has_api_key():
                        # First, generate local design to review
                        task = progress.add_task("Generating DynamoDB design...", total=None)
                        
                        # Build table stats for designer
                        table_stats = []
                        for ts in result.table_statistics:
                            table_selected = mutation_analyzer.selected_columns.get(ts.table, {}) if mutation_analyzer else {}
                            sorted_selected = sorted(table_selected.items(), key=lambda x: x[1], reverse=True)
                            from schema_travels.analyzer.models import TableStatistics
                            table_stats.append(TableStatistics(
                                table=ts.table,
                                total_accesses=ts.total_accesses,
                                solo_accesses=ts.solo_accesses,
                                joined_accesses=ts.joined_accesses,
                                total_time_ms=ts.total_time_ms,
                                frequently_filtered_columns=ts.frequently_filtered_columns,
                                frequently_updated_columns=ts.frequently_updated_columns,
                                frequently_selected_columns=[col for col, _ in sorted_selected[:10]],
                                has_select_star=ts.table in (mutation_analyzer.select_star_tables if mutation_analyzer else set()),
                            ))
                        
                        # Create local design
                        from schema_travels.recommender.dynamodb_designer import DynamoDBDesigner
                        designer = DynamoDBDesigner(
                            mode=dynamo_design_mode,
                            co_access_threshold=0.70,
                        )
                        local_design = designer.design(
                            table_stats=table_stats,
                            access_patterns=result.access_patterns,
                            join_patterns=result.join_patterns,
                            mutation_patterns=result.mutation_patterns,
                            filtered_columns=dict(mutation_analyzer.filtered_columns) if mutation_analyzer else {},
                            selected_columns=dict(mutation_analyzer.selected_columns) if mutation_analyzer else {},
                            select_star_tables=mutation_analyzer.select_star_tables if mutation_analyzer else set(),
                        )
                        progress.update(task, completed=True)
                        console.print(f"  Local design: [bold]{local_design.design_mode.value}[/bold] (confidence: {local_design.confidence:.0%})")
                        
                        # Compute cache key for review
                        mode = CacheMode(cache_mode)
                        input_hash = compute_input_hash(schema, result, target_db, mode) + "_review"
                        
                        # Check cache for review
                        cached_review = None
                        if not no_cache:
                            task = progress.add_task("Checking review cache...", total=None)
                            cached_review = cache.get(input_hash)
                            progress.update(task, completed=True)
                            
                            if cached_review:
                                # Reconstruct review from cached dict
                                from schema_travels.recommender.dynamodb_models import DynamoDBReview
                                try:
                                    dynamodb_review = DynamoDBReview(**cached_review)
                                    cache_used = True
                                    console.print(f"  [green]✓ Using cached AI review[/green] [dim](hash: {input_hash})[/dim]")
                                except Exception as e:
                                    logger.warning(f"Failed to load cached review: {e}")
                                    cached_review = None
                        
                        # If not cached, get AI review
                        if not cached_review:
                            try:
                                task = progress.add_task("Getting AI review of design...", total=None)
                                advisor = ClaudeAdvisor()
                                dynamodb_review = advisor.review_dynamodb_design(
                                    local_design, result, schema
                                )
                                progress.update(task, completed=True)
                                
                                # Cache the review
                                cache.put(input_hash, dynamodb_review.to_dict(), metadata={
                                    "analysis_id": analysis_id,
                                    "design_mode": local_design.design_mode.value,
                                    "cache_mode": cache_mode,
                                })
                                console.print(f"  [dim]Cached AI review (hash: {input_hash})[/dim]")
                                
                                # Show review summary
                                if dynamodb_review.approved:
                                    if dynamodb_review.has_changes:
                                        console.print(f"  [green]✓ AI approved with {dynamodb_review.change_count} suggestions[/green]")
                                    else:
                                        console.print(f"  [green]✓ AI approved design (no changes)[/green]")
                                else:
                                    console.print(f"  [yellow]⚠ AI flagged issues ({dynamodb_review.change_count} changes suggested)[/yellow]")
                                
                            except APIKeyNotConfiguredError as e:
                                console.print(e.message)
                                sys.exit(1)
                            except Exception as e:
                                console.print(f"  [yellow]⚠ AI review failed: {e}[/yellow]")
                                console.print(f"  [dim]Continuing with local design only[/dim]")
                    else:
                        console.print("[yellow]⚠ API key not configured, using algorithmic design only[/yellow]")
                        console.print("[dim]  Set ANTHROPIC_API_KEY for AI review[/dim]")
                else:
                    console.print("  [dim]DynamoDB mode: Using algorithmic design (--no-ai)[/dim]")
                    
            elif use_ai:
                settings = get_settings()
                
                # Check if API key is configured
                if not settings.has_api_key():
                    console.print("[yellow]⚠ API key not configured, using rule-based recommendations[/yellow]")
                    console.print("[dim]  Set ANTHROPIC_API_KEY or use --no-ai flag[/dim]")
                    recommendations = analyzer.get_embedding_recommendations(result)
                else:
                    # Compute input hash for cache lookup
                    mode = CacheMode(cache_mode)
                    input_hash = compute_input_hash(schema, result, target_db, mode)
                    
                    # Check cache first (unless --no-cache)
                    if not no_cache:
                        task = progress.add_task("Checking recommendation cache...", total=None)
                        cached_recs = cache.get(input_hash)
                        progress.update(task, completed=True)
                        
                        if cached_recs:
                            recommendations = cached_recs
                            cache_used = True
                            console.print(f"  [green]✓ Using cached recommendations[/green] [dim](hash: {input_hash}, mode: {cache_mode})[/dim]")
                    
                    # If not cached, call Claude API
                    if not recommendations:
                        try:
                            task = progress.add_task("Getting AI recommendations...", total=None)
                            advisor = ClaudeAdvisor()
                            recommendations = advisor.get_recommendations(
                                schema, result, target_db
                            )
                            progress.update(task, completed=True)
                            
                            # Cache the recommendations
                            cache.put(input_hash, recommendations, metadata={
                                "analysis_id": analysis_id,
                                "logs_dir": str(logs_dir),
                                "schema_file": str(schema_file),
                                "cache_mode": cache_mode,
                            })
                            console.print(f"  [dim]Cached recommendations (hash: {input_hash}, mode: {cache_mode})[/dim]")
                            
                        except APIKeyNotConfiguredError as e:
                            console.print(e.message)
                            sys.exit(1)
            else:
                recommendations = analyzer.get_embedding_recommendations(result)

            # Save recommendations
            if recommendations:
                from schema_travels.recommender.models import SchemaRecommendation, RelationshipDecision
                
                def to_schema_rec(r):
                    """Convert various recommendation formats to SchemaRecommendation."""
                    if isinstance(r, SchemaRecommendation):
                        return r
                    elif isinstance(r, dict):
                        # Handle decision - could be string or enum
                        decision = r.get("decision", "reference")
                        if isinstance(decision, str):
                            # Normalize string to enum
                            try:
                                decision = RelationshipDecision(decision.lower())
                            except ValueError:
                                decision = RelationshipDecision.REFERENCE
                        return SchemaRecommendation(
                            parent_table=r.get("parent_table", "") or "",
                            child_table=r.get("child_table", "") or "",
                            decision=decision,
                            confidence=r.get("confidence", 0.5) or 0.5,
                            reasoning=r.get("reasoning", []) or [],
                            warnings=r.get("warnings", []) or [],
                        )
                    else:
                        # Object with attributes
                        decision = r.decision
                        if isinstance(decision, str):
                            try:
                                decision = RelationshipDecision(decision.lower())
                            except ValueError:
                                decision = RelationshipDecision.REFERENCE
                        return SchemaRecommendation(
                            parent_table=r.parent_table or "",
                            child_table=r.child_table or "",
                            decision=decision,
                            confidence=r.confidence if hasattr(r, 'confidence') else 0.5,
                            reasoning=r.reasoning if hasattr(r, 'reasoning') else [],
                            warnings=r.warnings if hasattr(r, 'warnings') else [],
                        )
                
                schema_recs = [to_schema_rec(r) for r in recommendations]
                
                # Filter out invalid recommendations (must have both parent and child tables)
                valid_recs = [
                    r for r in schema_recs 
                    if r.parent_table and r.child_table
                ]
                
                if valid_recs:
                    repo.save_recommendations(analysis_id, valid_recs)
                    if len(valid_recs) < len(schema_recs):
                        console.print(f"  [yellow]Filtered {len(schema_recs) - len(valid_recs)} invalid recommendations[/yellow]")
                else:
                    console.print("  [yellow]No valid recommendations to save[/yellow]")

            # Generate target schema
            task = progress.add_task("Generating target schema...", total=None)
            
            # v2.0.0: Pass DynamoDB-specific options to SchemaGenerator
            # v2.0.1: Also pass AI review for DynamoDB
            if target_db == TargetDatabase.DYNAMODB and mutation_analyzer:
                generator = SchemaGenerator(
                    schema, 
                    result, 
                    valid_recs,
                    dynamodb_mode=dynamo_design_mode,
                    filtered_columns=dict(mutation_analyzer.filtered_columns),
                    selected_columns=dict(mutation_analyzer.selected_columns),
                    select_star_tables=mutation_analyzer.select_star_tables,
                    dynamodb_review=dynamodb_review,  # v2.0.1: AI review
                )
            elif target_db == TargetDatabase.DYNAMODB:
                # DynamoDB without mutation analyzer (shouldn't happen, but handle it)
                generator = SchemaGenerator(
                    schema, 
                    result, 
                    valid_recs,
                    dynamodb_mode=dynamo_design_mode,
                    dynamodb_review=dynamodb_review,
                )
            else:
                generator = SchemaGenerator(schema, result, valid_recs)
            
            target_schema = generator.generate(target_db)
            repo.save_target_schema(analysis_id, target_schema)
            progress.update(task, completed=True)
            
            # v2.0.0: Show DynamoDB design mode in output
            # v2.0.1: Also show AI review status
            if target_db == TargetDatabase.DYNAMODB:
                design_mode = target_schema.metadata.get("design_mode", "unknown")
                confidence = target_schema.metadata.get("confidence", 0)
                dynamodb_design = target_schema.metadata.get("dynamodb_design", {})
                ai_reviewed = dynamodb_design.get("ai_reviewed", False)
                ai_applied = dynamodb_design.get("ai_review_applied", False)
                
                review_status = ""
                if ai_reviewed:
                    if ai_applied:
                        review_status = " [green](AI reviewed + applied)[/green]"
                    else:
                        review_status = " [dim](AI reviewed)[/dim]"
                        
                console.print(f"  DynamoDB design: [bold]{design_mode}[/bold] (confidence: {confidence:.0%}){review_status}")

        # Display results
        _display_analysis_summary(
            result, recommendations, target_schema, 
            cache_used=cache_used,
            min_confidence=min_confidence,
            show_rewrites=show_rewrites,
            target_db=target_db,
        )

        # Save to file if requested
        if output:
            # v2.0.0: Handle DynamoDB output formats
            if target_db == TargetDatabase.DYNAMODB and dynamodb_output:
                dynamo_design = target_schema.metadata.get("dynamodb_design")
                if dynamo_design:
                    # Re-create design object for formatting
                    from schema_travels.recommender.dynamodb_models import DynamoDBDesign
                    
                    # The design is already a dict from to_dict(), format it directly
                    if dynamodb_output == "terraform":
                        # For terraform, we need to reconstruct the design object
                        # or use the dict directly with a custom formatter
                        output_content = _format_dynamodb_terraform(dynamo_design)
                        output_file = output.with_suffix(".tf") if not str(output).endswith(".tf") else output
                        with open(output_file, "w") as f:
                            f.write(output_content)
                        console.print(f"\n[green]Terraform saved to {output_file}[/green]")
                    elif dynamodb_output == "nosql_workbench":
                        output_content = _format_dynamodb_workbench(dynamo_design, analysis_id)
                        output_file = output.with_suffix(".workbench.json") if not str(output).endswith(".workbench.json") else output
                        with open(output_file, "w") as f:
                            f.write(output_content)
                        console.print(f"\n[green]NoSQL Workbench JSON saved to {output_file}[/green]")
                    else:
                        # Default JSON
                        with open(output, "w") as f:
                            json.dump(dynamo_design, f, indent=2)
                        console.print(f"\n[green]DynamoDB design saved to {output}[/green]")
                else:
                    console.print("[yellow]Warning: DynamoDB design not found in metadata[/yellow]")
            else:
                # Standard JSON output
                output_data = {
                    "analysis_id": analysis_id,
                    "cache_used": cache_used,
                    "cache_mode": cache_mode,
                    "analysis": result.to_dict(),
                    "target_schema": target_schema.to_dict(),
                }
                # v2.0.1: Only include recommendations for MongoDB
                # For DynamoDB, the design is in target_schema.metadata.dynamodb_design
                if target_db == TargetDatabase.MONGODB:
                    output_data["recommendations"] = [
                        r.to_dict() if hasattr(r, 'to_dict') else r 
                        for r in recommendations
                    ]
                with open(output, "w") as f:
                    json.dump(output_data, f, indent=2)
                console.print(f"\n[green]Results saved to {output}[/green]")

        console.print(f"\n[bold green]✓ Analysis complete![/bold green]")
        console.print(f"  Analysis ID: {analysis_id}")
        if cache_used:
            console.print(f"  [dim]Used cached recommendations. Run with --no-cache for fresh analysis.[/dim]")
        console.print(f"  View report: schema-travels report --analysis-id {analysis_id}")

    except Exception as e:
        repo.update_analysis_status(analysis_id, "failed")
        console.print(f"[bold red]Error:[/bold red] {e}")
        logger.exception("Analysis failed")
        sys.exit(1)


# =============================================================================
# v2.0.0: DynamoDB Output Formatters (CLI helpers)
# =============================================================================

def _format_dynamodb_terraform(design_dict: dict) -> str:
    """Format DynamoDB design as Terraform HCL."""
    lines = []
    lines.append("# =============================================================================")
    lines.append("# DynamoDB Tables - Generated by schema-travels v2.0.0")
    lines.append(f"# Design Mode: {design_dict.get('design_mode', 'unknown')}")
    lines.append("# =============================================================================")
    lines.append("")
    
    if design_dict.get("design_mode") == "single_table":
        table_name = design_dict.get("table_name", "main_table")
        pk = design_dict.get("partition_key", "PK")
        sk = design_dict.get("sort_key", "SK")
        
        lines.append(f'resource "aws_dynamodb_table" "{_to_resource_name(table_name)}" {{')
        lines.append(f'  name         = "{table_name}"')
        lines.append('  billing_mode = "PAY_PER_REQUEST"')
        lines.append(f'  hash_key     = "{pk}"')
        if sk:
            lines.append(f'  range_key    = "{sk}"')
        lines.append("")
        lines.append(f'  attribute {{')
        lines.append(f'    name = "{pk}"')
        lines.append(f'    type = "S"')
        lines.append('  }')
        if sk:
            lines.append("")
            lines.append(f'  attribute {{')
            lines.append(f'    name = "{sk}"')
            lines.append(f'    type = "S"')
            lines.append('  }')
        
        # GSIs
        for gsi in design_dict.get("gsis", []):
            lines.append("")
            lines.append(f'  attribute {{')
            lines.append(f'    name = "{gsi["pk_attribute"]}"')
            lines.append('    type = "S"')
            lines.append('  }')
            if gsi.get("sk_attribute"):
                lines.append("")
                lines.append(f'  attribute {{')
                lines.append(f'    name = "{gsi["sk_attribute"]}"')
                lines.append('    type = "S"')
                lines.append('  }')
            lines.append("")
            lines.append('  global_secondary_index {')
            lines.append(f'    name            = "{gsi["name"]}"')
            lines.append(f'    hash_key        = "{gsi["pk_attribute"]}"')
            if gsi.get("sk_attribute"):
                lines.append(f'    range_key       = "{gsi["sk_attribute"]}"')
            lines.append(f'    projection_type = "{gsi.get("projection_type", "ALL")}"')
            lines.append('  }')
        
        lines.append("}")
        
        # Entity comments
        if design_dict.get("entities"):
            lines.append("")
            lines.append("# Entity Patterns:")
            for entity in design_dict["entities"]:
                lines.append(f"# - {entity['name']}: PK={entity['pk_pattern']}, SK={entity['sk_pattern']}")
    else:
        # Multi-table
        for table in design_dict.get("tables", []):
            table_name = table.get("table_name", "table")
            pk = table.get("partition_key", "id")
            sk = table.get("sort_key")
            
            lines.append(f'resource "aws_dynamodb_table" "{_to_resource_name(table_name)}" {{')
            lines.append(f'  name         = "{table_name}"')
            lines.append('  billing_mode = "PAY_PER_REQUEST"')
            lines.append(f'  hash_key     = "{pk}"')
            if sk:
                lines.append(f'  range_key    = "{sk}"')
            lines.append("")
            lines.append(f'  attribute {{')
            lines.append(f'    name = "{pk}"')
            lines.append(f'    type = "S"')
            lines.append('  }')
            if sk:
                lines.append("")
                lines.append(f'  attribute {{')
                lines.append(f'    name = "{sk}"')
                lines.append(f'    type = "S"')
                lines.append('  }')
            lines.append("}")
            lines.append("")
    
    return "\n".join(lines)


def _format_dynamodb_workbench(design_dict: dict, model_name: str) -> str:
    """Format DynamoDB design as NoSQL Workbench JSON."""
    model = {
        "ModelName": f"SchemaTravel-{model_name}",
        "ModelMetadata": {
            "Author": "schema-travels",
            "DateCreated": None,
            "DateLastModified": None,
            "Description": f"Generated from SQL analysis. Mode: {design_dict.get('design_mode', 'unknown')}",
            "Version": "2.0.0",
        },
        "DataModel": [],
    }
    
    if design_dict.get("design_mode") == "single_table":
        table_name = design_dict.get("table_name", "MainTable")
        pk = design_dict.get("partition_key", "PK")
        sk = design_dict.get("sort_key", "SK")
        
        table_def = {
            "TableName": table_name,
            "KeyAttributes": {
                "PartitionKey": {
                    "AttributeName": pk,
                    "AttributeType": "S",
                },
            },
            "NonKeyAttributes": [],
            "DataAccess": {"MySql": {}},
        }
        
        if sk:
            table_def["KeyAttributes"]["SortKey"] = {
                "AttributeName": sk,
                "AttributeType": "S",
            }
        
        # GSIs
        if design_dict.get("gsis"):
            table_def["GlobalSecondaryIndexes"] = []
            for gsi in design_dict["gsis"]:
                gsi_def = {
                    "IndexName": gsi["name"],
                    "KeySchema": [
                        {"AttributeName": gsi["pk_attribute"], "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": gsi.get("projection_type", "ALL")},
                }
                if gsi.get("sk_attribute"):
                    gsi_def["KeySchema"].append(
                        {"AttributeName": gsi["sk_attribute"], "KeyType": "RANGE"}
                    )
                table_def["GlobalSecondaryIndexes"].append(gsi_def)
        
        # Facets
        if design_dict.get("entities"):
            table_def["TableFacets"] = []
            for entity in design_dict["entities"]:
                table_def["TableFacets"].append({
                    "FacetName": entity["name"],
                    "KeyAttributeAlias": {
                        "PartitionKeyAlias": entity["pk_pattern"],
                        "SortKeyAlias": entity["sk_pattern"],
                    },
                    "NonKeyAttributes": entity.get("attributes", []),
                })
        
        model["DataModel"].append(table_def)
    else:
        # Multi-table
        for table in design_dict.get("tables", []):
            table_def = {
                "TableName": table.get("table_name", "Table"),
                "KeyAttributes": {
                    "PartitionKey": {
                        "AttributeName": table.get("partition_key", "id"),
                        "AttributeType": "S",
                    },
                },
                "NonKeyAttributes": [],
                "DataAccess": {"MySql": {}},
            }
            if table.get("sort_key"):
                table_def["KeyAttributes"]["SortKey"] = {
                    "AttributeName": table["sort_key"],
                    "AttributeType": "S",
                }
            model["DataModel"].append(table_def)
    
    return json.dumps(model, indent=2)


def _to_resource_name(table_name: str) -> str:
    """Convert table name to valid Terraform resource name."""
    name = table_name.replace("-", "_").replace(".", "_")
    if name[0].isdigit():
        name = "table_" + name
    return name


@cli.command()
@click.option(
    "--analysis-id",
    required=True,
    help="Analysis ID to generate report for",
)
@click.option(
    "--format",
    type=click.Choice(["text", "json", "markdown"]),
    default="text",
    help="Output format",
)
def report(analysis_id: str, format: str) -> None:
    """View analysis report.

    Display detailed report for a previous analysis including
    hot joins, mutation patterns, and recommendations.
    """
    repo = AnalysisRepository()

    analysis = repo.get_analysis(analysis_id)
    if not analysis:
        console.print(f"[red]Analysis not found: {analysis_id}[/red]")
        sys.exit(1)

    result = repo.get_analysis_result(analysis_id)
    recommendations = repo.get_recommendations(analysis_id)
    target_schema = repo.get_target_schema(analysis_id)

    if format == "json":
        output = {
            "analysis": analysis,
            "result": result,
            "recommendations": recommendations,
            "target_schema": target_schema,
        }
        console.print_json(data=output)
    elif format == "markdown":
        _print_markdown_report(analysis, result, recommendations, target_schema)
    else:
        _print_text_report(analysis, result, recommendations, target_schema)


@cli.command()
@click.option(
    "--analysis-id",
    required=True,
    help="Analysis ID to simulate",
)
@click.option(
    "--row-counts",
    type=click.Path(exists=True, path_type=Path),
    help="JSON file with table row counts",
)
def simulate(analysis_id: str, row_counts: Path | None) -> None:
    """Run migration simulation.

    Estimate storage, latency, and cost impact of the migration
    based on analysis results.
    """
    repo = AnalysisRepository()

    analysis = repo.get_analysis(analysis_id)
    if not analysis:
        console.print(f"[red]Analysis not found: {analysis_id}[/red]")
        sys.exit(1)

    # Load data
    result_data = repo.get_analysis_result(analysis_id)
    target_schema_data = repo.get_target_schema(analysis_id)

    if not result_data or not target_schema_data:
        console.print("[red]Analysis result or target schema not found[/red]")
        sys.exit(1)

    # Load row counts if provided
    table_row_counts = None
    if row_counts:
        with open(row_counts) as f:
            table_row_counts = json.load(f)

    # Reconstruct objects (simplified - would need proper deserialization)
    console.print("[yellow]Simulation functionality requires schema reconstruction...[/yellow]")
    console.print("For full simulation, please re-run analysis with --simulate flag.")


@cli.command()
@click.option(
    "--limit",
    default=20,
    help="Maximum number of analyses to show",
)
def history(limit: int) -> None:
    """List past analyses.

    Show a list of all previous analyses with their status and key metrics.
    """
    repo = AnalysisRepository()
    analyses = repo.list_analyses(limit=limit)

    if not analyses:
        console.print("[yellow]No analyses found.[/yellow]")
        console.print("Run 'schema-travels analyze' to create one.")
        return

    table = Table(title="Analysis History")
    table.add_column("ID", style="cyan")
    table.add_column("Created", style="green")
    table.add_column("Source", style="blue")
    table.add_column("Target", style="blue")
    table.add_column("Queries", justify="right")
    table.add_column("Tables", justify="right")
    table.add_column("Status", style="yellow")

    for a in analyses:
        table.add_row(
            a["id"],
            str(a["created_at"])[:19],
            a["source_db_type"],
            a["target_db_type"],
            str(a["total_queries"]),
            str(a["tables_analyzed"]),
            a["status"],
        )

    console.print(table)


@cli.command()
@click.option(
    "--analysis-id",
    required=True,
    help="Analysis ID to delete",
)
@click.confirmation_option(prompt="Are you sure you want to delete this analysis?")
def delete(analysis_id: str) -> None:
    """Delete an analysis.

    Remove an analysis and all associated data from the database.
    """
    repo = AnalysisRepository()

    if repo.delete_analysis(analysis_id):
        console.print(f"[green]Deleted analysis: {analysis_id}[/green]")
    else:
        console.print(f"[red]Analysis not found: {analysis_id}[/red]")
        sys.exit(1)


@cli.command()
def config() -> None:
    """Show current configuration.

    Display the current configuration settings including
    API key status and default values.
    """
    settings = get_settings()

    console.print(Panel.fit(
        "[bold]Schema Travels Configuration[/bold]",
        title="Config",
    ))

    table = Table(show_header=True)
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("API Key", "✓ Configured" if settings.has_api_key() else "✗ Not set")
    table.add_row("Model", settings.anthropic_model)
    table.add_row("Database", str(settings.db_path))
    table.add_row("Cache Dir", str(settings.db_path.parent / "cache"))
    table.add_row("Default Target", settings.default_target)
    table.add_row("Default DB Type", settings.default_db_type)
    table.add_row("Log Level", settings.log_level)

    console.print(table)
    
    # Show cache stats
    cache = get_cache()
    entries = cache.list_entries()
    console.print(f"\n[dim]Cached recommendations: {len(entries)}[/dim]")


@cli.command("clear-cache")
@click.confirmation_option(prompt="Are you sure you want to clear all cached recommendations?")
def clear_cache_cmd() -> None:
    """Clear all cached recommendations.

    Remove all cached AI recommendations. Next analysis will
    fetch fresh recommendations from Claude.
    """
    cache = get_cache()
    count = cache.invalidate_all()
    console.print(f"[green]Cleared {count} cached recommendations[/green]")


def _confidence_color(confidence: float) -> str:
    """Return Rich color markup based on confidence level."""
    if confidence >= 0.85:
        return "green"
    elif confidence >= 0.70:
        return "yellow"
    else:
        return "red"


def _display_analysis_summary(
    result, 
    recommendations, 
    target_schema, 
    cache_used: bool = False,
    min_confidence: float | None = None,
    show_rewrites: bool = False,
    target_db: TargetDatabase = TargetDatabase.MONGODB,
) -> None:
    """Display analysis summary in console."""
    console.print("\n")

    # Hot joins table
    if result.join_patterns:
        table = Table(title="🔥 Hot Joins (Top 10)")
        table.add_column("Tables", style="cyan")
        table.add_column("Frequency", justify="right")
        table.add_column("Avg Time", justify="right")
        table.add_column("Cost Score", justify="right", style="yellow")

        for jp in result.join_patterns[:10]:
            table.add_row(
                f"{jp.left_table} ⟷ {jp.right_table}",
                f"{jp.frequency:,}",
                f"{jp.avg_time_ms:.1f}ms",
                f"{jp.cost_score:,.0f}",
            )

        console.print(table)

    # Mutation patterns
    if result.mutation_patterns:
        console.print("\n")
        table = Table(title="📊 Mutation Patterns")
        table.add_column("Table", style="cyan")
        table.add_column("Reads", justify="right")
        table.add_column("Writes", justify="right")
        table.add_column("Write %", justify="right")
        table.add_column("Type", style="yellow")

        for mp in sorted(result.mutation_patterns, key=lambda m: m.total_operations, reverse=True)[:10]:
            type_label = "📖 Read-heavy" if mp.is_read_heavy else ("✏️ Write-heavy" if mp.is_write_heavy else "⚖️ Mixed")
            table.add_row(
                mp.table,
                f"{mp.select_count:,}",
                f"{mp.total_writes:,}",
                f"{mp.write_ratio:.0%}",
                type_label,
            )

        console.print(table)

    # v2.0.0: DynamoDB-specific output
    if target_db == TargetDatabase.DYNAMODB:
        _display_dynamodb_summary(target_schema)
        return  # v2.0.1: Don't show MongoDB-style recommendations for DynamoDB

    # Recommendations (MongoDB only)
    if recommendations:
        # Convert to consistent format and filter by confidence
        from schema_travels.recommender.models import SchemaRecommendation, RelationshipDecision
        
        filtered_recs = []
        for r in recommendations:
            if isinstance(r, dict):
                parent = r.get("parent_table", "")
                child = r.get("child_table", "")
                decision = r.get("decision", "")
                confidence = r.get("confidence", 0)
                reasoning = r.get("reasoning", [])
            else:
                parent = r.parent_table
                child = r.child_table
                decision = r.decision.value if hasattr(r.decision, 'value') else r.decision
                confidence = r.confidence
                reasoning = r.reasoning
            
            # Apply min_confidence filter
            if min_confidence is not None and confidence < min_confidence:
                continue
                
            filtered_recs.append({
                "parent": parent,
                "child": child,
                "decision": decision,
                "confidence": confidence,
                "reasoning": reasoning,
            })
        
        console.print("\n")
        title = "💡 Schema Recommendations"
        if cache_used:
            title += " [dim](cached)[/dim]"
        if min_confidence is not None:
            title += f" [dim](≥{min_confidence:.0%} confidence)[/dim]"
        table = Table(title=title)
        table.add_column("Relationship", style="cyan")
        table.add_column("Decision", style="green")
        table.add_column("Confidence", justify="right")
        table.add_column("Reasoning")

        for r in filtered_recs[:10]:
            color = _confidence_color(r["confidence"])
            decision_str = r["decision"].upper() if isinstance(r["decision"], str) else str(r["decision"]).upper()
            table.add_row(
                f"{r['parent']} → {r['child']}",
                decision_str,
                f"[{color}]{r['confidence']:.0%}[/{color}]",
                r["reasoning"][0] if r["reasoning"] else "",
            )

        console.print(table)
        
        if min_confidence is not None and len(filtered_recs) < len(recommendations):
            console.print(f"  [dim]({len(recommendations) - len(filtered_recs)} recommendations below {min_confidence:.0%} confidence hidden)[/dim]")
        
        # Show query rewrites if requested (MongoDB only for now)
        if show_rewrites and filtered_recs and target_db == TargetDatabase.MONGODB:
            console.print("\n")
            console.print(Panel.fit(
                "[bold]📝 SQL → MongoDB Query Rewrites[/bold]",
                title="Query Examples",
            ))
            
            # Convert filtered_recs back to SchemaRecommendation for generate_rewrites
            schema_recs_for_rewrite = []
            for r in filtered_recs:
                decision = r["decision"]
                if isinstance(decision, str):
                    try:
                        decision = RelationshipDecision(decision.lower())
                    except ValueError:
                        decision = RelationshipDecision.REFERENCE
                schema_recs_for_rewrite.append(SchemaRecommendation(
                    parent_table=r["parent"],
                    child_table=r["child"],
                    decision=decision,
                    confidence=r["confidence"],
                    reasoning=r["reasoning"],
                    warnings=[],
                ))
            
            rewrite_result = generate_rewrites(schema_recs_for_rewrite)
            
            for example in rewrite_result.examples:
                color = "green" if example.decision == "EMBED" else (
                    "blue" if example.decision == "REFERENCE" else (
                    "yellow" if example.decision == "SEPARATE" else "magenta"
                ))
                console.print(f"\n[bold {color}]━━━ {example.relationship} ({example.decision}) ━━━[/bold {color}]")
                console.print(f"[dim]Scenario:[/dim] {example.scenario}\n")
                console.print("[bold]SQL:[/bold]")
                console.print(Panel(example.sql, border_style="dim"))
                console.print("[bold]MongoDB:[/bold]")
                console.print(Panel(example.mongodb, border_style="dim"))
                console.print(f"[dim]Why:[/dim] {example.explanation}")
            
            if rewrite_result.errors:
                console.print("\n[yellow]Rewrite warnings:[/yellow]")
                for err in rewrite_result.errors:
                    console.print(f"  [dim]• {err}[/dim]")


def _display_dynamodb_summary(target_schema) -> None:
    """Display DynamoDB-specific design summary."""
    console.print("\n")
    
    metadata = target_schema.metadata
    design_mode = metadata.get("design_mode", "unknown")
    confidence = metadata.get("confidence", 0)
    rationale = metadata.get("rationale", "")
    
    # Design mode panel
    mode_color = "green" if design_mode == "single_table" else "blue"
    console.print(Panel.fit(
        f"[bold {mode_color}]DynamoDB Design: {design_mode.upper().replace('_', '-')}[/bold {mode_color}]\n"
        f"Confidence: {confidence:.0%}\n"
        f"[dim]{rationale}[/dim]",
        title="🗄️ DynamoDB Schema",
    ))
    
    # Get full design from metadata
    dynamo_design = metadata.get("dynamodb_design", {})
    
    if design_mode == "single_table":
        # Show entities
        entities = dynamo_design.get("entities", [])
        if entities:
            table = Table(title="Entity Patterns")
            table.add_column("Entity", style="cyan")
            table.add_column("PK Pattern", style="green")
            table.add_column("SK Pattern", style="yellow")
            
            for entity in entities:
                table.add_row(
                    entity.get("name", ""),
                    entity.get("pk_pattern", ""),
                    entity.get("sk_pattern", ""),
                )
            
            console.print(table)
        
        # Show GSIs
        gsis = dynamo_design.get("gsis", [])
        if gsis:
            console.print("\n")
            table = Table(title="Global Secondary Indexes")
            table.add_column("Name", style="cyan")
            table.add_column("PK", style="green")
            table.add_column("SK", style="green")
            table.add_column("Projection", style="yellow")
            
            for gsi in gsis:
                table.add_row(
                    gsi.get("name", ""),
                    gsi.get("pk_attribute", ""),
                    gsi.get("sk_attribute", "-"),
                    gsi.get("projection_type", "ALL"),
                )
            
            console.print(table)
    else:
        # Multi-table: show table list
        tables = dynamo_design.get("tables", [])
        if tables:
            table = Table(title="DynamoDB Tables")
            table.add_column("Table", style="cyan")
            table.add_column("PK", style="green")
            table.add_column("SK", style="green")
            table.add_column("GSIs", justify="right")
            
            for t in tables:
                table.add_row(
                    t.get("table_name", ""),
                    t.get("partition_key", ""),
                    t.get("sort_key", "-"),
                    str(len(t.get("gsis", []))),
                )
            
            console.print(table)
    
    # Show warnings
    warnings = dynamo_design.get("warnings", [])
    if warnings:
        console.print("\n[yellow]⚠ Warnings:[/yellow]")
        for w in warnings:
            console.print(f"  [dim]• {w}[/dim]")


def _print_text_report(analysis, result, recommendations, target_schema) -> None:
    """Print text format report."""
    console.print(Panel.fit(
        f"[bold]Analysis Report[/bold]\n"
        f"ID: {analysis['id']}\n"
        f"Created: {analysis['created_at']}\n"
        f"Status: {analysis['status']}",
        title="Analysis",
    ))

    if result:
        console.print(f"\nQueries analyzed: {len(result.get('join_patterns', []))} join patterns found")

    if recommendations:
        console.print(f"\n[bold]Recommendations ({len(recommendations)}):[/bold]")
        for r in recommendations:
            console.print(f"  • {r['parent_table']} → {r['child_table']}: {r['decision']}")


def _print_markdown_report(analysis, result, recommendations, target_schema) -> None:
    """Print markdown format report."""
    lines = [
        f"# Analysis Report: {analysis['id']}",
        "",
        f"**Created:** {analysis['created_at']}",
        f"**Source:** {analysis['source_db_type']}",
        f"**Target:** {analysis['target_db_type']}",
        f"**Status:** {analysis['status']}",
        "",
    ]

    if recommendations:
        lines.extend([
            "## Recommendations",
            "",
            "| Relationship | Decision | Confidence |",
            "|-------------|----------|------------|",
        ])
        for r in recommendations:
            lines.append(
                f"| {r['parent_table']} → {r['child_table']} | {r['decision']} | {r['confidence']:.0%} |"
            )

    console.print("\n".join(lines))


if __name__ == "__main__":
    cli()
