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
from schema_travels.config import get_settings
from schema_travels.collector import PostgresLogParser, MySQLLogParser, SchemaParser
from schema_travels.analyzer import PatternAnalyzer
from schema_travels.recommender import ClaudeAdvisor, SchemaGenerator
from schema_travels.recommender.models import TargetDatabase
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
def analyze(
    logs_dir: Path,
    schema_file: Path,
    db_type: str,
    target: str,
    output: Path | None,
    use_ai: bool,
) -> None:
    """Analyze database access patterns and generate recommendations.

    Parses query logs and schema to identify hot joins, mutation patterns,
    and co-access patterns. Generates recommendations for NoSQL schema design.
    """
    analysis_id = str(uuid.uuid4())[:8]
    target_db = TargetDatabase(target)

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

            # Save analysis result
            repo.save_analysis_result(result)

            # Get recommendations
            recommendations = []
            if use_ai:
                settings = get_settings()
                if settings.validate_api_key():
                    task = progress.add_task("Getting AI recommendations...", total=None)
                    advisor = ClaudeAdvisor()
                    recommendations = advisor.get_recommendations(
                        schema, result, target_db
                    )
                    progress.update(task, completed=True)
                else:
                    console.print("[yellow]⚠ API key not configured, using rule-based recommendations[/yellow]")
                    recommendations = analyzer.get_embedding_recommendations(result)
            else:
                recommendations = analyzer.get_embedding_recommendations(result)

            # Save recommendations
            if recommendations:
                from schema_travels.recommender.models import SchemaRecommendation, RelationshipDecision
                schema_recs = [
                    SchemaRecommendation(
                        parent_table=r.get("parent_table", r["parent_table"]) if isinstance(r, dict) else r.parent_table,
                        child_table=r.get("child_table", r["child_table"]) if isinstance(r, dict) else r.child_table,
                        decision=RelationshipDecision(r.get("decision", "reference")) if isinstance(r, dict) else r.decision,
                        confidence=r.get("confidence", 0.5) if isinstance(r, dict) else r.confidence,
                        reasoning=r.get("reasoning", []) if isinstance(r, dict) else r.reasoning,
                        warnings=r.get("warnings", []) if isinstance(r, dict) else r.warnings,
                    )
                    for r in recommendations
                ]
                repo.save_recommendations(analysis_id, schema_recs)

            # Generate target schema
            task = progress.add_task("Generating target schema...", total=None)
            generator = SchemaGenerator(schema, result, schema_recs if recommendations else [])
            target_schema = generator.generate(target_db)
            repo.save_target_schema(analysis_id, target_schema)
            progress.update(task, completed=True)

        # Display results
        _display_analysis_summary(result, recommendations, target_schema)

        # Save to file if requested
        if output:
            output_data = {
                "analysis_id": analysis_id,
                "analysis": result.to_dict(),
                "recommendations": [r.to_dict() if hasattr(r, 'to_dict') else r for r in recommendations],
                "target_schema": target_schema.to_dict(),
            }
            with open(output, "w") as f:
                json.dump(output_data, f, indent=2)
            console.print(f"\n[green]Results saved to {output}[/green]")

        console.print(f"\n[bold green]✓ Analysis complete![/bold green]")
        console.print(f"  Analysis ID: {analysis_id}")
        console.print(f"  View report: schema-travels report --analysis-id {analysis_id}")

    except Exception as e:
        repo.update_analysis_status(analysis_id, "failed")
        console.print(f"[bold red]Error:[/bold red] {e}")
        logger.exception("Analysis failed")
        sys.exit(1)


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

    table.add_row("API Key", "✓ Configured" if settings.validate_api_key() else "✗ Not set")
    table.add_row("Model", settings.anthropic_model)
    table.add_row("Database", str(settings.db_path))
    table.add_row("Default Target", settings.default_target)
    table.add_row("Default DB Type", settings.default_db_type)
    table.add_row("Log Level", settings.log_level)

    console.print(table)


def _display_analysis_summary(result, recommendations, target_schema) -> None:
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

    # Recommendations
    if recommendations:
        console.print("\n")
        table = Table(title="💡 Schema Recommendations")
        table.add_column("Relationship", style="cyan")
        table.add_column("Decision", style="green")
        table.add_column("Confidence", justify="right")
        table.add_column("Reasoning")

        for r in recommendations[:10]:
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

            table.add_row(
                f"{parent} → {child}",
                decision.upper(),
                f"{confidence:.0%}",
                reasoning[0] if reasoning else "",
            )

        console.print(table)


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
