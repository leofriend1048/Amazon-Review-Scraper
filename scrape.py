#!/usr/bin/env python3
"""
Amazon Review Scraper — 100% local, no paid APIs.

Scrapes tens of thousands of reviews from any ASIN using:
- TLS fingerprint impersonation (curl_cffi)
- Local ML CAPTCHA solving (amazoncaptcha)
- Tor circuit rotation for free IP rotation
- Smart star-splitting to break the 5K pagination wall
- Checkpoint/resume so you never lose progress
- Google cache fallback when Amazon blocks direct access

Usage:
    python scrape.py B08N5WRWNW
    python scrape.py B08N5WRWNW --limit 5000 --sort recent
    python scrape.py B08N5WRWNW --stars 1,2 --sort helpful -o pain_points.csv
    python scrape.py B08N5WRWNW --no-tor --workers 1
    python scrape.py export B08N5WRWNW --format csv -o reviews.csv
    python scrape.py stats B08N5WRWNW
"""

import sys
import signal
import logging
import click
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.text import Text

console = Console()

# ── Logging setup ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_stars(ctx, param, value):
    """Parse comma-separated star ratings."""
    if not value:
        return None
    try:
        stars = [int(s.strip()) for s in value.split(",")]
        for s in stars:
            if s < 1 or s > 5:
                raise click.BadParameter(f"Star rating must be 1-5, got {s}")
        return stars
    except ValueError:
        raise click.BadParameter("Stars must be comma-separated integers (e.g., 1,2,3)")


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Amazon Review Scraper — 100% local, no paid APIs."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.argument("asin")
@click.option("--limit", "-l", type=int, default=None,
              help="Maximum number of reviews to scrape (default: all available)")
@click.option("--sort", "-s", type=click.Choice(["recent", "helpful", "all"]),
              default="all", help="Sort order. 'all' uses both for max coverage.")
@click.option("--stars", callback=parse_stars, default=None,
              help="Filter by star ratings, comma-separated (e.g., 1,2)")
@click.option("--output", "-o", default=None,
              help="Output file (auto-detects format from extension: .csv, .json, .parquet)")
@click.option("--no-tor", is_flag=True, default=False,
              help="Disable Tor — use direct connection only")
@click.option("--workers", "-w", type=int, default=3,
              help="Number of parallel workers (default: 3)")
@click.option("--verbose", "-v", is_flag=True, default=False,
              help="Enable debug logging")
def fetch(asin, limit, sort, stars, output, no_tor, workers, verbose):
    """Scrape reviews for an ASIN."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    from scraper.orchestrator import Orchestrator

    console.print(Panel(
        f"[bold]Amazon Review Scraper[/bold]\n"
        f"ASIN: [cyan]{asin}[/cyan]\n"
        f"Limit: [cyan]{limit or 'all'}[/cyan]  |  "
        f"Sort: [cyan]{sort}[/cyan]  |  "
        f"Stars: [cyan]{stars or 'all'}[/cyan]\n"
        f"Tor: [cyan]{'disabled' if no_tor else 'enabled'}[/cyan]  |  "
        f"Workers: [cyan]{workers}[/cyan]",
        title="Configuration",
    ))

    # Progress tracking
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[bold]{task.completed}/{task.total}"),
        console=console,
    )

    overall_task = None

    def progress_callback(task_key, page, new_reviews, total_reviews):
        nonlocal overall_task
        if overall_task is None:
            return
        progress.update(overall_task, completed=total_reviews,
                       description=f"[cyan]{task_key}[/cyan] p{page} +{new_reviews}")

    orchestrator = Orchestrator(
        asin=asin,
        limit=limit,
        sort=sort,
        stars=stars,
        use_tor=not no_tor,
        workers=workers,
        progress_callback=progress_callback,
    )

    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        console.print("\n[yellow]Stopping gracefully (progress saved)...[/yellow]")
        orchestrator.stop()

    signal.signal(signal.SIGINT, signal_handler)

    # Phase 1: Plan
    console.print("\n[bold]Phase 1:[/bold] Reconnaissance...")
    plan = orchestrator.plan()

    if not plan.tasks:
        console.print("[red]No tasks generated — check ASIN and try again[/red]")
        return

    if plan.review_info:
        info = plan.review_info
        table = Table(title="Review Summary")
        table.add_column("Metric", style="bold")
        table.add_column("Value", style="cyan")
        table.add_row("Total Ratings", f"{info.total_ratings:,}")
        table.add_row("Average Rating", f"{info.average_rating}")
        for s in sorted(info.star_counts.keys(), reverse=True):
            table.add_row(f"  {s}-star", f"~{info.star_counts[s]:,}")
        console.print(table)

    console.print(f"\n[bold]Plan:[/bold] {len(plan.tasks)} tasks")
    for task in plan.tasks:
        star_label = f"{task.star_filter}-star" if task.star_filter else "all stars"
        console.print(f"  - {star_label} × {task.sort_by} (~{task.estimated_reviews:,} reviews)")

    # Phase 2: Execute
    console.print(f"\n[bold]Phase 2:[/bold] Scraping...")
    target = limit or (plan.review_info.total_reviews if plan.review_info else 30000)

    try:
        with progress:
            overall_task = progress.add_task("Starting...", total=target)
            results = orchestrator.execute(plan)
    finally:
        orchestrator.cleanup()

    # Results
    console.print()
    if "error" in results:
        console.print(f"[red]Error: {results['error']}[/red]")
        return

    stats = results.get("stats", {})
    table = Table(title=f"Scrape Complete — {asin}")
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="green")
    table.add_row("Total Reviews", f"{results['total_reviews']:,}")
    table.add_row("New This Run", f"{results['new_reviews']:,}")
    table.add_row("Time", f"{results['elapsed_seconds']:.0f}s ({results['elapsed_seconds']/60:.1f}m)")
    table.add_row("Speed", f"{results['reviews_per_minute']:.0f} reviews/min")
    if stats:
        table.add_row("Avg Rating", f"{stats.get('avg_rating') or 'N/A'}")
        table.add_row("Verified", f"{stats.get('verified') or 0:,}")
        table.add_row("Date Range", f"{stats.get('earliest_date') or '?'} → {stats.get('latest_date') or '?'}")
    console.print(table)

    # Auto-export if output specified
    if output:
        _do_export(asin, output, stars)

    console.print(f"\n[dim]Database saved: data/{asin}.db[/dim]")
    console.print(f"[dim]Export anytime: python scrape.py export {asin} -o reviews.csv[/dim]")


@cli.command()
@click.argument("asin")
@click.option("--format", "-f", "fmt", type=click.Choice(["csv", "json", "parquet"]),
              default="csv", help="Export format")
@click.option("--output", "-o", default=None, help="Output file path")
@click.option("--stars", callback=parse_stars, default=None,
              help="Filter by star ratings, comma-separated")
def export(asin, fmt, output, stars):
    """Export scraped reviews to a file."""
    from scraper.storage import ReviewStorage

    asin = asin.upper().strip()
    storage = ReviewStorage(asin)
    count = storage.get_review_count()

    if count == 0:
        console.print(f"[red]No reviews found for {asin}. Run 'fetch' first.[/red]")
        return

    if not output:
        output = f"{asin}_reviews.{fmt}"

    _do_export(asin, output, stars)
    storage.close()


def _do_export(asin: str, output: str, stars=None):
    """Handle the actual export."""
    from scraper.storage import ReviewStorage

    storage = ReviewStorage(asin)

    if output.endswith(".json"):
        count = storage.export_json(output, stars)
    elif output.endswith(".parquet"):
        count = storage.export_parquet(output, stars)
    else:
        count = storage.export_csv(output, stars)

    storage.close()
    console.print(f"[green]Exported {count:,} reviews → {output}[/green]")


@cli.command()
@click.argument("asin")
def stats(asin):
    """Show statistics for previously scraped reviews."""
    from scraper.storage import ReviewStorage

    asin = asin.upper().strip()
    storage = ReviewStorage(asin)
    count = storage.get_review_count()

    if count == 0:
        console.print(f"[red]No reviews found for {asin}[/red]")
        return

    s = storage.get_stats()
    table = Table(title=f"Review Stats — {asin}")
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="cyan")
    table.add_row("Total Reviews", f"{s['total']:,}")
    table.add_row("Avg Rating", f"{s['avg_rating']}")
    table.add_row("5-star", f"{s['five_star']:,}")
    table.add_row("4-star", f"{s['four_star']:,}")
    table.add_row("3-star", f"{s['three_star']:,}")
    table.add_row("2-star", f"{s['two_star']:,}")
    table.add_row("1-star", f"{s['one_star']:,}")
    table.add_row("Verified Purchase", f"{s['verified']:,}")
    table.add_row("Date Range", f"{s['earliest_date']} → {s['latest_date']}")
    console.print(table)
    storage.close()


@cli.command()
@click.argument("asin")
def resume(asin):
    """Resume a previously interrupted scrape."""
    # Just call fetch — checkpointing handles the resume automatically
    console.print(f"[yellow]Resuming scrape for {asin}...[/yellow]")
    console.print("[dim]Tip: This is the same as running 'fetch' — checkpoints are automatic.[/dim]")


@cli.command(name="google-cache")
@click.argument("asin")
@click.option("--max-pages", type=int, default=50)
@click.option("--output", "-o", default=None)
def google_cache_cmd(asin, max_pages, output):
    """Fallback: scrape reviews from Google's cache instead of Amazon directly."""
    from scraper.google_cache import GoogleCacheScraper
    from scraper.storage import ReviewStorage

    console.print(f"[yellow]Using Google Cache fallback for {asin}[/yellow]")
    console.print("[dim]Reviews may be slightly stale but bypasses Amazon blocks[/dim]\n")

    scraper = GoogleCacheScraper(asin)
    reviews = scraper.scrape_cached_pages(max_pages=max_pages)
    scraper.close()

    if reviews:
        storage = ReviewStorage(asin)
        saved = storage.save_reviews(reviews)
        total = storage.get_review_count()
        console.print(f"[green]Got {len(reviews)} reviews from cache ({saved} new), {total} total in DB[/green]")

        if output:
            _do_export(asin, output)
        storage.close()
    else:
        console.print("[red]No cached pages found[/red]")


@cli.command()
def login():
    """Log into Amazon (opens a browser window). Required for full review access."""
    from scraper.auth import login_interactive, has_saved_session

    if has_saved_session():
        console.print("[yellow]You already have a saved session.[/yellow]")
        console.print("Run [bold]python scrape.py logout[/bold] first to clear it, or continue to re-login.")

    success = login_interactive()
    if success:
        console.print("[green]Login successful! You can now scrape reviews.[/green]")
        console.print("[dim]Session is saved locally — you won't need to log in again until it expires.[/dim]")
    else:
        console.print("[red]Login failed. Please try again.[/red]")


@cli.command()
@click.argument("asin")
@click.option("--format", "-f", "fmt", type=click.Choice(["html", "pdf", "both"]),
              default="html", help="Output format")
@click.option("--output", "-o", default=None, help="Output file path")
@click.option("--model", "-m", default="sonnet", type=click.Choice(["sonnet", "opus"]),
              help="Claude model for analysis (sonnet=faster/cheaper, opus=higher quality)")
def dossier(asin, fmt, output, model):
    """Generate a Creative Intelligence Dossier from scraped reviews."""
    from dossier.analyzer import DossierAnalyzer
    from dossier.renderer import DossierRenderer

    asin = asin.upper().strip()

    # Check reviews exist
    from scraper.storage import ReviewStorage
    storage = ReviewStorage(asin)
    count = storage.get_review_count()
    storage.close()

    if count == 0:
        console.print(f"[red]No reviews found for {asin}. Run 'fetch' first.[/red]")
        return

    console.print(Panel(
        f"[bold]Creative Intelligence Dossier[/bold]\n"
        f"ASIN: [cyan]{asin}[/cyan]  |  Reviews: [cyan]{count:,}[/cyan]\n"
        f"Model: [cyan]{model}[/cyan]  |  Format: [cyan]{fmt}[/cyan]",
        title="Dossier Generation",
    ))

    # Progress tracking
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[bold]{task.completed}%"),
        console=console,
    )

    phases = {
        "Setup": 5, "ML": 25, "AI": 60, "Synthesis": 90, "Done": 100,
    }

    def progress_callback(phase, step, pct):
        base = phases.get(phase, 0)
        progress.update(task_id, completed=base, description=f"[cyan]{phase}:[/cyan] {step}")

    with progress:
        task_id = progress.add_task("Starting...", total=100)

        try:
            analyzer = DossierAnalyzer(
                asin=asin,
                model=model,
                progress_callback=progress_callback,
            )
            results = analyzer.run_full_analysis()
            analyzer.close()
        except Exception as e:
            console.print(f"\n[red]Analysis failed: {e}[/red]")
            return

        progress.update(task_id, completed=95, description="[cyan]Rendering dossier...[/cyan]")

        renderer = DossierRenderer(results, asin)

        if not output:
            output = f"{asin}_dossier"

        files_created = []

        if fmt in ("html", "both"):
            html_path = output if output.endswith(".html") else f"{output}.html"
            renderer.render_html(html_path)
            files_created.append(html_path)

        if fmt in ("pdf", "both"):
            pdf_path = output if output.endswith(".pdf") else f"{output}.pdf"
            try:
                renderer.render_pdf(pdf_path)
                files_created.append(pdf_path)
            except ImportError:
                console.print("[yellow]weasyprint not installed — skipping PDF. Install: pip install weasyprint[/yellow]")

        progress.update(task_id, completed=100, description="[green]Complete![/green]")

    console.print()
    for f in files_created:
        console.print(f"[green]Dossier saved: {f}[/green]")
    console.print(f"\n[dim]Analysis based on {count:,} reviews for {asin}[/dim]")


@cli.command()
def logout():
    """Clear saved Amazon session."""
    from scraper.auth import clear_session
    clear_session()
    console.print("[green]Session cleared.[/green]")


@cli.command()
def status():
    """Check if you have a valid saved Amazon session."""
    from scraper.auth import has_saved_session
    if has_saved_session():
        console.print("[green]Saved Amazon session found.[/green]")
        console.print("You can scrape review pages directly.")
    else:
        console.print("[yellow]No saved session.[/yellow]")
        console.print("Run [bold]python scrape.py login[/bold] to log in.")
        console.print("Without login, only product page reviews are available (limited).")


if __name__ == "__main__":
    cli()
