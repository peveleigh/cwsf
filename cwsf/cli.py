import re
import click
import sys
import asyncio
import os
from cwsf import __version__
from cwsf.utils.logging import setup_logging
from cwsf.core.orchestrator import Orchestrator
from cwsf.core.queue import PriorityJobQueue
from cwsf.config.loader import load_config, ConfigParseError
from cwsf.config.validator import validate_config
from typing import List, Dict, Any, Tuple, Optional
from cwsf.utils.run_history import RunHistoryStore, RunResult

@click.group(invoke_without_command=True)
@click.version_option(version=__version__, prog_name="CWSF")
@click.option("--verbose", "-v", is_flag=True, help="Increase log output to DEBUG level.")
@click.option("--quiet", "-q", is_flag=True, help="Suppress all output except errors and final results.")
@click.option("--config-dir", default="./configs", help="Path to the configuration directory.")
@click.pass_context
def main(ctx, verbose, quiet, config_dir):
    """
    Configurable Web Scraping Framework (CWSF)
    
    A configuration-driven framework for web scraping.
    """
    if verbose and quiet:
        click.echo("Error: --verbose and --quiet are mutually exclusive", err=True)
        sys.exit(2)

    # Store global options in context
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["quiet"] = quiet
    ctx.obj["config_dir"] = config_dir

    # Bootstrap logging before any command executes
    log_level = "DEBUG" if verbose else "ERROR" if quiet else "INFO"
    setup_logging(level=log_level)
    
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())

def validate_config_dir(config_dir: str):
    """Validate that the config directory exists and is a directory."""
    if not os.path.exists(config_dir) or not os.path.isdir(config_dir):
        click.echo(f"Error: Config directory '{config_dir}' does not exist or is not a directory.", err=True)
        sys.exit(1)

@main.command()
@click.option("--all", "validate_all", is_flag=True, help="Validate all configuration files in the directory.")
@click.option("--site", help="Validate a specific site configuration.")
@click.pass_context
def validate(ctx, validate_all, site):
    config_dir = ctx.obj["config_dir"]
    validate_config_dir(config_dir)
    """
    Validate configuration file(s) against the schema.
    """
    if not validate_all and not site:
        click.echo("Error: Must specify either --all or --site <name>", err=True)
        sys.exit(2)
    
    if validate_all and site:
        click.echo("Error: --all and --site are mutually exclusive", err=True)
        sys.exit(2)

    if not os.path.exists(config_dir) or not os.path.isdir(config_dir):
        click.echo(f"Error: Config directory '{config_dir}' does not exist or is not a directory.", err=True)
        sys.exit(1)

    files_to_validate = []
    for f in os.listdir(config_dir):
        if f.endswith(".yaml") or f.endswith(".yml"):
            files_to_validate.append(os.path.join(config_dir, f))
    
    if not files_to_validate:
        click.echo(f"No configuration files found in {config_dir}")
        sys.exit(0)

    valid_count = 0
    total_count = 0
    
    # Sort files for consistent output
    files_to_validate.sort()

    for file_path in files_to_validate:
        try:
            config_dict = load_config(file_path)
            
            # If --site is specified, we only care about the matching site_name
            if site and config_dict.get("site_name") != site:
                continue
                
            total_count += 1
            result = validate_config(config_dict, config_file=os.path.basename(file_path))
            
            if result.is_valid:
                valid_count += 1
                click.echo(click.style(f"✓ {config_dict.get('site_name', 'unknown')} ({file_path})", fg="green"))
            else:
                click.echo(click.style(f"✗ {config_dict.get('site_name', 'unknown')} ({file_path})", fg="red"))
                for error in result.errors:
                    click.echo(f"  - Error: {error.field_path}: {error.message}")
                for warning in result.warnings:
                    click.echo(f"  - Warning: {warning.field_path}: {warning.message}")
            
            # If we were looking for a specific site and found it, we can stop
            if site:
                break
                
        except ConfigParseError as e:
            # If --site is specified, we might not know the site_name yet,
            # but we should still report the parse error if we're validating --all
            # or if we suspect this file might be the one (though we can't know without parsing)
            if validate_all:
                total_count += 1
                click.echo(click.style(f"✗ {file_path}", fg="red"))
                click.echo(f"  - Parse Error: {e.message}")
        except Exception as e:
            if validate_all:
                total_count += 1
                click.echo(click.style(f"✗ {file_path}", fg="red"))
                click.echo(f"  - Unexpected Error: {str(e)}")

    if site:
        if total_count == 0:
            click.echo(f"Error: No configuration found for site '{site}'", err=True)
            sys.exit(1)
        if valid_count == 0:
            sys.exit(1)

    if validate_all:
        click.echo(f"\nSummary: {valid_count} of {total_count} configs valid")
        if valid_count < total_count:
            sys.exit(1)
    
    sys.exit(0)

@main.command(name="list")
@click.pass_context
def list_configs(ctx):
    """
    List all discovered configurations and their status.
    """
    config_dir = ctx.obj["config_dir"]
    validate_config_dir(config_dir)

    # Collect all YAML files
    yaml_files = []
    for f in os.listdir(config_dir):
        if f.endswith(".yaml") or f.endswith(".yml"):
            yaml_files.append(os.path.join(config_dir, f))

    if not yaml_files:
        click.echo(f"No configuration files found in {config_dir}")
        sys.exit(0)

    # Build rows: (site_name, file, status, schedule, priority)
    rows: List[Tuple[str, str, str, str, str]] = []

    for file_path in yaml_files:
        file_name = os.path.basename(file_path)
        try:
            config_dict = load_config(file_path)
            result = validate_config(config_dict, config_file=file_name)

            site_name = config_dict.get("site_name", "unknown")

            if result.is_valid:
                status = "valid"
            else:
                status = "invalid"

            # Schedule column
            schedule = config_dict.get("schedule", {})
            if isinstance(schedule, dict) and schedule.get("every"):
                schedule_str = f"every {schedule['every']}"
            else:
                schedule_str = "—"

            # Priority column
            priority_val = config_dict.get("priority")
            if priority_val is not None:
                priority_str = str(priority_val)
            else:
                priority_str = "default"

        except ConfigParseError as e:
            site_name = file_name
            status = "error"
            schedule_str = "—"
            priority_str = "—"
        except Exception as e:
            site_name = file_name
            status = "error"
            schedule_str = "—"
            priority_str = "—"

        rows.append((site_name, file_name, status, schedule_str, priority_str))

    # Sort alphabetically by site name
    rows.sort(key=lambda r: r[0].lower())

    # Determine column widths
    headers = ("Site Name", "File", "Status", "Schedule", "Priority")
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    # Format and print header
    sep = "  "
    header_line = sep.join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    divider = sep.join("-" * col_widths[i] for i in range(len(headers)))
    click.echo(header_line)
    click.echo(divider)

    # Print rows with colour coding for status
    status_colours = {"valid": "green", "invalid": "red", "error": "red"}
    for row in rows:
        site_name, file_name, status, schedule_str, priority_str = row
        colour = status_colours.get(status)
        cells = [
            site_name.ljust(col_widths[0]),
            file_name.ljust(col_widths[1]),
            click.style(status.ljust(col_widths[2]), fg=colour),
            schedule_str.ljust(col_widths[3]),
            priority_str.ljust(col_widths[4]),
        ]
        click.echo(sep.join(cells))

    sys.exit(0)


_URL_RE = re.compile(r'^https?://', re.IGNORECASE)


@main.command()
@click.option("--site", help="Run the scraper for a single specific site.")
@click.option(
    "--base-url",
    "base_url_override",
    default=None,
    help="Override the base_url for this run. Replaces the value in the config file(s).",
)
@click.pass_context
def run(ctx, site, base_url_override):
    """
    Start the framework and process all valid configuration files.
    """
    config_dir = ctx.obj["config_dir"]
    validate_config_dir(config_dir)

    if base_url_override and not _URL_RE.match(base_url_override):
        click.echo(
            f"Error: --base-url '{base_url_override}' does not look like a valid URL.",
            err=True,
        )
        sys.exit(2)

    overrides = {"base_url": base_url_override} if base_url_override else {}

    queue = PriorityJobQueue()
    orchestrator = Orchestrator(queue=queue, config_dir=config_dir, config_overrides=overrides)
    
    try:
        asyncio.run(orchestrator.run(once=True, site_name=site))
        
        # Story 8.2 AC 51: Exit with non-zero if any jobs failed
        if orchestrator.last_run_summary and orchestrator.last_run_summary.sites_failed > 0:
            sys.exit(1)
            
    except Exception as e:
        # Story 8.3 AC 77: Clear error message for missing site
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@main.command()
@click.option("--site", help="Show detailed status for a specific site.")
@click.pass_context
def status(ctx, site):
    """
    Show last run results per site.
    """
    # Note: status doesn't strictly need config_dir for history,
    # but Story 8.8 says it should be available on all sub-commands that interact with configs.
    # We'll validate it anyway for consistency.
    config_dir = ctx.obj["config_dir"]
    validate_config_dir(config_dir)

    store = RunHistoryStore()
    
    if site:
        history = store.get_site_history(site, limit=5)
        if not history:
            click.echo(f"No run history found for site '{site}'.")
            sys.exit(0)
        
        click.echo(f"Status for site: {click.style(site, bold=True)}")
        click.echo("-" * 40)
        for run in history:
            status_color = "green" if run.status == "success" else "yellow" if run.status == "partial" else "red"
            click.echo(f"Run at: {run.timestamp}")
            click.echo(f"Status: {click.style(run.status, fg=status_color)}")
            click.echo(f"Records: {run.records_count}")
            click.echo(f"Errors: {run.error_count}")
            if run.last_error:
                click.echo(f"Last Error: {click.style(run.last_error, fg='red')}")
            click.echo("-" * 20)
    else:
        last_runs = store.get_last_runs()
        if not last_runs:
            click.echo("No run history found. Execute `cwsf run` to begin scraping.")
            sys.exit(0)
            
        headers = ("Site Name", "Last Run", "Records", "Status", "Errors")
        col_widths = [len(h) for h in headers]
        
        # Calculate widths
        for run in last_runs:
            col_widths[0] = max(col_widths[0], len(run.site_name))
            col_widths[1] = max(col_widths[1], len(run.timestamp))
            col_widths[2] = max(col_widths[2], len(str(run.records_count)))
            col_widths[3] = max(col_widths[3], len(run.status))
            col_widths[4] = max(col_widths[4], len(str(run.error_count)))
            
        sep = "  "
        header_line = sep.join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        divider = sep.join("-" * col_widths[i] for i in range(len(headers)))
        click.echo(header_line)
        click.echo(divider)
        
        for run in last_runs:
            status_color = "green" if run.status == "success" else "yellow" if run.status == "partial" else "red"
            cells = [
                run.site_name.ljust(col_widths[0]),
                run.timestamp.ljust(col_widths[1]),
                str(run.records_count).ljust(col_widths[2]),
                click.style(run.status.ljust(col_widths[3]), fg=status_color),
                str(run.error_count).ljust(col_widths[4])
            ]
            click.echo(sep.join(cells))

    sys.exit(0)

def entry_point():
    """Entry point for the CLI that handles top-level exceptions and Ctrl+C."""
    try:
        # Use standalone_mode=False to handle our own exit codes and exceptions
        main(standalone_mode=False)
    except click.UsageError as e:
        e.show()
        sys.exit(2)
    except click.ClickException as e:
        e.show()
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nScraping interrupted. Shutting down gracefully...", err=True)
        sys.exit(1)
    except Exception as e:
        # Story 8.8 AC 207: Unexpected exceptions caught at top level
        # Log full traceback at DEBUG level
        logging.getLogger("cwsf").debug("Unexpected exception", exc_info=True)
        click.echo(f"Error: An unexpected error occurred: {e}", err=True)
        click.echo("Suggestion: Re-run with --verbose for more details.", err=True)
        sys.exit(1)

if __name__ == "__main__":
    entry_point()
