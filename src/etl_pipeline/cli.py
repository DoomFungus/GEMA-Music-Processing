import logging

import click

from etl_pipeline.config import settings
from etl_pipeline.pipeline import run_pipeline


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@click.group()
def cli() -> None:
    configure_logging()


@cli.command()
@click.option("--input", "input_path", required=True, type=click.Path(exists=True), help="Path to source CSV.")
def run(input_path: str) -> None:
    written = run_pipeline(input_path)
    click.echo(f"wrote {written} rows")


if __name__ == "__main__":
    cli()
