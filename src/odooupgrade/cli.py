import click
import logging
from rich.logging import RichHandler
from .core import OdooUpgrader

# Configure Rich Logging
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)


@click.command()
@click.option(
    "--source",
    required=True,
    help="Path to local .zip/.dump file or URL"
)
@click.option(
    "--version",
    required=True,
    type=click.Choice(OdooUpgrader.VALID_VERSIONS),
    help="Target Odoo version"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging"
)
@click.option(
    "--postgres-version",
    default="13",
    help="PostgreSQL version for the database container (default: 13)"
)
@click.option(
    "--log-file",
    type=click.Path(),
    help="Path to log file"
)
def main(source, version, verbose, postgres_version, log_file):
    """
    Odoo Database Upgrade Tool.

    Automates the upgrade of an Odoo database (zip or dump)
    to a target version using OCA/OpenUpgrade.
    """
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logging.getLogger("odooupgrade").addHandler(file_handler)

    upgrader = OdooUpgrader(
        source=source,
        target_version=version,
        verbose=verbose,
        postgres_version=postgres_version
    )
    upgrader.run()


if __name__ == "__main__":
    main()