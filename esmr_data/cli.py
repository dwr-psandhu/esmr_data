import sys
import click
import logging
from esmr_data import esmr, dash

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.argument("esmr_csv_file", type=click.Path(exists=True))
def show_dash(esmr_csv_file):
    """
    Show the ESMR Dash application
    """
    logger.info("Reading data from %s", esmr_csv_file)
    df = esmr.read_data_csv(esmr_csv_file)
    logger.info("Creating ESMR data dashboard")
    data = esmr.ESMR(df)
    ui = dash.ESMRDash(data)
    ui.show()


main.add_command(show_dash)
if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
