"""The `poly` CLI surface: `poly bars` must stay a subcommand (a single-command Typer app would otherwise
collapse into `poly --tf ...`), and the removed `poly actions` must not come back."""
from typer.testing import CliRunner

from polygon_ingest.cli import app

runner = CliRunner()


def test_poly_bars_is_a_subcommand():
    r = runner.invoke(app, ["bars", "--help"])
    assert r.exit_code == 0 and "--layout" in r.output and "--watch" in r.output


def test_root_help_lists_bars_only():
    r = runner.invoke(app, ["--help"])
    assert r.exit_code == 0 and "bars" in r.output and "actions" not in r.output


def test_actions_command_is_gone():
    r = runner.invoke(app, ["actions", "--ticker", "AAPL"])
    assert r.exit_code != 0
