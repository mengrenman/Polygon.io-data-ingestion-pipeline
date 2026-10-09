"""The `poly` CLI surface: `poly bars` must stay a subcommand (a single-command Typer app would otherwise
collapse into `poly --tf ...`), and the removed `poly actions` must not come back."""
from typer.testing import CliRunner

from polygon_ingest.cli import app

runner = CliRunner()


def test_poly_bars_is_a_subcommand():
    r = runner.invoke(app, ["bars", "--help"])
    assert r.exit_code == 0 and "--layout" in r.output and "--watch" in r.output


def test_replace_month_reaches_the_ingester(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr("polygon_ingest.cli.run_ingest", lambda **kw: seen.update(kw))
    args = ["bars", "--tf", "day", "--src", str(tmp_path), "--out", str(tmp_path / "lake")]
    assert runner.invoke(app, args).exit_code == 0 and seen["replace_month"] is False
    assert runner.invoke(app, args + ["--replace-month"]).exit_code == 0 and seen["replace_month"] is True


def test_root_help_lists_bars_only():
    r = runner.invoke(app, ["--help"])
    assert r.exit_code == 0 and "bars" in r.output and "actions" not in r.output


def test_actions_command_is_gone():
    r = runner.invoke(app, ["actions", "--ticker", "AAPL"])
    assert r.exit_code != 0
