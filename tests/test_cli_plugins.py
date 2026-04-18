"""CLI tests for `et plugins`, `et sinks`, `et sources`."""

from __future__ import annotations


def test_plugins_lists_source_and_sink(cli_env, run_cli):
    result = run_cli("plugins")
    assert result.exit_code == 0, result.output
    assert "Source" in result.output
    assert "Sink" in result.output


def test_sinks_shortcut_includes_fake_bucket(cli_env, run_cli):
    result = run_cli("sinks")
    assert result.exit_code == 0, result.output
    # FakeBucketSink registered by the conftest fixture
    assert "fake_bucket" in result.output
    # LocalSink ships built-in
    assert "local" in result.output


def test_sources_shortcut_includes_direct(cli_env, run_cli):
    result = run_cli("sources")
    assert result.exit_code == 0, result.output
    assert "direct" in result.output
