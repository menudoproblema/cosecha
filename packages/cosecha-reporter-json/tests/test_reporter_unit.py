from __future__ import annotations

import json

from types import SimpleNamespace

import pytest

from cosecha.reporter.json import JsonReporter
from cosecha_internal.testkit import build_config


@pytest.mark.asyncio
async def test_json_reporter_contract_capabilities_and_add_test(
    tmp_path,
) -> None:
    output_path = tmp_path / 'report.json'
    reporter = JsonReporter(output_path)
    reporter.initialize(build_config(tmp_path), SimpleNamespace(name='pytest'))

    assert JsonReporter.reporter_name() == 'json'
    assert JsonReporter.reporter_output_kind() == 'structured'
    capability_names = {
        descriptor.name
        for descriptor in JsonReporter.describe_capabilities()
    }
    assert 'report_lifecycle' in capability_names

    await reporter.start()
    await reporter.add_test(object())
    await reporter.print_report()

    payload = json.loads(output_path.read_text(encoding='utf-8'))
    assert payload['reporter'] == 'json'
    assert payload['summary']['total_tests'] == 0
