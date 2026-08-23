from __future__ import annotations

from typing import Any

import pytest

from streamchart.errors import FetchError, NoDataError
from streamchart.integrations.yahoo import fetch_intraday, parse_chart


def _payload(timestamps: list[int], **series: list[Any]) -> dict[str, Any]:
    return {
        "chart": {
            "error": None,
            "result": [
                {
                    "timestamp": timestamps,
                    "indicators": {"quote": [series]},
                }
            ],
        }
    }


def test_parse_chart_ok() -> None:
    payload = _payload(
        [1_755_785_400, 1_755_785_460],
        open=[100.0, 101.0],
        high=[100.5, 101.5],
        low=[99.5, 100.5],
        close=[100.2, 101.2],
        volume=[1000, 2000],
    )
    bars = parse_chart(payload, ticker="msft", interval="1m")
    assert len(bars) == 2
    assert bars[0].ticker == "MSFT"
    assert bars[0].open == 100.0
    assert bars[1].volume == 2000


def test_parse_chart_skips_null_rows() -> None:
    payload = _payload(
        [1, 2, 3],
        open=[100.0, None, 102.0],
        high=[100.5, None, 102.5],
        low=[99.5, None, 101.5],
        close=[100.2, None, 102.2],
        volume=[1000, None, 3000],
    )
    bars = parse_chart(payload, ticker="MSFT", interval="1m")
    assert len(bars) == 2


def test_parse_chart_empty_result_raises() -> None:
    with pytest.raises(NoDataError):
        parse_chart({"chart": {"error": None, "result": []}}, ticker="MSFT", interval="1m")


def test_parse_chart_error_node_raises() -> None:
    payload = {"chart": {"error": {"code": "Not Found"}, "result": None}}
    with pytest.raises(FetchError):
        parse_chart(payload, ticker="MSFT", interval="1m")


def test_fetch_intraday_http() -> None:
    respx = pytest.importorskip("respx")
    import httpx

    payload = _payload(
        [1_755_785_400],
        open=[100.0],
        high=[100.5],
        low=[99.5],
        close=[100.2],
        volume=[1000],
    )
    with respx.mock(assert_all_called=True) as mock:
        mock.get(url__regex=r".*/MSFT").mock(return_value=httpx.Response(200, json=payload))
        bars = fetch_intraday("MSFT", "1m", "1d")
    assert len(bars) == 1
    assert bars[0].close == 100.2
