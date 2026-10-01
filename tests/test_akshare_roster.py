"""Regression tests for complete and fresh AkShare stock rosters."""

from functools import lru_cache
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from quanti.data.akshare_adapter import AkShareAdapter


@pytest.mark.parametrize(("symbol", "stock_type"), [("主板A股", "1"), ("科创板", "8")])
def test_sse_request_preserves_industry(symbol, stock_type):
    response = Mock()
    response.json.return_value = {
        "result": [{
            "A_STOCK_CODE": "688001" if stock_type == "8" else "600519",
            "SEC_NAME_CN": "测试股票", "LIST_DATE": "2019-07-22",
            "CSRC_CODE": "C", "CSRC_CODE_DESC": "制造业",
        }],
        "pageHelp": {"total": 1},
    }
    with patch("quanti.data.akshare_adapter.httpx.get", return_value=response) as get:
        frame = AkShareAdapter._fetch_sh_stock_list(symbol)
    response.raise_for_status.assert_called_once()
    assert get.call_args.kwargs["params"]["STOCK_TYPE"] == stock_type
    assert frame.iloc[0]["所属行业"] == "C 制造业"
    assert frame.iloc[0]["上市日期"] == "2019-07-22"


@pytest.mark.parametrize("payload", [
    {"result": []},
    {"result": [{"A_STOCK_CODE": "600519"}]},
    {"result": [{
        "A_STOCK_CODE": "600519", "SEC_NAME_CN": "测试股票", "LIST_DATE": "2001-08-27",
        "CSRC_CODE": "", "CSRC_CODE_DESC": "",
    }]},
    {"result": [{
        "A_STOCK_CODE": "600519", "SEC_NAME_CN": "测试股票", "LIST_DATE": "2001-08-27",
        "CSRC_CODE": "C", "CSRC_CODE_DESC": "制造业",
    }], "pageHelp": {"total": 2}},
])
def test_sse_invalid_or_truncated_response_is_rejected(payload):
    response = Mock()
    response.json.return_value = payload
    with patch("quanti.data.akshare_adapter.httpx.get", return_value=response):
        with pytest.raises(ValueError):
            AkShareAdapter._fetch_sh_stock_list("主板A股")


def test_later_source_failure_does_not_write_partial_roster(monkeypatch):
    monkeypatch.setattr("quanti.data.akshare_adapter.RETRY_DELAY", 0)
    db = Mock()
    valid = pd.DataFrame([{
        "证券代码": "600519", "证券简称": "贵州茅台",
        "上市日期": "2001-08-27", "所属行业": "C 制造业",
    }])
    with patch.object(AkShareAdapter, "_fetch_sh_stock_list",
                      side_effect=[valid, RuntimeError("failed"),
                                   RuntimeError("failed"), RuntimeError("failed")]):
        with pytest.raises(RuntimeError):
            AkShareAdapter(db).sync_stock_list()
    db.upsert_stock.assert_not_called()


def test_invalid_date_is_rejected_before_writing():
    db = Mock()
    invalid = pd.DataFrame([{
        "证券代码": "600519", "证券简称": "贵州茅台",
        "上市日期": None, "所属行业": "C 制造业",
    }])
    with patch.object(AkShareAdapter, "_fetch_sh_stock_list", return_value=invalid):
        with pytest.raises(ValueError):
            AkShareAdapter(db).sync_stock_list()
    db.upsert_stock.assert_not_called()


def test_cached_roster_is_fetched_again():
    @lru_cache()
    def cached():
        cached.calls += 1
        return pd.DataFrame([{
            "A股代码": "000001", "A股简称": "平安银行",
            "A股上市日期": "1991-04-03", "所属行业": "J 金融业",
        }])
    cached.calls = 0
    sources = (("stock_info_sz_name_code", {}, "A股代码", "A股简称",
                "A股上市日期", None, "SZ", "所属行业"),)
    with patch.object(AkShareAdapter, "_ROSTER_SOURCES", sources):
        with patch("quanti.data.akshare_adapter.ak.stock_info_sz_name_code", cached):
            adapter = AkShareAdapter(Mock())
            assert adapter.sync_stock_list() == 1
            assert adapter.sync_stock_list() == 1
    assert cached.calls == 2
