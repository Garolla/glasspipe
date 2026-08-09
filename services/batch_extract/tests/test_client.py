from datetime import date

import httpx
import pytest

from batch_extract.client import (
    PageviewsApiError,
    build_top_pageviews_url,
    fetch_top_pageviews,
    parse_top_pageviews_response,
)

SAMPLE_RESPONSE = {
    "items": [
        {
            "project": "en.wikipedia",
            "access": "all-access",
            "year": "2026",
            "month": "08",
            "day": "01",
            "articles": [
                {"article": "Main_Page", "views": 500000, "rank": 1},
                {"article": "Data_engineering", "views": 12345, "rank": 2},
            ],
        }
    ]
}


def test_build_top_pageviews_url_pads_month_and_day():
    url = build_top_pageviews_url("https://wikimedia.org/api/rest_v1", "en.wikipedia", "all-access", date(2026, 8, 1))
    assert url == "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/2026/08/01"


def test_parse_top_pageviews_response_extracts_rows():
    rows = parse_top_pageviews_response(
        SAMPLE_RESPONSE, project="en.wikipedia", access="all-access", day=date(2026, 8, 1)
    )
    assert len(rows) == 2
    assert rows[0] == {
        "date": date(2026, 8, 1),
        "project": "en.wikipedia",
        "access": "all-access",
        "article": "Main_Page",
        "views": 500000,
        "rank": 1,
    }


def test_parse_top_pageviews_response_empty_items():
    rows = parse_top_pageviews_response({"items": []}, project="p", access="a", day=date(2026, 8, 1))
    assert rows == []


def test_parse_top_pageviews_response_missing_items_raises():
    with pytest.raises(PageviewsApiError):
        parse_top_pageviews_response({"unexpected": True}, project="p", access="a", day=date(2026, 8, 1))


def test_fetch_top_pageviews_calls_api_and_parses(httpx_mock):
    httpx_mock.add_response(
        url="https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/2026/08/01",
        json=SAMPLE_RESPONSE,
    )
    with httpx.Client() as client:
        rows = fetch_top_pageviews(
            client,
            "https://wikimedia.org/api/rest_v1",
            project="en.wikipedia",
            access="all-access",
            day=date(2026, 8, 1),
            user_agent="test-agent",
        )
    assert len(rows) == 2

    request = httpx_mock.get_requests()[0]
    assert request.headers["user-agent"] == "test-agent"
