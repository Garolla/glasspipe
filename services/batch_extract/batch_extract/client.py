"""Client for the Wikimedia pageviews "top articles" REST API.

This is deliberately a different kind of data than the NRT recentchange
stream: a daily aggregate, published with roughly a day of latency, pulled
on a schedule rather than pushed continuously. It exists to give the batch
lane something real to extract, not to be reconciled with the NRT lane.

NOTE: this sandbox's network egress policy blocks wikimedia.org, so the
exact response shape below is based on public documentation/examples, not
a live call made from this environment. It should be smoke-tested against
the real endpoint before this service is trusted in production; if the
shape has drifted, `parse_top_pageviews_response` is the one place to fix.
"""

from __future__ import annotations

from datetime import date

import httpx


class PageviewsApiError(Exception):
    pass


def build_top_pageviews_url(base_url: str, project: str, access: str, day: date) -> str:
    return (
        f"{base_url}/metrics/pageviews/top/{project}/{access}"
        f"/{day.year:04d}/{day.month:02d}/{day.day:02d}"
    )


def parse_top_pageviews_response(payload: dict, *, project: str, access: str, day: date) -> list[dict]:
    try:
        items = payload["items"]
    except (KeyError, TypeError) as exc:
        raise PageviewsApiError(f"unexpected response shape: missing 'items' ({payload!r})") from exc

    if not items:
        return []

    articles = items[0].get("articles", [])
    rows = []
    for entry in articles:
        rows.append(
            {
                "date": day,
                "project": project,
                "access": access,
                "article": entry.get("article"),
                "views": entry.get("views"),
                "rank": entry.get("rank"),
            }
        )
    return rows


def fetch_top_pageviews(
    client: httpx.Client,
    base_url: str,
    *,
    project: str,
    access: str,
    day: date,
    user_agent: str,
) -> list[dict]:
    url = build_top_pageviews_url(base_url, project, access, day)
    response = client.get(url, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return parse_top_pageviews_response(response.json(), project=project, access=access, day=day)
