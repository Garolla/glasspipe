from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import date, timedelta

import httpx

from batch_extract.client import fetch_top_pageviews
from batch_extract.config import BatchExtractConfig
from batch_extract.writer import get_clickhouse_client, insert_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("batch_extract")

_shutdown = False


def _handle_signal(signum, frame) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down after current run", signum)
    _shutdown = True


def run_once(config: BatchExtractConfig, http_client: httpx.Client, ch_client) -> int:
    day = date.today() - timedelta(days=config.lookback_days)
    logger.info("extracting pageviews for %s (%s/%s) on %s", config.project, config.access, day, day)
    rows = fetch_top_pageviews(
        http_client,
        config.api_base_url,
        project=config.project,
        access=config.access,
        day=day,
        user_agent=config.user_agent,
    )
    inserted = insert_rows(ch_client, rows)
    logger.info("inserted %d rows into raw_batch for %s", inserted, day)
    return inserted


def run(config: BatchExtractConfig) -> None:
    ch_client = get_clickhouse_client(config)
    with httpx.Client() as http_client:
        while not _shutdown:
            try:
                run_once(config, http_client, ch_client)
            except Exception:
                logger.exception("batch extract run failed, will retry next interval")

            for _ in range(int(config.poll_interval_hours * 3600)):
                if _shutdown:
                    break
                time.sleep(1)

    logger.info("batch_extract stopped cleanly")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run(BatchExtractConfig.from_env())


if __name__ == "__main__":
    sys.exit(main())
