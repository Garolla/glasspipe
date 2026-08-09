from __future__ import annotations

import logging
import signal
import sys
import time
from pathlib import Path

from glasspipe_common.paths import ParquetPaths
from glasspipe_common.schema import EventValidationError, parse_event, to_flat_record

from landing.config import LandingConfig
from landing.consumer import OffsetTracker, build_consumer
from landing.writer import BufferedParquetWriter, DeadLetterWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("landing")

_shutdown = False


def _handle_signal(signum, frame) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down after current flush", signum)
    _shutdown = True


def run(config: LandingConfig) -> None:
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))
    writer = BufferedParquetWriter(paths)
    dead_letter = DeadLetterWriter(paths)
    offsets = OffsetTracker()

    consumer = build_consumer(config.kafka_bootstrap_servers, config.kafka_group_id)
    consumer.subscribe([config.kafka_topic])

    last_flush = time.monotonic()
    accepted = 0
    rejected = 0

    try:
        while not _shutdown:
            msg = consumer.poll(config.poll_timeout_seconds)
            if msg is not None:
                if msg.error():
                    logger.error("consumer error: %s", msg.error())
                else:
                    raw_line = msg.value().decode("utf-8")
                    try:
                        event = parse_event(raw_line)
                        writer.add(to_flat_record(event, raw_line))
                        accepted += 1
                    except EventValidationError as exc:
                        dead_letter.write(exc.raw_line, str(exc.cause))
                        rejected += 1
                    offsets.record(msg.topic(), msg.partition(), msg.offset())

            should_flush_by_count = writer.buffered_count >= config.flush_max_records
            should_flush_by_time = (time.monotonic() - last_flush) >= config.flush_max_seconds
            has_pending_offsets = bool(offsets.pending_offsets())

            if has_pending_offsets and (should_flush_by_count or should_flush_by_time or _shutdown):
                written_files = writer.flush_all()
                if written_files:
                    logger.info("flushed %d parquet file(s), accepted=%d rejected=%d", len(written_files), accepted, rejected)
                consumer.commit(offsets=offsets.pending_offsets(), asynchronous=False)
                offsets.clear()
                last_flush = time.monotonic()
                accepted = 0
                rejected = 0
    finally:
        consumer.close()

    logger.info("landing stopped cleanly")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run(LandingConfig.from_env())


if __name__ == "__main__":
    sys.exit(main())
