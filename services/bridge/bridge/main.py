from __future__ import annotations

import logging
import signal
import sys
from pathlib import Path

import httpx

from bridge.checkpoint import Checkpoint
from bridge.config import BridgeConfig
from bridge.producer import EventProducer
from bridge.sse import backoff_seconds, sleep, stream_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("bridge")

_shutdown = False


def _handle_signal(signum, frame) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down after current event", signum)
    _shutdown = True


def touch_heartbeat(path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()


def resolve_resume_id(
    last_event_id: str | None, checkpoint_age_seconds: float | None, max_age_seconds: float
) -> str | None:
    """Decide what Last-Event-ID (if any) to resume from.

    A checkpoint older than max_age_seconds is treated as unusable: resuming
    from it would ask Wikimedia to replay everything since then, which for
    the unfiltered global firehose can mean hours of backlog delivered all
    at once (this is what caused the original CPU/disk incident). Accepting
    a small gap in the data by starting from "now" is the safer trade.
    """
    if last_event_id is None:
        return None
    if checkpoint_age_seconds is not None and checkpoint_age_seconds > max_age_seconds:
        return None
    return last_event_id


def run(config: BridgeConfig) -> None:
    checkpoint = Checkpoint(config.checkpoint_path)
    producer = EventProducer(config.kafka_bootstrap_servers)

    attempt = 0
    with httpx.Client() as client:
        while not _shutdown:
            stored_event_id = checkpoint.read()
            checkpoint_age = checkpoint.age_seconds()
            last_event_id = resolve_resume_id(stored_event_id, checkpoint_age, config.max_checkpoint_age_seconds)
            if stored_event_id is not None and last_event_id is None:
                logger.warning(
                    "checkpoint is %.0fs old (> %.0fs), skipping resume to avoid a large backlog replay",
                    checkpoint_age, config.max_checkpoint_age_seconds,
                )
            logger.info("connecting to %s (resume from %s)", config.stream_url, last_event_id)
            # event.id is buffered here and only fsync'd to disk every 50
            # events (or at connection end, below) -- checkpointing every
            # single event turned out to dominate the container's disk
            # I/O (~3GB written for a few hundred KB of actual state) for
            # no real benefit: a resume replays at most 50 events, already
            # deduped downstream by event_id (staging.stg_events_nrt).
            pending_checkpoint_id: str | None = None
            try:
                events_seen_this_connection = 0
                for event in stream_events(
                    client,
                    config.stream_url,
                    last_event_id=last_event_id,
                    user_agent=config.user_agent,
                    connect_timeout=config.connect_timeout_seconds,
                    read_timeout=config.read_timeout_seconds,
                ):
                    if _shutdown:
                        break
                    if not event.data:
                        continue

                    producer.produce(config.kafka_topic, key=event.id, value=event.data)
                    if event.id:
                        pending_checkpoint_id = event.id

                    events_seen_this_connection += 1
                    if events_seen_this_connection % 50 == 0:
                        touch_heartbeat(config.heartbeat_path)
                        producer.flush(timeout=1.0)
                        if pending_checkpoint_id:
                            checkpoint.write(pending_checkpoint_id)

                    if events_seen_this_connection == 1:
                        attempt = 0  # connection is healthy, reset backoff

            except (httpx.HTTPError, ConnectionError, OSError) as exc:
                logger.warning("stream connection lost: %s", exc)

            producer.flush(timeout=5.0)
            if pending_checkpoint_id:
                checkpoint.write(pending_checkpoint_id)
            touch_heartbeat(config.heartbeat_path)

            if _shutdown:
                break

            delay = backoff_seconds(attempt, base=config.reconnect_base_seconds, cap=config.reconnect_max_seconds)
            logger.info("reconnecting in %.1fs (attempt %d)", delay, attempt)
            sleep(delay)
            attempt += 1

    logger.info("bridge stopped cleanly")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run(BridgeConfig.from_env())


if __name__ == "__main__":
    sys.exit(main())
