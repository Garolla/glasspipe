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


def run(config: BridgeConfig) -> None:
    checkpoint = Checkpoint(config.checkpoint_path)
    producer = EventProducer(config.kafka_bootstrap_servers)

    attempt = 0
    with httpx.Client() as client:
        while not _shutdown:
            last_event_id = checkpoint.read()
            logger.info("connecting to %s (resume from %s)", config.stream_url, last_event_id)
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
                        checkpoint.write(event.id)

                    events_seen_this_connection += 1
                    if events_seen_this_connection % 50 == 0:
                        touch_heartbeat(config.heartbeat_path)
                        producer.flush(timeout=1.0)

                    if events_seen_this_connection == 1:
                        attempt = 0  # connection is healthy, reset backoff

            except (httpx.HTTPError, ConnectionError, OSError) as exc:
                logger.warning("stream connection lost: %s", exc)

            producer.flush(timeout=5.0)
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
