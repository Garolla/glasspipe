from __future__ import annotations

import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class EventProducer:
    """Thin wrapper over confluent_kafka.Producer with delivery-failure logging.

    Delivery reports are checked on the *next* produce() call (librdkafka
    polls internally) and on flush(); a failed delivery is logged, not
    raised, so one bad broker hiccup does not crash the whole bridge loop --
    the event is still safe because it hasn't been checkpointed yet, so a
    reconnect/restart will legitimately be able to resend it.
    """

    def __init__(self, bootstrap_servers: str, *, extra_config: dict | None = None):
        config = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
        }
        if extra_config:
            config.update(extra_config)
        self._producer = Producer(config)

    def produce(self, topic: str, *, key: str | None, value: str) -> None:
        self._producer.produce(
            topic,
            key=key.encode("utf-8") if key is not None else None,
            value=value.encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            logger.error("delivery failed for key=%s: %s", msg.key(), err)
