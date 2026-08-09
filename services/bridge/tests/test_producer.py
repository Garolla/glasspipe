from unittest.mock import patch

from bridge.producer import EventProducer


def test_produce_encodes_key_and_value():
    with patch("bridge.producer.Producer") as MockProducer:
        instance = MockProducer.return_value
        producer = EventProducer("redpanda:9092")
        producer.produce("my-topic", key="abc", value='{"x": 1}')

        instance.produce.assert_called_once()
        _, kwargs = instance.produce.call_args
        assert kwargs["key"] == b"abc"
        assert kwargs["value"] == b'{"x": 1}'
        instance.poll.assert_called_once_with(0)


def test_produce_allows_none_key():
    with patch("bridge.producer.Producer") as MockProducer:
        instance = MockProducer.return_value
        producer = EventProducer("redpanda:9092")
        producer.produce("my-topic", key=None, value="v")

        _, kwargs = instance.produce.call_args
        assert kwargs["key"] is None


def test_flush_delegates_to_underlying_producer():
    with patch("bridge.producer.Producer") as MockProducer:
        instance = MockProducer.return_value
        instance.flush.return_value = 0
        producer = EventProducer("redpanda:9092")
        assert producer.flush(timeout=3.0) == 0
        instance.flush.assert_called_once_with(3.0)
