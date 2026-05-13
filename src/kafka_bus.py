from __future__ import annotations

import json
import logging
import os
import queue
import threading
import uuid
from typing import Any

from src.secrets import VaultError, fetch_secret

logger = logging.getLogger("kafka_bus")

_KAFKA_VAULT_PATH = os.environ.get("VAULT_KAFKA_SECRET_PATH", "secret/data/kafka")
_MESSAGE_QUEUE: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=64)
_consumer_thread: threading.Thread | None = None
_producer: Any = None
_topic: str | None = None
_lock = threading.Lock()


def kafka_enabled() -> bool:
    return os.environ.get("KAFKA_ENABLED", "1").strip().lower() not in ("0", "false", "")


def _kafka_config_from_vault() -> tuple[str, str]:
    data = fetch_secret(_KAFKA_VAULT_PATH)
    bootstrap = (data.get("bootstrap_servers") or "").strip()
    topic = (data.get("topic") or "").strip()
    if not bootstrap or not topic:
        raise VaultError(f"В секрете Kafka не заданы bootstrap_servers/topic: ключи {list(data.keys())}")
    return bootstrap, topic


def start_consumer_background() -> None:
    global _consumer_thread, _topic
    if not kafka_enabled():
        logger.info("Kafka выключена (KAFKA_ENABLED=0).")
        return
    try:
        bootstrap, topic = _kafka_config_from_vault()
    except VaultError as e:
        logger.warning("Kafka consumer не запущен: %s", e)
        return
    _topic = topic
    group_id = f"model-api-{uuid.uuid4()}"

    def _run() -> None:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError

        consumer = None
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap.split(","),
                group_id=group_id,
                enable_auto_commit=True,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            consumer.poll(timeout_ms=15000)
        except KafkaError as e:
            logger.error("Kafka consumer init failed: %s", e)
            return
        logger.info("Kafka consumer слушает topic=%s group=%s", topic, group_id)
        try:
            for msg in consumer:
                if msg.value is None:
                    continue
                try:
                    _MESSAGE_QUEUE.put_nowait(msg.value)
                except queue.Full:
                    try:
                        _MESSAGE_QUEUE.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        _MESSAGE_QUEUE.put_nowait(msg.value)
                    except queue.Full:
                        pass
        except KafkaError as e:
            logger.warning("Kafka consumer loop: %s", e)
        finally:
            if consumer is not None:
                consumer.close()

    _consumer_thread = threading.Thread(target=_run, name="kafka-consumer", daemon=True)
    _consumer_thread.start()


def shutdown_kafka() -> None:
    global _producer
    with _lock:
        if _producer is not None:
            try:
                _producer.flush(timeout=5)
                _producer.close(timeout=5)
            except Exception as e:
                logger.warning("Kafka producer close: %s", e)
            _producer = None


def _get_producer() -> tuple[Any, str] | None:
    global _producer, _topic
    if not kafka_enabled():
        return None
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    with _lock:
        if _producer is not None and _topic is not None:
            return _producer, _topic
        try:
            bootstrap, topic = _kafka_config_from_vault()
        except VaultError as e:
            logger.warning("Kafka producer недоступен: %s", e)
            return None
        try:
            _producer = KafkaProducer(
                bootstrap_servers=bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                linger_ms=5,
            )
        except KafkaError as e:
            logger.warning("Kafka producer init failed: %s", e)
            return None
        _topic = topic
        return _producer, topic


def publish_prediction_result(result: dict[str, Any]) -> None:
    from kafka.errors import KafkaError

    pr = _get_producer()
    if pr is None:
        return
    producer, topic = pr
    try:
        producer.send(topic, value=result)
        producer.flush(timeout=10)
    except KafkaError as e:
        logger.warning("Не удалось отправить результат в Kafka: %s", e)


def drain_message_queue() -> None:
    while True:
        try:
            _MESSAGE_QUEUE.get_nowait()
        except queue.Empty:
            break


def wait_next_message(timeout_sec: float) -> dict[str, Any]:
    return _MESSAGE_QUEUE.get(timeout=timeout_sec)
