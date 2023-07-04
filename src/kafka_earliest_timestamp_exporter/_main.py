import datetime
import logging
from dataclasses import dataclass
from time import sleep
from typing import Any

import click
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge


logger = logging.getLogger(__name__)


_default_config = {
    'group.id': '__kafka_earliest_timestamp_exporter',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
}

kafka_topic_first_message_timestamp = Gauge(
    'kafka_topic_first_message_timestamp',
    'Timestamp of the first message in the topic',
    ['topic', 'partition'],
)


def collect(config: dict[str, Any], interval: float, debug: bool) -> None:
    consumer = Consumer(
        **_default_config,
        **config,
    )
    while True:
        _once(consumer, debug)
        sleep(interval)


def _once(consumer: Consumer, debug: bool) -> None:
    for topic, metadata in consumer.list_topics().topics.items():

        topic_partitions = []
        for partition in metadata.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition))
            # Skip empty partitions
            if low == high:
                continue

            offset_timestamp = offset_cache.get(topic, partition)
            # Skip partitions if low watermark is not changed
            if offset_timestamp and offset_timestamp.offset == low:
                continue

            topic_partitions.append(
                TopicPartition(topic, partition, confluent_kafka.OFFSET_BEGINNING)
            )

        consumer.assign(topic_partitions)
        if debug:
            click.echo(f'Waiting for messages for topic {topic} ...')
        while topic_partitions:
            messages = consumer.poll(1)
            if messages:
                if messages.error():
                    click.echo(f'Error while polling messages for topic {topic}: {messages.error()}', err=True)
                else:
                    partition = messages.partition()
                    timestamp = int(messages.timestamp()[1] / 1000)
                    # Store offset and timestamp to cache
                    offset_cache.set(topic, partition, _OffsetTimestamp(messages.offset(), timestamp))

                    # Remove partition from list of partitions to poll
                    topic_partitions = [
                        tp
                        for tp in topic_partitions
                        if not (tp.partition == partition and tp.topic == topic)
                    ]
                    # Assign remaining partitions
                    consumer.assign(topic_partitions)
                    kafka_topic_first_message_timestamp.labels(topic, partition).set(timestamp)
                    if debug:
                        click.echo(f'{topic}/{partition} -> {datetime.datetime.fromtimestamp(timestamp)}')


@dataclass
class _OffsetTimestamp:
    offset: int
    timestamp: int


class _Cache:
    _cache: dict[tuple[str, int], _OffsetTimestamp]

    def __init__(self):
        self._cache = {}

    def get(self, topic: str, partition: int) -> _OffsetTimestamp | None:
        return self._cache.get((topic, partition), None)

    def set(self, topic: str, partition: int, offset_timestamp: _OffsetTimestamp):
        self._cache[(topic, partition)] = offset_timestamp


offset_cache = _Cache()
