import datetime
import json
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep
from typing import Any

import click
import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Gauge
from serpyco_rs import Serializer


_default_consumer_config = {
    'group.id': '__kafka_earliest_timestamp_exporter',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
    'fetch.message.max.bytes': 1024 * 10,  # 10 KB
    'queued.max.messages.kbytes': 1024,  # 1 MB
}

kafka_topic_first_message_timestamp = Gauge(
    'kafka_topic_first_message_timestamp',
    'Timestamp of the first message in the topic (in seconds)',
    ['topic', 'partition'],
)


def collect(config: dict[str, Any], interval: float, state_persistence: bool, debug: bool) -> None:
    offset_state = _PersistentState(config) if state_persistence else _State()
    while True:
        click.echo(f'Collecting metrics ...')
        with create_consumer(config) as consumer:
            _once(consumer, offset_state, debug)
            sleep(interval)


def _once(consumer: Consumer, offset_state: '_State', debug: bool) -> None:
    for topic, metadata in consumer.list_topics().topics.items():
        if topic in offset_state.internal_topics():
            continue

        topic_partitions = []
        topic_offsets = {}
        for partition in metadata.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), cached=True)
            # Skip empty partitions
            if low == high:
                continue

            offset_timestamp = offset_state.get(topic, partition)
            # Skip partitions if low watermark is not changed

            if offset_timestamp and offset_timestamp.offset == low:
                continue

            topic_offsets[partition] = (low, high)
            topic_partitions.append(
                TopicPartition(topic, partition, confluent_kafka.OFFSET_BEGINNING)
            )

        consumer.assign(topic_partitions)
        # librdkafka does not flush paused partitions after unassign
        consumer.resume(topic_partitions)
        if debug and topic_offsets:
            click.echo(f'Waiting for messages for topic {topic} ...')
        while topic_offsets:
            message = consumer.poll(1)
            if message:
                if message.error():
                    click.echo(f'Error while polling message for topic {topic}: {message.error()}', err=True)
                else:
                    partition = message.partition()
                    timestamp = int(message.timestamp()[1] / 1000)
                    offset = message.offset()

                    if partition not in topic_offsets:
                        _stop_listen_partition(consumer, topic, partition)
                        continue

                    low = topic_offsets[partition][0]
                    if offset > low:
                        topic_offsets.pop(partition)
                        _stop_listen_partition(consumer, topic, partition)
                        continue

                    # Store offset and timestamp to cache
                    offset_state.set(topic, partition, _OffsetTimestamp(offset, timestamp))

                    topic_offsets.pop(partition)
                    offset_state.emit_metrics()
                    _stop_listen_partition(consumer, topic, partition)
                    if debug:
                        click.echo(f'{topic}/{partition} -> {datetime.datetime.fromtimestamp(timestamp)} | {offset}')
        consumer.unassign()


def _stop_listen_partition(consumer: Consumer, topic: str, partition: int) -> None:
    # Stop processing messages for this partition and clear partition buffer in librdkafka
    consumer.pause([TopicPartition(topic, partition)])


@contextmanager
def create_consumer(config: dict[str, Any]) -> None:
    click.echo(f'Creating consumer ...')
    consumer = Consumer({**_default_consumer_config, **config})
    try:
        yield consumer
    finally:
        click.echo(f'Closing consumer ...')
        consumer.close()


@dataclass
class _OffsetTimestamp:
    offset: int
    timestamp: int


class _State:
    _cache: dict[tuple[str, int], _OffsetTimestamp]

    def __init__(self) -> None:
        self._cache = {}

    def get(self, topic: str, partition: int) -> _OffsetTimestamp | None:
        return self._cache.get((topic, partition), None)

    def set(self, topic: str, partition: int, offset_timestamp: _OffsetTimestamp) -> None:
        self._cache[(topic, partition)] = offset_timestamp

    def emit_metrics(self) -> None:
        for (topic, partition), offset_timestamp in self._cache.items():
            kafka_topic_first_message_timestamp.labels(topic, partition).set(offset_timestamp.timestamp)

    def internal_topics(self) -> list[str]:
        return []


class _PersistentState(_State):
    _state_topic = 'kafka-earliest-timestamp-exporter-state'

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__()
        self._offset_timestamp_serializer = Serializer(_OffsetTimestamp)
        self._config = config
        self._producer = Producer(
            {
                **config,
            }
        )
        self._load()

    def set(self, topic: str, partition: int, offset_timestamp: _OffsetTimestamp) -> None:
        super().set(topic, partition, offset_timestamp)
        self._publish(topic, partition, offset_timestamp)

    def internal_topics(self) -> list[str]:
        return [self._state_topic]

    def _publish(self, topic: str, partition: int, offset_timestamp: _OffsetTimestamp) -> None:
        key = f'{topic}/{partition}'
        self._producer.produce(
            topic=self._state_topic,
            key=key,
            value=json.dumps(self._offset_timestamp_serializer.dump(offset_timestamp)),
        )
        in_queue = self._producer.flush(1)
        if in_queue > 0:
            click.echo(f'Messages in publish queue: {in_queue}')

    def _create_topic(self) -> None:
        click.echo(f'Creating topic {self._state_topic} for storing offsets and timestamps')
        admin = AdminClient(self._config)
        admin.create_topics(
            [
                NewTopic(
                    topic=self._state_topic,
                    num_partitions=1,
                    config={
                        'cleanup.policy': 'compact',
                        'segment.bytes': 1024 * 1024 * 64,
                    }
                )
            ]
        )[self._state_topic].result()

    def _load(self) -> None:
        with create_consumer(self._config) as consumer:
            topic_metadata = consumer.list_topics(self._state_topic).topics.get(self._state_topic)
            if topic_metadata is None or topic_metadata.error:
                self._create_topic()

            consumer_assignments = []
            current_high_offsets = {}
            in_progress_partitions = set()

            for partition in topic_metadata.partitions:
                _, high = consumer.get_watermark_offsets(TopicPartition(self._state_topic, partition))
                current_high_offsets[partition] = high - 1
                if high > 0:
                    in_progress_partitions.add(partition)
                    consumer_assignments.append(
                        TopicPartition(self._state_topic, partition, confluent_kafka.OFFSET_BEGINNING)
                    )

            if not in_progress_partitions:
                return

            consumer.assign(consumer_assignments)
            click.echo(f'Loading offsets and timestamps from topic {self._state_topic} ...')
            while in_progress_partitions:
                message = consumer.poll(1)
                if message:
                    if message.error():
                        click.echo(
                            f'Error while polling message for topic {self._state_topic}: {message.error()}', err=True
                        )
                    else:
                        key = message.key() if isinstance(message.key(), str) else message.key().decode('utf-8')
                        topic, partition = key.split('/')
                        offset_timestamp = self._offset_timestamp_serializer.load(json.loads(message.value()))
                        self._cache[(topic, int(partition))] = offset_timestamp
                        if message.offset() >= current_high_offsets[message.partition()]:
                            in_progress_partitions.remove(message.partition())

            self.emit_metrics()
