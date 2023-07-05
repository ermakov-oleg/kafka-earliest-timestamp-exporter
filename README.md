# Kafka earliest offset timestamp exporter

This is a simple exporter for Kafka that exports the earliest offset timestamp for each partition of a topic.

Exported metrics are:
* `kafka_topic_first_message_timestamp` - earliest offset timestamp for each partition of a topic

## Usage

```bash
kafka-earliest-timestamp-exporter --config example.json --debug
```

## Connection configuration
Connection configuration is done via a JSON file.
All parameters pass as is to the [librdkafka consumer](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

## State persistence

The exporter can store the last offset timestamp for each partition in compacted Kafka topic.
This is useful when the exporter is restarted, and you want to avoid exporting the same offset timestamp again.

The topic name is `kafka-earliest-timestamp-exporter-state`.
For enabling state persistence, use the `--state-persistence` flag.