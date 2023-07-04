# Kafka earliest offset timestamp exporter

This is a simple exporter for Kafka that exports the earliest offset timestamp for each partition of a topic.


## Usage

```bash
kafka-earliest-timestamp-exporter --config example.json --debug
```

## Connection configuration
Connection configuration is done via a JSON file.
All parameters pass as is to the [librdkafka consumer](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

