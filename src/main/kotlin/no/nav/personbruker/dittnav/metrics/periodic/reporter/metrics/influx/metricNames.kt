package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx

private const val METRIC_NAMESPACE = "dittnav.kafka.events.v1"

const val DB_TOTAL_EVENTS_IN_CACHE = "$METRIC_NAMESPACE.db.aggregated.total"
const val DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER = "$METRIC_NAMESPACE.db.producer.total"
const val KAFKA_TOTAL_EVENTS_ON_TOPIC = "$METRIC_NAMESPACE.topic.aggregated.total"
const val KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER = "$METRIC_NAMESPACE.topic.producer.total"
const val KAFKA_UNIQUE_EVENTS_ON_TOPIC = "$METRIC_NAMESPACE.topic.aggregated.unique"
const val KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER = "$METRIC_NAMESPACE.topic.producer.unique"
const val KAFKA_DUPLICATE_EVENTS_ON_TOPIC = "$METRIC_NAMESPACE.topic.duplicates"
