package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics

import io.prometheus.client.Gauge
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType

object PrometheusMetricsCollector {

    private const val NAMESPACE = "dittnav_consumer"

    private const val KAFKA_TOPIC_TOTAL_EVENTS = "kafka_topic_total_events"
    private const val KAFKA_TOPIC_TOTAL_EVENTS_BY_PRODUCER = "kafka_topic_total_events_by_producer"
    private const val KAFKA_TOPIC_UNIQUE_EVENTS = "kafka_topic_unique_events"
    private const val KAFKA_TOPIC_UNIQUE_EVENTS_BY_PRODUCER = "kafka_topic_unique_events_by_producer"
    private const val KAFKA_TOPIC_DUPLICATED_EVENTS = "kafka_topic_duplicated_events"
    private const val KAFKA_TOPIC_COUNT_PROCESSING_TIME = "kafka_topic_count_processing_time"
    private const val DB_TOTAL_EVENTS = "db_total_events"
    private const val DB_TOTAL_EVENTS_BY_PRODUCER = "db_total_events_by_producer"
    private const val DB_COUNT_PROCESSING_TIME = "db_count_processing_time"

    private val MESSAGES_UNIQUE: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_UNIQUE_EVENTS)
            .namespace(NAMESPACE)
            .help("Number of unique events of type on topic")
            .labelNames("type")
            .register()

    private val MESSAGES_UNIQUE_BY_PRODUCER: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_UNIQUE_EVENTS_BY_PRODUCER)
            .namespace(NAMESPACE)
            .help("Number of unique events of type on topic grouped by producer")
            .labelNames("type", "producer")
            .register()

    private val MESSAGES_DUPLICATES: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_DUPLICATED_EVENTS)
            .namespace(NAMESPACE)
            .help("Number of duplicated events of type on topic")
            .labelNames("type", "producer")
            .register()

    private val MESSAGES_COUNT_PROCESSING_TIME: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_COUNT_PROCESSING_TIME)
            .namespace(NAMESPACE)
            .help("Time used to count events of this type in nanoseconds")
            .labelNames("type")
            .register()

    private val MESSAGES_TOTAL: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_TOTAL_EVENTS)
            .namespace(NAMESPACE)
            .help("Total number of events of type on topic")
            .labelNames("type")
            .register()

    private val MESSAGES_TOTAL_BY_PRODUCER: Gauge = Gauge.build()
            .name(KAFKA_TOPIC_TOTAL_EVENTS_BY_PRODUCER)
            .namespace(NAMESPACE)
            .help("Total number of events of type on topic grouped by producer")
            .labelNames("type", "producer")
            .register()

    private val CACHED_EVENTS_IN_TOTAL: Gauge = Gauge.build()
            .name(DB_TOTAL_EVENTS)
            .namespace(NAMESPACE)
            .help("Total number of events of type in the cache")
            .labelNames("type")
            .register()

    private val CACHED_EVENTS_IN_TOTAL_BY_PRODUCER: Gauge = Gauge.build()
            .name(DB_TOTAL_EVENTS_BY_PRODUCER)
            .namespace(NAMESPACE)
            .help("Total number of events of type in the cache grouped by producer")
            .labelNames("type", "producer")
            .register()

    private val CACHED_COUNT_PROCESSING_TIME: Gauge = Gauge.build()
        .name(DB_COUNT_PROCESSING_TIME)
        .namespace(NAMESPACE)
        .help("Time used to count events of this type in nanoseconds")
        .labelNames("type")
        .register()

    fun registerUniqueEvents(count: Int, eventType: EventType) {
        MESSAGES_UNIQUE.labels(eventType.eventType).set(count.toDouble())
    }

    fun registerUniqueEventsByProducer(count: Int, eventType: EventType, producer: String) {
        MESSAGES_UNIQUE_BY_PRODUCER.labels(eventType.eventType, producer).set(count.toDouble())
    }

    fun registerDuplicatedEventsOnTopic(count: Int, eventType: EventType, producer: String) {
        MESSAGES_DUPLICATES.labels(eventType.eventType, producer).set(count.toDouble())
    }

    fun registerTotalNumberOfEvents(count: Int, eventType: EventType) {
        MESSAGES_TOTAL.labels(eventType.eventType).set(count.toDouble())
    }

    fun registerTotalNumberOfEventsByProducer(count: Int, eventType: EventType, producer: String) {
        MESSAGES_TOTAL_BY_PRODUCER.labels(eventType.eventType, producer).set(count.toDouble())
    }

    fun registerProcessingTime(processingTime: Long, eventType: EventType) {
        MESSAGES_COUNT_PROCESSING_TIME.labels(eventType.eventType).set(processingTime.toDouble())
    }

    fun registerTotalNumberOfEventsInCache(count: Int, eventType: EventType) {
        CACHED_EVENTS_IN_TOTAL.labels(eventType.eventType).set(count.toDouble())
    }

    fun registerTotalNumberOfEventsInCacheByProducer(count: Int, eventType: EventType, producer: String) {
        CACHED_EVENTS_IN_TOTAL_BY_PRODUCER.labels(eventType.eventType, producer).set(count.toDouble())
    }

    fun registerProcessingTimeInCache(processingTime: Long, eventType: EventType) {
        CACHED_COUNT_PROCESSING_TIME.labels(eventType.eventType).set(processingTime.toDouble())
    }

}
