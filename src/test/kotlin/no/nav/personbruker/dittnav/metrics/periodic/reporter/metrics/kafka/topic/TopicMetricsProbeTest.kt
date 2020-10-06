package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameResolver
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.PrometheusMetricsCollector
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.amshove.kluent.`should equal`
import org.amshove.kluent.shouldBeGreaterOrEqualTo
import org.amshove.kluent.shouldBeLessThan
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class TopicMetricsProbeTest {

    private val metricsReporter = mockk<MetricsReporter>(relaxed = true)
    private val prometheusCollector = mockkObject(PrometheusMetricsCollector)
    private val producerNameResolver = mockk<ProducerNameResolver>(relaxed = true)

    @BeforeEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `Should report correct number of events`() {
        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"
        val nameScrubber = ProducerNameScrubber(producerNameResolver)
        val metricsProbe = TopicMetricsProbe(metricsReporter, nameScrubber)

        val capturedFieldsForUnique = slot<Map<String, Any>>()
        val capturedFieldsForDuplicated = slot<Map<String, Any>>()
        val capturedFieldsForTotalEvents = slot<Map<String, Any>>()
        val capturedFieldsForUniqueByProducer = slot<Map<String, Any>>()
        val capturedFieldsForTotalEventsByProducer = slot<Map<String, Any>>()

        coEvery { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC, capture(capturedFieldsForUnique), any()) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC, capture(capturedFieldsForTotalEvents), any()) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_DUPLICATE_EVENTS_ON_TOPIC, capture(capturedFieldsForDuplicated), any()) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER, capture(capturedFieldsForTotalEventsByProducer), any()) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER, capture(capturedFieldsForUniqueByProducer), any()) } returns Unit

        runBlocking {
            metricsProbe.runWithMetrics(EventType.BESKJED) {
                countEvent(UniqueKafkaEventIdentifier("1", "producer", "123"))
                countEvent(UniqueKafkaEventIdentifier("2", "producer", "123"))
                countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
                countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
            }
        }

        coVerify(exactly = 6) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEvents(3, any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEvents(4, any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEventsByProducer(3, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(1, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(4, any(), any()) }

        capturedFieldsForDuplicated.captured["counter"] `should equal` 1
        capturedFieldsForTotalEvents.captured["counter"] `should equal` 4
        capturedFieldsForUnique.captured["counter"] `should equal` 3
        capturedFieldsForUniqueByProducer.captured["counter"] `should equal` 3
        capturedFieldsForTotalEventsByProducer.captured["counter"] `should equal` 4
    }

    @Test
    fun `Should report the elapsed time to count the events`() {
        val expectedProcessingTimeMs = 100L
        val expectedProcessingTimeNs = expectedProcessingTimeMs * 1000000

        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"
        val nameScrubber = ProducerNameScrubber(producerNameResolver)
        val metricsProbe = TopicMetricsProbe(metricsReporter, nameScrubber)

        val capturedFieldsForProcessingTime = slot<Map<String, Long>>()

        coEvery { metricsReporter.registerDataPoint(KAFKA_COUNT_PROCESSING_TIME, capture(capturedFieldsForProcessingTime), any()) } returns Unit

        runBlocking {
            metricsProbe.runWithMetrics(EventType.BESKJED) {
                countEvent(UniqueKafkaEventIdentifier("1", "sys-t-user", "123"))
                delay(expectedProcessingTimeMs)
            }
        }

        capturedFieldsForProcessingTime.captured["counter"]!!.shouldBeGreaterOrEqualTo(expectedProcessingTimeNs)
        val fiftyPercentMoreThanExpectedTime  = (expectedProcessingTimeNs * 1.5).toLong()
        capturedFieldsForProcessingTime.captured["counter"]!!.shouldBeLessThan(fiftyPercentMoreThanExpectedTime)
    }

    @Test
    fun `Should replace system name with alias`() {
        val producerName = "sys-t-user"
        val producerAlias = "test-user"

        coEvery { producerNameResolver.getProducerNameAlias(producerName) } returns producerAlias
        val nameScrubber = ProducerNameScrubber(producerNameResolver)
        val metricsProbe = TopicMetricsProbe(metricsReporter, nameScrubber)

        val producerNameForPrometheus = slot<String>()
        val capturedTagsForUniqueByProducer = slot<Map<String, String>>()
        val capturedTagsForDuplicates = slot<Map<String, String>>()
        val capturedTagsForTotalByProducer = slot<Map<String, String>>()

        every { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), capture(producerNameForPrometheus)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_DUPLICATE_EVENTS_ON_TOPIC, any(), capture(capturedTagsForDuplicates)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER, any(), capture(capturedTagsForTotalByProducer)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER, any(), capture(capturedTagsForUniqueByProducer)) } returns Unit

        runBlocking {
            metricsProbe.runWithMetrics(EventType.BESKJED) {
                countEvent(UniqueKafkaEventIdentifier("1", producerName, "123"))
                countEvent(UniqueKafkaEventIdentifier("1", producerName, "123"))
            }
        }

        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER, any(), any()) }
        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_DUPLICATE_EVENTS_ON_TOPIC, any(), any()) }
        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER, any(), any()) }

        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(any(), any(), any()) }

        producerNameForPrometheus.captured `should equal` producerAlias
        capturedTagsForUniqueByProducer.captured["producer"] `should equal` producerAlias
        capturedTagsForTotalByProducer.captured["producer"] `should equal` producerAlias
        capturedTagsForDuplicates.captured["producer"] `should equal` producerAlias
    }

    @Test
    fun `Should not report metrics for count sessions with a lower count than the previous count session`() {
        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"
        val nameScrubber = ProducerNameScrubber(producerNameResolver)
        val metricsProbe = TopicMetricsProbe(metricsReporter, nameScrubber)

        `sesjon som teller tre eventer`(metricsProbe)
        `sesjon som feilaktig teller to eventer`(metricsProbe)
        `sesjon som teller tre eventer`(metricsProbe)

        val numberOfSuccessfulCountingSessions = 2
        coVerify(exactly = 5 * numberOfSuccessfulCountingSessions) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEvents(3, any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEvents(3, any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEventsByProducer(3, any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(3, any(), any()) }
    }

    @Test
    fun `Should not report metrics if current count is zero`() {
        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"
        val nameScrubber = ProducerNameScrubber(producerNameResolver)
        val metricsProbe = TopicMetricsProbe(metricsReporter, nameScrubber)

        runBlocking {
            metricsProbe.runWithMetrics(EventType.BESKJED) {
                // Triggers uten at det blir rapportert noen eventer, for å simulere en feiltelling.
            }
        }

        val numberOfSuccessfulCountingSessions = 0
        coVerify(exactly = numberOfSuccessfulCountingSessions) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEvents(any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEvents(any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(any(), any(), any()) }
    }

    private fun `sesjon som teller tre eventer`(metricsProbe: TopicMetricsProbe) = runBlocking {
        metricsProbe.runWithMetrics(EventType.BESKJED) {
            countEvent(UniqueKafkaEventIdentifier("1", "producer", "123"))
            countEvent(UniqueKafkaEventIdentifier("2", "producer", "123"))
            countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
        }
    }

    private fun `sesjon som feilaktig teller to eventer`(metricsProbe: TopicMetricsProbe) = runBlocking {
        metricsProbe.runWithMetrics(EventType.BESKJED) {
            countEvent(UniqueKafkaEventIdentifier("1", "producer", "123"))
            countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
        }
    }

}
