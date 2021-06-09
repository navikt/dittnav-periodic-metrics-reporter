package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.common.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.`with message containing`
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.MetricsReportingException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.amshove.kluent.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class TopicMetricsReporterTest {

    private val metricsReporter = mockk<MetricsReporter>(relaxed = true)
    private val producerNameResolver = mockk<ProducerNameResolver>(relaxed = true)
    private val prometheusCollector = mockkObject(PrometheusMetricsCollector)

    private val nameScrubber = ProducerNameScrubber(producerNameResolver)
    private val topicMetricsReporter = TopicMetricsReporter(metricsReporter, nameScrubber)

    @BeforeEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `Should report correct number of events`() {
        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"

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

        val session = TopicMetricsSession(EventType.BESKJED)
        session.countEvent(UniqueKafkaEventIdentifier("1", "producer", "123"))
        session.countEvent(UniqueKafkaEventIdentifier("2", "producer", "123"))
        session.countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
        session.countEvent(UniqueKafkaEventIdentifier("3", "producer", "123"))
        runBlocking {
            topicMetricsReporter.report(session)
        }

        coVerify(exactly = 6) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEvents(3, any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEvents(4, any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEventsByProducer(3, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(1, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(4, any(), any()) }

        capturedFieldsForDuplicated.captured["counter"] `should be equal to` 1
        capturedFieldsForTotalEvents.captured["counter"] `should be equal to` 4
        capturedFieldsForUnique.captured["counter"] `should be equal to` 3
        capturedFieldsForUniqueByProducer.captured["counter"] `should be equal to` 3
        capturedFieldsForTotalEventsByProducer.captured["counter"] `should be equal to` 4
    }

    @Test
    fun `Should report the elapsed time to count the events`() {
        val expectedProcessingTimeMs = 100L
        val expectedProcessingTimeNs = expectedProcessingTimeMs * 1000000

        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"

        val capturedFieldsForProcessingTime = slot<Map<String, Long>>()

        coEvery { metricsReporter.registerDataPoint(KAFKA_COUNT_PROCESSING_TIME, capture(capturedFieldsForProcessingTime), any()) } returns Unit

        val session = TopicMetricsSession(EventType.BESKJED)
        session.countEvent(UniqueKafkaEventIdentifier("1", "sys-t-user", "123"))
        runBlocking { delay(expectedProcessingTimeMs) }
        session.calculateProcessingTime()
        runBlocking {
            topicMetricsReporter.report(session)
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

        val producerNameForPrometheus = slot<String>()
        val capturedTagsForUniqueByProducer = slot<Map<String, String>>()
        val capturedTagsForDuplicates = slot<Map<String, String>>()
        val capturedTagsForTotalByProducer = slot<Map<String, String>>()

        every { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), capture(producerNameForPrometheus)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_DUPLICATE_EVENTS_ON_TOPIC, any(), capture(capturedTagsForDuplicates)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER, any(), capture(capturedTagsForTotalByProducer)) } returns Unit
        coEvery { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER, any(), capture(capturedTagsForUniqueByProducer)) } returns Unit

        val session = TopicMetricsSession(EventType.BESKJED)
        session.countEvent(UniqueKafkaEventIdentifier("1", producerName, "123"))
        session.countEvent(UniqueKafkaEventIdentifier("1", producerName, "123"))
        runBlocking {
            topicMetricsReporter.report(session)
        }

        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER, any(), any()) }
        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_DUPLICATE_EVENTS_ON_TOPIC, any(), any()) }
        coVerify(exactly = 1) { metricsReporter.registerDataPoint(KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER, any(), any()) }

        verify(exactly = 1) { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(any(), any(), any()) }

        producerNameForPrometheus.captured `should be equal to` producerAlias
        capturedTagsForUniqueByProducer.captured["producer"] `should be equal to` producerAlias
        capturedTagsForTotalByProducer.captured["producer"] `should be equal to` producerAlias
        capturedTagsForDuplicates.captured["producer"] `should be equal to` producerAlias
    }

    @Test
    fun `Should not report metrics if current count is zero`() {
        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"

        val sessionWithoutAnyReportedEventsSimulatesACountError = TopicMetricsSession(EventType.BESKJED)
        runBlocking {
            topicMetricsReporter.report(sessionWithoutAnyReportedEventsSimulatesACountError)
        }

        val numberOfSuccessfulCountingSessions = 0
        coVerify(exactly = numberOfSuccessfulCountingSessions) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEvents(any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEvents(any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerUniqueEventsByProducer(any(), any(), any()) }
        verify(exactly = numberOfSuccessfulCountingSessions) { PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(any(), any(), any()) }
    }

    @Test
    internal fun `should handle exceptions and rethrow them as internal exceptions`() {
        val simulatedException = Exception("Simulated error in a test")
        coEvery { metricsReporter.registerDataPoint(any(), any(), any()) } throws simulatedException

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeBeskjedSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "beskjed"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeBeskjedInternSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "beskjed_intern"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeDoneSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "done"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeDoneInternSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "done_intern"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeInnboksSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "innboks"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeInnboksInternSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "innboks_intern"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeOppgaveSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "oppgave"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeOppgaveInternSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "oppgave_intern"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeStatusoppdateringSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "statusoppdatering"

        invoking {
            runBlocking {
                topicMetricsReporter.report(TopicMetricsSessionObjectMother.giveMeStatusoppdateringInternSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "statusoppdatering_intern"
    }

}
