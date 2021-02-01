package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.common.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.`with message containing`
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.MetricsReportingException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameResolver
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.PrometheusMetricsCollector
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.DB_COUNT_PROCESSING_TIME
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER
import org.amshove.kluent.*
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.BeforeEach

internal class DbMetricsReporterTest {

    private val metricsReporter = mockk<MetricsReporter>(relaxed = true)
    private val producerNameResolver = mockk<ProducerNameResolver>(relaxed = true)
    private val prometheusCollector = mockkObject(PrometheusMetricsCollector)

    private val nameScrubber = ProducerNameScrubber(producerNameResolver)
    private val dbMetricsReporter = DbMetricsReporter(metricsReporter, nameScrubber)

    @BeforeEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `Should report correct number of events`() {
        val dummyCountResultFromDb = mutableMapOf<String, Int>().apply {
            put("produsent1", 1)
            put("produsent2", 2)
        }

        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"

        val capturedTotalEventsInCacheByProducer = slot<Map<String, Any>>()

        coEvery { metricsReporter.registerDataPoint(DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER, capture(capturedTotalEventsInCacheByProducer), any()) } returns Unit

        val session = DbCountingMetricsSession(EventType.BESKJED)
        session.addEventsByProducer(dummyCountResultFromDb)
        runBlocking {
            dbMetricsReporter.report(session)
        }

        coVerify(exactly = 4) { metricsReporter.registerDataPoint(any(), any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsInCache(3, any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsInCacheByProducer(1, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsInCacheByProducer(2, any(), any()) }
        verify(exactly = 1) { PrometheusMetricsCollector.registerProcessingTimeInCache(any(), any()) }

        capturedTotalEventsInCacheByProducer.captured["counter"] `should be equal to` 1
    }

    @Test
    fun `Should report the elapsed time to count the events`() {
        val expectedProcessingTimeMs = 100L
        val expectedProcessingTimeNs = expectedProcessingTimeMs * 1000000

        val dummyCountResultFromDb = mutableMapOf<String, Int>().apply {
            put("produsent1", 1)
            put("produsent2", 2)
        }

        coEvery { producerNameResolver.getProducerNameAlias(any()) } returns "test-user"

        val capturedProcessingTime = slot<Map<String, Long>>()

        coEvery { metricsReporter.registerDataPoint(DB_COUNT_PROCESSING_TIME, capture(capturedProcessingTime), any()) } returns Unit

        val session = DbCountingMetricsSession(EventType.BESKJED)
        session.addEventsByProducer(dummyCountResultFromDb)
        runBlocking {
            delay(expectedProcessingTimeMs)
        }
        session.calculateProcessingTime()
        runBlocking {
            dbMetricsReporter.report(session)
        }

        capturedProcessingTime.captured["counter"]!!.shouldBeGreaterOrEqualTo(expectedProcessingTimeNs)
        val twentyPercentMoreThanExpectedTime  = (expectedProcessingTimeNs * 1.2).toLong()
        capturedProcessingTime.captured["counter"]!!.shouldBeLessThan(twentyPercentMoreThanExpectedTime)
    }

    @Test
    fun `Should replace system name with alias`() {
        val producerName = "sys-t-user"
        val producerAlias = "test-user"

        coEvery { producerNameResolver.getProducerNameAlias(producerName) } returns producerAlias

        val producerNameForPrometheus = slot<String>()
        val capturedTagsForTotalByProducer = slot<Map<String, String>>()

        coEvery { metricsReporter.registerDataPoint(DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER, any(), capture(capturedTagsForTotalByProducer)) } returns Unit
        every { PrometheusMetricsCollector.registerTotalNumberOfEventsInCacheByProducer(any(), any(), capture(producerNameForPrometheus)) } returns Unit

        val session = DbCountingMetricsSession(EventType.BESKJED)
        session.addEventsByProducer(mapOf(Pair(producerName, 2)))
        runBlocking {
            dbMetricsReporter.report(session)
        }

        coVerify(exactly = 1) { metricsReporter.registerDataPoint(DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER, any(), any()) }

        verify(exactly = 1) { PrometheusMetricsCollector.registerTotalNumberOfEventsInCacheByProducer(any(), any(), any()) }

        producerNameForPrometheus.captured `should be equal to` producerAlias
        capturedTagsForTotalByProducer.captured["producer"] `should be equal to` producerAlias
    }

    @Test
    internal fun `should handle exceptions and rethrow them as internal exceptions`() {
        val simulatedException = Exception("Simulated error in a test")
        coEvery { metricsReporter.registerDataPoint(any(), any(), any()) } throws simulatedException

        invoking {
            runBlocking {
                dbMetricsReporter.report(DbCountingMetricsSessionObjectMother.giveMeBeskjedSessionWithOneCountedEvent())
            }
        } `should throw` MetricsReportingException::class `with message containing` "beskjed"

        invoking {
            runBlocking {
                dbMetricsReporter.report(DbCountingMetricsSessionObjectMother.giveMeDoneSessionWithTwoCountedEvents())
            }
        } `should throw` MetricsReportingException::class `with message containing` "done"

        invoking {
            runBlocking {
                dbMetricsReporter.report(DbCountingMetricsSessionObjectMother.giveMeInnboksSessionWithThreeCountedEvents())
            }
        } `should throw` MetricsReportingException::class `with message containing` "innboks"

        invoking {
            runBlocking {
                dbMetricsReporter.report(DbCountingMetricsSessionObjectMother.giveMeOppgaveSessionWithFourCountedEvents())
            }
        } `should throw` MetricsReportingException::class `with message containing` "oppgave"
    }

}
