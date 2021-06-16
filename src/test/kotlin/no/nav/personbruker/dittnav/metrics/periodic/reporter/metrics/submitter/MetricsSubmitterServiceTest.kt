package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import io.mockk.*
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessionsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsSession
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterGCPService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterAivenService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsSession
import org.amshove.kluent.`should contain`
import org.amshove.kluent.`should not contain`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class MetricsSubmitterServiceTest {

    private val dbMetricsReporter: DbMetricsReporter = mockk(relaxed = true)
    private val kafkaMetricsReporter: TopicMetricsReporter = mockk(relaxed = true)
    private val dbEventCounterGCPService: DbEventCounterGCPService = mockk(relaxed = true)
    private val topicEventCounterServiceAiven: TopicEventCounterAivenService<NokkelIntern> = mockk(relaxed = true)


    private val submitter = MetricsSubmitterService(
        dbEventCounterGCPService = dbEventCounterGCPService,
        topicEventCounterServiceAiven = topicEventCounterServiceAiven,
        dbMetricsReporter = dbMetricsReporter,
        kafkaMetricsReporter = kafkaMetricsReporter
    )

    @BeforeEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `Should report metrics for both kafka topics and the database cache`() {
        val topicMetricsInternSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllInternalEventTypes()
        val dbMetricsInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()

        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsInternSessions
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricsInternSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 5) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 5) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics for event types without metrics session`() {
        val topicMetricsInternSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllInternalEventTypesExceptForInnboks()
        val dbMetricInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()

        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsInternSessions
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricInternSessions

        val reportedTopicMetricsForEventTypes = mutableListOf<EventType>()
        val capturedReportedTopicMetrics = slot<TopicMetricsSession>()
        coEvery {
            kafkaMetricsReporter.report(capture(capturedReportedTopicMetrics))
        } answers {
            val currentlyCapturedEventType = capturedReportedTopicMetrics.captured.eventType
            reportedTopicMetricsForEventTypes.add(currentlyCapturedEventType)
        }

        val reportedDbMetricsForEventTypes = mutableListOf<EventType>()
        val capturedReportedDbMetrics = slot<DbCountingMetricsSession>()
        coEvery {
            dbMetricsReporter.report(capture(capturedReportedDbMetrics))
        } answers {
            val currentlyCapturedEventType = capturedReportedDbMetrics.captured.eventType
            reportedDbMetricsForEventTypes.add(currentlyCapturedEventType)
        }

        runBlocking {
            submitter.submitMetrics()
        }

        reportedTopicMetricsForEventTypes `should not contain` EventType.INNBOKS_INTERN
        reportedTopicMetricsForEventTypes `should contain` EventType.BESKJED_INTERN
        reportedTopicMetricsForEventTypes `should contain` EventType.DONE_INTERN
        reportedTopicMetricsForEventTypes `should contain` EventType.OPPGAVE_INTERN

        reportedDbMetricsForEventTypes `should not contain` EventType.INNBOKS_INTERN
        reportedDbMetricsForEventTypes `should contain` EventType.BESKJED_INTERN
        reportedDbMetricsForEventTypes `should contain` EventType.DONE_INTERN
        reportedDbMetricsForEventTypes `should contain` EventType.OPPGAVE_INTERN
    }

    @Test
    fun `Should not report metrics for count sessions with a lower count than the previous count session`() {
        val sessionWithCorrectCount = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllInternalEventTypes()
        val simulatedWrongCount =
            CountingMetricsSessionsObjectMother.giveMeTopicSessionsWithSingleEventForAllInternalEventTypes()
        val dbMetricInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()

        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns sessionWithCorrectCount andThen simulatedWrongCount andThen sessionWithCorrectCount
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricInternSessions

        runBlocking {
            submitter.submitMetrics()
            submitter.submitMetrics()
            submitter.submitMetrics()
        }

        coVerify(exactly = 3) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 5 * 2) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 5 * 2) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should report metrics even if Feilrespons has a lower count than the previous count session`() {
        val sessionWithHighestCount = CountingMetricsSessionsObjectMother.giveMeTopicSessionsWithFiveEventsForFeilrespons()
        val sessionWithLowestCount = CountingMetricsSessionsObjectMother.giveMeTopicSessionsWithSingleEventForFeilrespons()
        val dbMetricInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()
        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns sessionWithHighestCount andThen sessionWithLowestCount
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricInternSessions

        runBlocking {
            submitter.submitMetrics()
            submitter.submitMetrics()
        }

        coVerify(exactly = 2) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 1 * 2) { kafkaMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceAiven)
    }

    @Test
    fun `Should not report metrics if one of the counting fails`() {
        val simulatedException = Exception("Simulated error in a test")
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllExternalEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllExternalEventTypes()
        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } throws simulatedException andThen dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterGCPService.countAllEventTypesAsync() }

        coVerify(exactly = 0) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 0) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }
}
