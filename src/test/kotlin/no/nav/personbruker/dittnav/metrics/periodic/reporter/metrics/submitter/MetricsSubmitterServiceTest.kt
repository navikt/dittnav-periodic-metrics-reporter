package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import io.mockk.*
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessionsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.DbEventCounterGCPService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsSession
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterOnPremService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterAivenService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterOnPremService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsSession
import org.amshove.kluent.`should contain`
import org.amshove.kluent.`should not contain`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

internal class MetricsSubmitterServiceTest {

    private val dbMetricsReporter: DbMetricsReporter = mockk(relaxed = true)
    private val kafkaMetricsReporter: TopicMetricsReporter = mockk(relaxed = true)
    private val dbEventCounterOnPremService: DbEventCounterOnPremService = mockk(relaxed = true)
    private val dbEventCounterGCPService: DbEventCounterGCPService = mockk(relaxed = true)
    private val topicEventCounterServiceOnPrem: TopicEventCounterOnPremService<Nokkel> = mockk(relaxed = true)
    private val topicEventCounterServiceAiven: TopicEventCounterAivenService<NokkelIntern> = mockk(relaxed = true)


    private val submitter = MetricsSubmitterService(
        dbEventCounterOnPremService = dbEventCounterOnPremService,
        dbEventCounterGCPService = dbEventCounterGCPService,
        topicEventCounterServiceOnPrem = topicEventCounterServiceOnPrem,
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
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        val dbMetricsInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()

        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterOnPremService.countAllEventTypesAsync() } returns dbMetricsSessions
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricsInternSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 1) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterOnPremService.countAllEventTypesAsync() }

        coVerify(exactly = 4) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 4) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(dbEventCounterOnPremService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics for event types without metrics session`() {
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypesExceptForInnboks()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypesExceptForInnboks()
        val dbMetricInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()

        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterOnPremService.countAllEventTypesAsync() } returns dbMetricsSessions
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

        reportedTopicMetricsForEventTypes `should not contain` EventType.INNBOKS
        reportedTopicMetricsForEventTypes `should contain` EventType.BESKJED
        reportedTopicMetricsForEventTypes `should contain` EventType.DONE
        reportedTopicMetricsForEventTypes `should contain` EventType.OPPGAVE

        reportedDbMetricsForEventTypes `should not contain` EventType.INNBOKS
        reportedDbMetricsForEventTypes `should contain` EventType.BESKJED
        reportedDbMetricsForEventTypes `should contain` EventType.DONE
        reportedDbMetricsForEventTypes `should contain` EventType.OPPGAVE
    }

    @Test
    fun `Should not report metrics for count sessions with a lower count than the previous count session`() {
        val sessionWithCorrectCount = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val simulatedWrongCount =
            CountingMetricsSessionsObjectMother.giveMeTopicSessionsWithSingleEventForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        val dbMetricInternSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllInternalEventTypes()
        coEvery {
            topicEventCounterServiceOnPrem.countAllEventTypesAsync()
        } returns sessionWithCorrectCount andThen simulatedWrongCount andThen sessionWithCorrectCount

        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns sessionWithCorrectCount andThen simulatedWrongCount andThen sessionWithCorrectCount
        coEvery { dbEventCounterOnPremService.countAllEventTypesAsync() } returns dbMetricsSessions
        coEvery { dbEventCounterGCPService.countAllEventTypesAsync() } returns dbMetricInternSessions

        runBlocking {
            submitter.submitMetrics()
            submitter.submitMetrics()
            submitter.submitMetrics()
        }

        coVerify(exactly = 3) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 3) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 3) { dbEventCounterOnPremService.countAllEventTypesAsync() }

        coVerify(exactly = 4 * 2) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 4 * 2) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(dbEventCounterOnPremService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics if one of the counting fails`() {
        val simulatedException = Exception("Simulated error in a test")
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceAiven.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterOnPremService.countAllEventTypesAsync() } throws simulatedException andThen dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 1) { topicEventCounterServiceAiven.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterOnPremService.countAllEventTypesAsync() }

        coVerify(exactly = 0) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 0) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(topicEventCounterServiceAiven)
        confirmVerified(dbEventCounterOnPremService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }
}
