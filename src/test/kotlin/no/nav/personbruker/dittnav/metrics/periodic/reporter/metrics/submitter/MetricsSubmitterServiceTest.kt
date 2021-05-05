package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import io.mockk.*
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessionsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsSession
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsSession
import org.amshove.kluent.`should contain`
import org.amshove.kluent.`should not contain`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class MetricsSubmitterServiceTest {

    private val dbMetricsReporter: DbMetricsReporter = mockk(relaxed = true)
    private val kafkaMetricsReporter: TopicMetricsReporter = mockk(relaxed = true)
    private val dbEventCounterServiceOnPrem: DbEventCounterService = mockk(relaxed = true)
    private val dbEventCounterServiceGCP: DbEventCounterService = mockk(relaxed = true)
    private val topicEventCounterServiceOnPrem: TopicEventCounterService = mockk(relaxed = true)
    private val topicEventCounterServiceGCP: TopicEventCounterService = mockk(relaxed = true)


    private val submitter = MetricsSubmitterService(
        dbEventCounterServiceOnPrem = dbEventCounterServiceOnPrem,
        dbEventCounterServiceGCP = dbEventCounterServiceGCP,
        topicEventCounterServiceOnPrem = topicEventCounterServiceOnPrem,
        topicEventCounterServiceGCP = topicEventCounterServiceGCP,
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
        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceGCP.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterServiceOnPrem.countAllEventTypesAsync() } returns dbMetricsSessions
        coEvery { dbEventCounterServiceGCP.countAllEventTypesAsync() } returns dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterServiceOnPrem.countAllEventTypesAsync() }

        coVerify(exactly = 8) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 8) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(dbEventCounterServiceOnPrem)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics for event types without metrics session`() {
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypesExceptForInnboks()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypesExceptForInnboks()
        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceGCP.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterServiceOnPrem.countAllEventTypesAsync() } returns dbMetricsSessions
        coEvery { dbEventCounterServiceGCP.countAllEventTypesAsync() } returns dbMetricsSessions

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
        coEvery {
            topicEventCounterServiceOnPrem.countAllEventTypesAsync()
        } returns sessionWithCorrectCount andThen simulatedWrongCount andThen sessionWithCorrectCount

        coEvery { topicEventCounterServiceGCP.countAllEventTypesAsync() } returns sessionWithCorrectCount andThen simulatedWrongCount andThen sessionWithCorrectCount

        coEvery { dbEventCounterServiceOnPrem.countAllEventTypesAsync() } returns dbMetricsSessions

        coEvery { dbEventCounterServiceGCP.countAllEventTypesAsync() } returns dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
            submitter.submitMetrics()
            submitter.submitMetrics()
        }

        coVerify(exactly = 3) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 3) { dbEventCounterServiceOnPrem.countAllEventTypesAsync() }

        coVerify(exactly = 8 * 2) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 8 * 2) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(dbEventCounterServiceOnPrem)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics if one of the counting fails`() {
        val simulatedException = Exception("Simulated error in a test")
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        coEvery { topicEventCounterServiceOnPrem.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { topicEventCounterServiceGCP.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterServiceOnPrem.countAllEventTypesAsync() } throws simulatedException andThen dbMetricsSessions
        coEvery { dbEventCounterServiceGCP.countAllEventTypesAsync() } returns dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterServiceOnPrem.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterServiceOnPrem.countAllEventTypesAsync() }

        coVerify(exactly = 0) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 0) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterServiceOnPrem)
        confirmVerified(dbEventCounterServiceOnPrem)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

}
