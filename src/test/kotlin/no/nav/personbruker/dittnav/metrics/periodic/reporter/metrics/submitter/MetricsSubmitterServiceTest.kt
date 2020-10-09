package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import io.mockk.*
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessionsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class MetricsSubmitterServiceTest {

    private val dbMetricsReporter: DbMetricsReporter = mockk(relaxed = true)
    private val kafkaMetricsReporter: TopicMetricsReporter = mockk(relaxed = true)
    private val dbEventCounterService: DbEventCounterService = mockk(relaxed = true)
    private val topicEventCounterService: TopicEventCounterService = mockk(relaxed = true)

    private val submitter = MetricsSubmitterService(
        dbEventCounterService,
        topicEventCounterService,
        dbMetricsReporter,
        kafkaMetricsReporter
    )

    @BeforeEach
    fun cleanup() {
        clearAllMocks()
    }

    @Test
    fun `Should report metrics for both kafka topics and the database cache`() {
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        coEvery { topicEventCounterService.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterService.countAllEventTypesAsync() } returns dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterService.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterService.countAllEventTypesAsync() }

        coVerify(exactly = 4) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 4) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterService)
        confirmVerified(dbEventCounterService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    // Sjekk at det ikke rapporteres hvis det ikke her telt for Innboks.


    @Test
    fun `Should not report metrics for count sessions with a lower count than the previous count session`() {
        val sessionWithCorretCount = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val simulatedWrongCount =
            CountingMetricsSessionsObjectMother.giveMeTopicSessionsWithSingleEventForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        coEvery {
            topicEventCounterService.countAllEventTypesAsync()
        } returns sessionWithCorretCount andThen simulatedWrongCount andThen sessionWithCorretCount

        coEvery {
            dbEventCounterService.countAllEventTypesAsync()
        } returns dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
            submitter.submitMetrics()
            submitter.submitMetrics()
        }

        coVerify(exactly = 3) { topicEventCounterService.countAllEventTypesAsync() }
        coVerify(exactly = 3) { dbEventCounterService.countAllEventTypesAsync() }

        coVerify(exactly = 4 * 2) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 4 * 2) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterService)
        confirmVerified(dbEventCounterService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

    @Test
    fun `Should not report metrics if one of the counting fails`() {
        val simulatedException = Exception("Simulated error in a test")
        val topicMetricsSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()
        val dbMetricsSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()
        coEvery { topicEventCounterService.countAllEventTypesAsync() } returns topicMetricsSessions
        coEvery { dbEventCounterService.countAllEventTypesAsync() } throws simulatedException andThen dbMetricsSessions

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { topicEventCounterService.countAllEventTypesAsync() }
        coVerify(exactly = 1) { dbEventCounterService.countAllEventTypesAsync() }

        coVerify(exactly = 0) { kafkaMetricsReporter.report(any()) }
        coVerify(exactly = 0) { dbMetricsReporter.report(any()) }

        confirmVerified(topicEventCounterService)
        confirmVerified(dbEventCounterService)
        confirmVerified(kafkaMetricsReporter)
        confirmVerified(dbMetricsReporter)
    }

}
