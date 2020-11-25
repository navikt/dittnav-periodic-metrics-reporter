package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should not be equal to`
import org.junit.jupiter.api.Test

internal class TopicMetricsSessionTest {

    val produsent1 = "p-1"
    val produsent2 = "p-2"
    val produsent3 = "p-3"

    @Test
    fun `Skal summere opp eventer riktig i hver kategori per produsent`() {
        val metricsSession = createAndInitiateBeskjedMetricsSession()

        metricsSession.getDuplicates() `should be equal to` 3 + 1 + 2
        metricsSession.getDuplicates(produsent1) `should be equal to` 3
        metricsSession.getDuplicates(produsent2) `should be equal to` 1
        metricsSession.getDuplicates(produsent3) `should be equal to` 2

        metricsSession.getTotalNumber() `should be equal to` 4 + 3 + 5
        metricsSession.getTotalNumber(produsent1) `should be equal to` 4
        metricsSession.getTotalNumber(produsent2) `should be equal to` 3
        metricsSession.getTotalNumber(produsent3) `should be equal to` 5

        metricsSession.getNumberOfUniqueEvents() `should be equal to` 1 + 2 + 3
        metricsSession.getNumberOfUniqueEvents(produsent1) `should be equal to` 1
        metricsSession.getNumberOfUniqueEvents(produsent2) `should be equal to` 2
        metricsSession.getNumberOfUniqueEvents(produsent3) `should be equal to` 3

        metricsSession.getProducersWithEvents().size `should be equal to` 3
    }

    @Test
    fun `Should copy state from one session to another`() {
        val originalMetricsSession = createAndInitiateBeskjedMetricsSession()

        val other = TopicMetricsSession(originalMetricsSession)

        other.getDuplicates() `should be equal to` originalMetricsSession.getDuplicates()
        other.getDuplicates(produsent1) `should be equal to` originalMetricsSession.getDuplicates(produsent1)
        other.getDuplicates(produsent2) `should be equal to` originalMetricsSession.getDuplicates(produsent2)
        other.getDuplicates(produsent3) `should be equal to` originalMetricsSession.getDuplicates(produsent3)

        other.getTotalNumber() `should be equal to` originalMetricsSession.getTotalNumber()
        other.getTotalNumber(produsent1) `should be equal to` originalMetricsSession.getTotalNumber(produsent1)
        other.getTotalNumber(produsent2) `should be equal to` originalMetricsSession.getTotalNumber(produsent2)
        other.getTotalNumber(produsent3) `should be equal to` originalMetricsSession.getTotalNumber(produsent3)

        other.getNumberOfUniqueEvents() `should be equal to` originalMetricsSession.getNumberOfUniqueEvents()
        other.getNumberOfUniqueEvents(produsent1) `should be equal to` originalMetricsSession.getNumberOfUniqueEvents(produsent1)
        other.getNumberOfUniqueEvents(produsent2) `should be equal to` originalMetricsSession.getNumberOfUniqueEvents(produsent2)
        other.getNumberOfUniqueEvents(produsent3) `should be equal to` originalMetricsSession.getNumberOfUniqueEvents(produsent3)

        other.getProducersWithEvents().size `should be equal to` 3

        other.getProcessingTime() `should not be equal to` originalMetricsSession.getProcessingTime()
    }

    @Test
    fun `Should copy state from one session to another and allow for further counting`() {
        val originalMetricsSession = createAndInitiateBeskjedMetricsSession()

        val other = TopicMetricsSession(originalMetricsSession)

        val produsent1Event2 = UniqueKafkaEventIdentifier("b-2", produsent1, "1")
        val produsent2Event3 = UniqueKafkaEventIdentifier("b-13", produsent2, "2")
        val produsent2Event4 = UniqueKafkaEventIdentifier("b-14", produsent2, "2")

        other.countEvent(produsent1Event2)
        other.countEvent(produsent1Event2)
        other.countEvent(produsent2Event3)
        other.countEvent(produsent2Event4)
        other.countEvent(produsent2Event4)
        other.countEvent(produsent2Event4)

        other.getDuplicates() `should be equal to` 3 + 1 + 2 + 1 + 2
        other.getDuplicates(produsent1) `should be equal to` 3 + 1
        other.getDuplicates(produsent2) `should be equal to` 1 + 2
        other.getDuplicates(produsent3) `should be equal to` 2

        other.getTotalNumber() `should be equal to` 4 + 3 + 5 + 2 + 4
        other.getTotalNumber(produsent1) `should be equal to` 4 + 2
        other.getTotalNumber(produsent2) `should be equal to` 3 + 4
        other.getTotalNumber(produsent3) `should be equal to` 5

        other.getNumberOfUniqueEvents() `should be equal to` 1 + 2 + 3 + 1 + 2
        other.getNumberOfUniqueEvents(produsent1) `should be equal to` 1 + 1
        other.getNumberOfUniqueEvents(produsent2) `should be equal to` 2 + 2
        other.getNumberOfUniqueEvents(produsent3) `should be equal to` 3

        other.getProducersWithEvents().size `should be equal to` 3
    }

    private fun createAndInitiateBeskjedMetricsSession(): TopicMetricsSession {

        val metricsSession = TopicMetricsSession(EventType.BESKJED)

        val produsent1Event1 = UniqueKafkaEventIdentifier("b-1", produsent1, "1")
        val produsent2Event1 = UniqueKafkaEventIdentifier("b-11", produsent2, "2")
        val produsent2Event2 = UniqueKafkaEventIdentifier("b-12", produsent2, "2")
        val produsent3Event1 = UniqueKafkaEventIdentifier("b-21", produsent3, "3")
        val produsent3Event2 = UniqueKafkaEventIdentifier("b-22", produsent3, "3")
        val produsent3Event3 = UniqueKafkaEventIdentifier("b-23", produsent3, "3")


        metricsSession.countEvent(produsent1Event1)
        metricsSession.countEvent(produsent1Event1)
        metricsSession.countEvent(produsent1Event1)
        metricsSession.countEvent(produsent1Event1)

        metricsSession.countEvent(produsent2Event1)
        metricsSession.countEvent(produsent2Event2)
        metricsSession.countEvent(produsent2Event2)

        metricsSession.countEvent(produsent3Event1)
        metricsSession.countEvent(produsent3Event2)
        metricsSession.countEvent(produsent3Event3)
        metricsSession.countEvent(produsent3Event3)
        metricsSession.countEvent(produsent3Event3)

        metricsSession.calculateProcessingTime()

        return metricsSession
    }

}
