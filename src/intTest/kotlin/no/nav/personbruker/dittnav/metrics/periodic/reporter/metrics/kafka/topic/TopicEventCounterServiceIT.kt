package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup.createCountConsumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.createNokkel
import org.amshove.kluent.`should be equal to`
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class TopicEventCounterServiceIT {

    private val topicen = "topicEventCounterServiceIT"
    private val embeddedEnv = KafkaTestUtil.createDefaultKafkaEmbeddedInstance(listOf(topicen))
    private val testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)

    private val adminClient = embeddedEnv.adminClient

    private val events = (1..5).map { createNokkel(it) to AvroBeskjedObjectMother.createBeskjed(it) }.toMap()

    private val environment = mockk<Environment>().also {
        every { it.deltaCountingEnabled } returns false
    }

    init {
        embeddedEnv.start()
        `Produser det samme settet av eventer tre ganger`()
    }

    @AfterAll
    fun `tear down`() {
        adminClient?.close()
        embeddedEnv.tearDown()
    }

    @Test
    fun `Skal telle korrekt total antall av eventer og gruppere de som er unike og duplikater`() {

        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topicen, testEnvironment, true)

        val topicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = beskjedCountConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = false
        )

        val metricsSession = runBlocking {
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getDuplicates() `should be equal to` events.size * 2
        metricsSession.getTotalNumber() `should be equal to` events.size * 3
        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size

        beskjedCountConsumer.close()
    }

    @Test
    fun `Skal telle riktig antall eventer flere ganger paa rad ved bruk av samme kafka-klient`() {
        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topicen, testEnvironment, true)
        val topicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = beskjedCountConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = false
        )

        `tell og verifiser korrekte antall eventer flere ganger paa rad`(topicEventTypeCounter)

        beskjedCountConsumer.close()
    }

    private fun `tell og verifiser korrekte antall eventer flere ganger paa rad`(
        topicEventTypeCounter: TopicEventTypeCounter
    ) {
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
    }

    private fun `tell og verifiser korrekt antall eventer`(
        topicEventTypeCounter: TopicEventTypeCounter
    ) {
        val metricsSession = runBlocking {
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size
    }

    private fun `Produser det samme settet av eventer tre ganger`() {
        runBlocking {
            val fikkProduserBatch1 = KafkaTestUtil.produceEvents(testEnvironment, topicen, events)
            val fikkProduserBatch2 = KafkaTestUtil.produceEvents(testEnvironment, topicen, events)
            val fikkProduserBatch3 = KafkaTestUtil.produceEvents(testEnvironment, topicen, events)
            fikkProduserBatch1 && fikkProduserBatch2 && fikkProduserBatch3
        } `should be equal to` true
    }

}
