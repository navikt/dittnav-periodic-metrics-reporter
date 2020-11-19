package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup.createCountConsumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.createNokkel
import org.amshove.kluent.`should be equal to`
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class TopicEventCounterServiceIT {

    private val topics = (1..4).map { "testTopic-${it}"}
    private val embeddedEnv = KafkaTestUtil.createDefaultKafkaEmbeddedInstance(topics)
    private val testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)

    private val adminClient = embeddedEnv.adminClient

    private val events = (1..5).map { createNokkel(it) to AvroBeskjedObjectMother.createBeskjed(it) }.toMap()

    init {
        embeddedEnv.start()
    }

    @AfterAll
    fun `tear down`() {
        adminClient?.close()
        embeddedEnv.tearDown()
    }

    @Test
    fun `Skal telle korrekt total antall av eventer og gruppere de som er unike og duplikater`() {
        val topic = "testTopic-1"
        `Produser det samme settet av eventer tre ganger`(topic)
        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
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
    fun `Ved deltatelling skal metrikkene akkumuleres fra forrige telling`() {
        val topic = "testTopic-2"

        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = beskjedCountConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = true
        )
        `Produser det samme settet av eventer tre ganger`(topic)
        runBlocking {
            deltaTopicEventTypeCounter.countEventsAsync().await()
        }

        `Produser det samme settet av eventer tre ganger`(topic)
        val metricsSession = runBlocking {
            deltaTopicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getDuplicates() `should be equal to` events.size * 5
        metricsSession.getTotalNumber() `should be equal to` events.size * 6
        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size

        beskjedCountConsumer.close()
    }


    @Test
    fun `Ved telling etter reset av offset blir resultatet det samme som etter deltatelling `() {
        val topic = "testTopic-3"

        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
        createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = beskjedCountConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = true
        )
        val fromScratchTopicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = beskjedCountConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = false
        )

        `Produser det samme settet av eventer tre ganger`(topic)
        runBlocking {
            deltaTopicEventTypeCounter.countEventsAsync().await()
        }
        `Produser det samme settet av eventer tre ganger`(topic)
        val deltaMetricsSession = runBlocking {
            deltaTopicEventTypeCounter.countEventsAsync().await()
        }

        val fromScratchMetricsSession = runBlocking {
            fromScratchTopicEventTypeCounter.countEventsAsync().await()
        }


        deltaMetricsSession.getDuplicates() `should be equal to` fromScratchMetricsSession.getDuplicates()
        deltaMetricsSession.getTotalNumber() `should be equal to` fromScratchMetricsSession.getTotalNumber()
        deltaMetricsSession.getNumberOfUniqueEvents() `should be equal to` fromScratchMetricsSession.getDuplicates()

        beskjedCountConsumer.close()
    }

    @Test
    fun `Skal telle riktig antall eventer flere ganger paa rad ved bruk av samme kafka-klient`() {
        val topic = "testTopic-4"
        `Produser det samme settet av eventer tre ganger`(topic)
        val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
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

    private fun `Produser det samme settet av eventer tre ganger`(topic: String) {
        runBlocking {
            val fikkProduserBatch1 = KafkaTestUtil.produceEvents(testEnvironment, topic, events)
            val fikkProduserBatch2 = KafkaTestUtil.produceEvents(testEnvironment, topic, events)
            val fikkProduserBatch3 = KafkaTestUtil.produceEvents(testEnvironment, topic, events)
            fikkProduserBatch1 && fikkProduserBatch2 && fikkProduserBatch3
        } `should be equal to` true
    }



}
