package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import com.squareup.wire.internal.copyOf
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup.createCountConsumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.createNokkel
import org.amshove.kluent.`should be equal to`
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.*

class TopicEventCounterServiceIT {

    private val topic = "topic"
    private lateinit var embeddedEnv : KafkaEnvironment
    private lateinit var testEnvironment: Environment

    private val events = (1..5).map { createNokkel(it) to AvroBeskjedObjectMother.createBeskjed(it) }.toMap()

    @AfterEach
    fun `tear down`() {
        embeddedEnv.adminClient?.close()
        embeddedEnv.tearDown()
    }

    @BeforeEach
    fun setup() {
        embeddedEnv = KafkaTestUtil.createDefaultKafkaEmbeddedInstance(listOf(topic))
        testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        embeddedEnv.start()
    }

    @Test
    fun `Skal telle korrekt total antall av eventer og gruppere de som er unike og duplikater`() {
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


    @Disabled
    @Test
    fun `Ved telling etter reset av offset blir resultatet det samme som etter deltatelling `() {
        val deltaCountingConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
        val fromScratchCountingConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, topic, testEnvironment, true)
        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = deltaCountingConsumer,
            eventType = EventType.BESKJED,
            deltaCountingEnabled = true
        )
        val fromScratchTopicEventTypeCounter = TopicEventTypeCounter(
            kafkaConsumer = fromScratchCountingConsumer,
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
        deltaMetricsSession.getNumberOfUniqueEvents() `should be equal to` fromScratchMetricsSession.getNumberOfUniqueEvents()

        deltaCountingConsumer.close()
        fromScratchCountingConsumer.close()
    }

    @Test
    fun `Skal telle riktig antall eventer flere ganger paa rad ved bruk av samme kafka-klient`() {
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
