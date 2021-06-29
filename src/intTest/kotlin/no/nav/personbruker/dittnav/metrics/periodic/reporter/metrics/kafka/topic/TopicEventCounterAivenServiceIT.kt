package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Kafka
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.AvroNokkelInternObjectMother
import org.amshove.kluent.`should be equal to`
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TopicEventCounterAivenServiceIT {

    private val topic = "topic"
    private lateinit var embeddedEnv: KafkaEnvironment
    private lateinit var testEnvironment: Environment
    private val topicActivityService =  TopicActivityService(30)

    private val events = (1..5).map { AvroNokkelInternObjectMother.createNokkelIntern(it) to AvroBeskjedInternObjectMother.createBeskjedIntern() }.toMap()

    @AfterEach
    fun `tear down`() {
        embeddedEnv.adminClient?.close()
        embeddedEnv.tearDown()
    }

    @BeforeEach
    fun setup() {
        embeddedEnv = KafkaTestUtil.createDefaultKafkaEmbeddedInstance(withSecurity = false, listOf(topic))
        testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        embeddedEnv.start()
    }

    @Test
    fun `Skal telle korrekt total antall av eventer og gruppere de som er unike og duplikater`() {
        `Produser det samme settet av eventer tre ganger`(topic)

        val kafkaProps = Kafka.counterConsumerAivenProps(testEnvironment, EventType.BESKJED_INTERN, false)
        val beskjedInternCountConsumer = KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaProps, topic)
        beskjedInternCountConsumer.startSubscription()

        val topicEventTypeCounter = TopicEventTypeCounter(
            consumer = beskjedInternCountConsumer,
            topicActivityService = topicActivityService,
            eventType = EventType.BESKJED_INTERN,
            deltaCountingEnabled = false
        )

        val metricsSession = runBlocking {
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getDuplicates() `should be equal to` events.size * 2
        metricsSession.getTotalNumber() `should be equal to` events.size * 3
        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size

        runBlocking { beskjedInternCountConsumer.stop() }
    }

    @Test
    fun `Ved deltatelling skal metrikkene akkumuleres fra forrige telling`() {
        val kafkaProps = Kafka.counterConsumerAivenProps(testEnvironment, EventType.BESKJED_INTERN, false)
        val beskjedInternCountConsumer = KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaProps, topic)
        beskjedInternCountConsumer.startSubscription()

        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
            consumer = beskjedInternCountConsumer,
            topicActivityService = topicActivityService,
            eventType = EventType.BESKJED_INTERN,
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

        runBlocking { beskjedInternCountConsumer.stop() }
    }

    @Test
    fun `Ved telling etter reset av offset blir resultatet det samme som etter deltatelling `() {
        val deltaCountingEnv = testEnvironment.copy(groupIdBase = "delta")
        val fromScratchCountingEnv = testEnvironment.copy(groupIdBase = "fromScratch")

        val kafkaPropsDeltaCounting = Kafka.counterConsumerAivenProps(deltaCountingEnv, EventType.BESKJED_INTERN, false)
        val deltaCountingConsumer =
            KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaPropsDeltaCounting, topic)
        deltaCountingConsumer.startSubscription()

        val kafkaPropsFromScratchCounting = Kafka.counterConsumerAivenProps(fromScratchCountingEnv, EventType.BESKJED_INTERN, false)
        val fromScratchCountingConsumer =
            KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaPropsFromScratchCounting, topic)
        fromScratchCountingConsumer.startSubscription()

        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
            consumer = deltaCountingConsumer,
            topicActivityService = topicActivityService,
            eventType = EventType.BESKJED_INTERN,
            deltaCountingEnabled = true
        )
        val fromScratchTopicEventTypeCounter = TopicEventTypeCounter(
            consumer = fromScratchCountingConsumer,
            topicActivityService = topicActivityService,
            eventType = EventType.BESKJED_INTERN,
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

        runBlocking {
            deltaCountingConsumer.stop()
            fromScratchCountingConsumer.stop()
        }
    }

    @Test
    fun `Skal telle riktig antall eventer flere ganger paa rad ved bruk av samme kafka-klient`() {
        `Produser det samme settet av eventer tre ganger`(topic)
        val kafkaProps = Kafka.counterConsumerAivenProps(testEnvironment, EventType.BESKJED_INTERN, false)
        val beskjedCountConsumer = KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaProps, topic)
        beskjedCountConsumer.startSubscription()

        val topicEventTypeCounter = TopicEventTypeCounter(
            consumer = beskjedCountConsumer,
            topicActivityService = topicActivityService,
            eventType = EventType.BESKJED_INTERN,
            deltaCountingEnabled = false
        )

        `tell og verifiser korrekte antall eventer flere ganger paa rad`(topicEventTypeCounter)

        runBlocking { beskjedCountConsumer.stop() }
    }

    private fun `tell og verifiser korrekte antall eventer flere ganger paa rad`(
        topicEventTypeCounter: TopicEventTypeCounter<NokkelIntern>
    ) {
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
    }

    private fun `tell og verifiser korrekt antall eventer`(
        topicEventTypeCounter: TopicEventTypeCounter<NokkelIntern>
    ) {
        val metricsSession = runBlocking {
            delay(500)
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size
    }

    private fun `Produser det samme settet av eventer tre ganger`(topic: String) {
        runBlocking {
            val fikkProduserBatch1 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity =  false, events)
            val fikkProduserBatch2 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity = false, events)
            val fikkProduserBatch3 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity = false, events)
            fikkProduserBatch1 && fikkProduserBatch2 && fikkProduserBatch3
        } `should be equal to` true
    }

}
