package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Kafka
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.AvroNokkelObjectMother.createNokkel
import org.amshove.kluent.`should be equal to`
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TopicEventCounterOnPremServiceIT {

    private val topic = "topic"
    private lateinit var embeddedEnv: KafkaEnvironment
    private lateinit var testEnvironment: Environment
    private val activityService: TopicActivityService = mockk()

    private val events = (1..5).map { createNokkel(it) to AvroBeskjedObjectMother.createBeskjed(it) }.toMap()

    @AfterEach
    fun `tear down`() {
        embeddedEnv.adminClient?.close()
        embeddedEnv.tearDown()
    }

    @BeforeEach
    fun setup() {
        embeddedEnv = KafkaTestUtil.createDefaultKafkaEmbeddedInstance(withSecurity = true, listOf(topic))
        testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        embeddedEnv.start()
    }

    @Test
    fun `Skal telle korrekt total antall av eventer og gruppere de som er unike og duplikater`() {
        `Produser det samme settet av eventer tre ganger`(topic)

        val kafkaProps = Kafka.counterConsumerOnPremProps(testEnvironment, EventType.BESKJED, true)
        val beskjedCountConsumer = KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaProps, topic)
        beskjedCountConsumer.startSubscription()

        every { activityService.reportEventsFound() } returns Unit
        every { activityService.reportNoEventsFound() } returns Unit

        val topicEventTypeCounter = TopicEventTypeCounter(
                consumer = beskjedCountConsumer,
                topicActivityService = activityService,
                eventType = EventType.BESKJED,
                deltaCountingEnabled = false
        )

        val metricsSession = runBlocking {
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getDuplicates() `should be equal to` events.size * 2
        metricsSession.getTotalNumber() `should be equal to` events.size * 3
        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size

        runBlocking { beskjedCountConsumer.stop() }
    }

    @Test
    fun `Ved deltatelling skal metrikkene akkumuleres fra forrige telling`() {
        val kafkaProps = Kafka.counterConsumerOnPremProps(testEnvironment, EventType.BESKJED, true)
        val beskjedCountConsumer = KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaProps, topic)
        beskjedCountConsumer.startSubscription()

        every { activityService.reportEventsFound() } returns Unit
        every { activityService.reportNoEventsFound() } returns Unit

        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
                consumer = beskjedCountConsumer,
                topicActivityService = activityService,
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

        runBlocking { beskjedCountConsumer.stop() }
    }

    @Test
    fun `Ved telling etter reset av offset blir resultatet det samme som etter deltatelling `() {
        val deltaCountingEnv = testEnvironment.copy(groupIdBase = "delta")
        val fromScratchCountingEnv = testEnvironment.copy(groupIdBase = "fromScratch")

        val kafkaPropsDeltaCounting = Kafka.counterConsumerOnPremProps(deltaCountingEnv, EventType.BESKJED, true)
        val deltaCountingConsumer = KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaPropsDeltaCounting, topic)
        deltaCountingConsumer.startSubscription()

        val kafkaPropsFromScratchCounting = Kafka.counterConsumerOnPremProps(fromScratchCountingEnv, EventType.BESKJED, true)
        val fromScratchCountingConsumer = KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaPropsFromScratchCounting, topic)
        fromScratchCountingConsumer.startSubscription()

        every { activityService.reportEventsFound() } returns Unit
        every { activityService.reportNoEventsFound() } returns Unit

        val deltaTopicEventTypeCounter = TopicEventTypeCounter(
                consumer = deltaCountingConsumer,
                topicActivityService = activityService,
                eventType = EventType.BESKJED,
                deltaCountingEnabled = true
        )
        val fromScratchTopicEventTypeCounter = TopicEventTypeCounter(
                consumer = fromScratchCountingConsumer,
                topicActivityService = activityService,
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

        runBlocking {
            deltaCountingConsumer.stop()
            fromScratchCountingConsumer.stop()
        }
    }

    @Test
    fun `Skal telle riktig antall eventer flere ganger paa rad ved bruk av samme kafka-klient`() {
        `Produser det samme settet av eventer tre ganger`(topic)
        val kafkaProps = Kafka.counterConsumerOnPremProps(testEnvironment, EventType.BESKJED, true)
        val beskjedCountConsumer = KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaProps, topic)
        beskjedCountConsumer.startSubscription()

        every { activityService.reportEventsFound() } returns Unit
        every { activityService.reportNoEventsFound() } returns Unit

        val topicEventTypeCounter = TopicEventTypeCounter(
                consumer = beskjedCountConsumer,
                topicActivityService = activityService,
                eventType = EventType.BESKJED,
                deltaCountingEnabled = false
        )

        `tell og verifiser korrekte antall eventer flere ganger paa rad`(topicEventTypeCounter)

        runBlocking { beskjedCountConsumer.stop() }
    }

    private fun `tell og verifiser korrekte antall eventer flere ganger paa rad`(
            topicEventTypeCounter: TopicEventTypeCounter<Nokkel>
    ) {
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
        `tell og verifiser korrekt antall eventer`(topicEventTypeCounter)
    }

    private fun `tell og verifiser korrekt antall eventer`(
            topicEventTypeCounter: TopicEventTypeCounter<Nokkel>
    ) {
        val metricsSession = runBlocking {
            delay(500)
            topicEventTypeCounter.countEventsAsync().await()
        }

        metricsSession.getNumberOfUniqueEvents() `should be equal to` events.size
    }

    private fun `Produser det samme settet av eventer tre ganger`(topic: String) {
        runBlocking {
            val fikkProduserBatch1 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity = true, events)
            val fikkProduserBatch2 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity = true, events)
            val fikkProduserBatch3 = KafkaTestUtil.produceEvents(testEnvironment, topic, enableSecurity = true, events)
            fikkProduserBatch1 && fikkProduserBatch2 && fikkProduserBatch3
        } `should be equal to` true
    }
}
