package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.SimpleEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.ThrowingEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util.KafkaTestUtil
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.RetriableDatabaseException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.UnretriableDatabaseException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Kafka
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.createNokkel
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should contain same`
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test

class ConsumerTestIT {

    private val beskjedEvents = (1..10).map { createNokkel(it) to AvroBeskjedObjectMother.createBeskjed(it) }.toMap()

    val topic = "kafkaConsumerStateTestTopic"


    @Test
    fun `Should attempt process each event exactly once if no exceptions are thrown`() {

        val embeddedEnv = KafkaTestUtil.createKafkaEmbeddedInstanceWithNumPartitions(listOf(topic), 4)
        val testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        val consumerProps = Kafka.consumerProps(testEnvironment, EventType.BESKJED, true).apply {
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
        }

        embeddedEnv.start()

        val eventProcessor = ThrowingEventCounterService<Beskjed>()
        val kafkaConsumer = KafkaConsumer<Nokkel, Beskjed>(consumerProps)
        val consumer = Consumer(topic, kafkaConsumer, eventProcessor)

        runBlocking {

            KafkaTestUtil.produceEvents(testEnvironment, topic, beskjedEvents)
            pollUntilDone(consumer)
        }

        embeddedEnv.tearDown()

        eventProcessor.successfulEventsCounter `should be equal to` beskjedEvents.size
        eventProcessor.invocationCounter `should be equal to` beskjedEvents.size
        eventProcessor.successfulEvents `should contain same` beskjedEvents.values
    }

    // Ønsker midlertidig at all polling stoppes ved alle typer feil. Denne endringen skal reverteres på et senere tidspunkt.
    @Test
    fun `Should not attempt to process some events multiple times if a retriable exception was raised`() {

        val embeddedEnv = KafkaTestUtil.createKafkaEmbeddedInstanceWithNumPartitions(listOf(topic), 4)
        val testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        val consumerProps = Kafka.consumerProps(testEnvironment, EventType.BESKJED, true).apply {
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
        }

        embeddedEnv.start()

        val failForEveryEvents = 5
        val eventProcessor = ThrowingEventCounterService<Beskjed>(RetriableDatabaseException("Transient error"), failForEveryEvents)
        val kafkaConsumer = KafkaConsumer<Nokkel, Beskjed>(consumerProps)
        val consumer = Consumer(topic, kafkaConsumer, eventProcessor)

        runBlocking {

            KafkaTestUtil.produceEvents(testEnvironment, topic, beskjedEvents)
            pollUntilDone(consumer)
        }

        embeddedEnv.tearDown()

        val listSizeOfSuccessfulEvents = failForEveryEvents - 1
        eventProcessor.successfulEventsCounter `should be equal to` listSizeOfSuccessfulEvents
        eventProcessor.invocationCounter `should be equal to` failForEveryEvents
    }

    @Test
    fun `Should stop processing events if a non-retriable exception was raised`() {

        val embeddedEnv = KafkaTestUtil.createKafkaEmbeddedInstanceWithNumPartitions(listOf(topic), 4)
        val testEnvironment = KafkaTestUtil.createEnvironmentForEmbeddedKafka(embeddedEnv)
        val consumerProps = Kafka.consumerProps(testEnvironment, EventType.BESKJED, true).apply {
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
        }

        embeddedEnv.start()

        val eventProcessor = ThrowingEventCounterService<Beskjed>(UnretriableDatabaseException("Fatal error"), 5)
        val kafkaConsumer = KafkaConsumer<Nokkel, Beskjed>(consumerProps)
        val consumer = Consumer(topic, kafkaConsumer, eventProcessor)

        runBlocking {

            KafkaTestUtil.produceEvents(testEnvironment, topic, beskjedEvents)
            pollUntilDone(consumer)
        }

        embeddedEnv.tearDown()

        eventProcessor.successfulEventsCounter `should be equal to` 4
        eventProcessor.invocationCounter `should be equal to` 5
    }

    suspend fun pollUntilDone(consumer: Consumer<Beskjed>) {
        consumer.startPolling()
        while (getProcessedCount(consumer) < beskjedEvents.size && consumer.job.isActive) {
            delay(100)
        }
        consumer.stopPolling()
    }

    private fun getProcessedCount(consumer: Consumer<Beskjed>): Int {
        val processor = consumer.eventBatchProcessorService

        return when(processor) {
            is SimpleEventCounterService<*> -> processor.eventCounter
            is ThrowingEventCounterService<*> -> processor.successfulEventsCounter
            else -> 0
        }
    }
}