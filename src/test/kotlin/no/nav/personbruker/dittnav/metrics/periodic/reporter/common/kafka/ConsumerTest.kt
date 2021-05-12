package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka

import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.objectmother.ConsumerRecordsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.Status
import org.amshove.kluent.`should be equal to`
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class ConsumerTest {

    private val kafkaConsumer = mockk<KafkaConsumer<Nokkel, Beskjed>>(relaxUnitFun = true)

    companion object {
        private val defaultMaxPollTimeout = Duration.ofMillis(5000)
    }

    @BeforeEach
    fun initMocks() {
        clearMocks(kafkaConsumer)
    }

    @Test
    fun `Skal polle mot Kafka hvis ingen feil skjer`() {
        val topic = "dummyTopicNoErrors"
        val records: ConsumerRecords<Nokkel, Beskjed>
        every { kafkaConsumer.poll(any<Duration>()) } returns ConsumerRecordsObjectMother.giveMeANumberOfBeskjedRecords(1, topic)

        val consumer: Consumer<Nokkel, Beskjed> = Consumer(topic, kafkaConsumer)

        runBlocking {
            consumer.startSubscription()
            records = consumer.kafkaConsumer.poll(defaultMaxPollTimeout)
            delay(300)

            records.count() `should be equal to` 1
            consumer.status().status `should be equal to` Status.OK
            consumer.stop()
        }
    }
}
