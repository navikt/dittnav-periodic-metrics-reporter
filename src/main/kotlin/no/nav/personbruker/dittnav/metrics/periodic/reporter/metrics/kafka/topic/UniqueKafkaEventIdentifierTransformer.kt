package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

object UniqueKafkaEventIdentifierTransformer {

    private val log = LoggerFactory.getLogger(UniqueKafkaEventIdentifierTransformer::class.java)

    fun toInternal(external: ConsumerRecord<Nokkel, GenericRecord>): UniqueKafkaEventIdentifier {
        val nokkel = external.key()
        val record = external.value()

        when {
            nokkel == null -> {
                val invalidEvent = UniqueKafkaEventIdentifier.createInvalidEvent()
                log.warn("Kan ikke telle eventet, fordi kafka-key (Nokkel) er null. Transformerer til et dummy-event: $invalidEvent.")
                return invalidEvent
            }
            record == null -> {
                val eventWithoutActualFnr = UniqueKafkaEventIdentifier.createEventWithoutValidFnr(
                    nokkel.getEventId(),
                    nokkel.getSystembruker()
                )
                log.warn("Kan ikke telle eventet, fordi kafka-value (Record) er null. Transformerer til et event med et dummy fødselsnummer: $eventWithoutActualFnr.")
                return eventWithoutActualFnr
            }
            else -> {
                return UniqueKafkaEventIdentifier(
                    nokkel.getEventId(),
                    nokkel.getSystembruker(),
                    record.get("fodselsnummer").toString()
                )
            }
        }
    }

}
