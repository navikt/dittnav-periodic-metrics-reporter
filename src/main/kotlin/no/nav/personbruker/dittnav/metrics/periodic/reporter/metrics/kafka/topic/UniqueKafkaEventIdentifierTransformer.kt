package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.internal.NokkelFeilrespons
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

object UniqueKafkaEventIdentifierTransformer {

    private val log = LoggerFactory.getLogger(UniqueKafkaEventIdentifierTransformer::class.java)

    fun <K> toInternal(external: ConsumerRecord<K, GenericRecord>): UniqueKafkaEventIdentifier {
        val key = external.key()
        val value = external.value()

        return when(key) {
            null -> {
                val invalidEvent = UniqueKafkaEventIdentifier.createInvalidEvent()
                log.warn("Kan ikke telle eventet, fordi kafka-key (Nokkel) er null. Transformerer til et dummy-event: $invalidEvent.")
                return invalidEvent
            }
            is Nokkel -> {
                fromNokkel(key, value)
            }
            is NokkelIntern -> {
                fromNokkelIntern(key)
            }
            is NokkelFeilrespons -> {
                fromNokkelFeilrespons(key)
            }
            else -> {
                val invalidEvent = UniqueKafkaEventIdentifier.createInvalidEvent()
                log.warn("Kan ikke telle eventet, fordi kafka-key (Nokkel) er av ukjent type. Transformerer til et dummy-event: $invalidEvent.")
                return invalidEvent
            }
        }
    }

    private fun fromNokkelIntern(key: NokkelIntern): UniqueKafkaEventIdentifier {
        return UniqueKafkaEventIdentifier(
            key.getEventId(),
            key.getSystembruker(),
            key.getFodselsnummer()
        )
    }

    private fun fromNokkelFeilrespons(key: NokkelFeilrespons): UniqueKafkaEventIdentifier {
        return UniqueKafkaEventIdentifier.createEventWithoutValidFnr(
            key.getEventId(),
            key.getSystembruker(),
        )
    }

    private fun fromNokkel(key: Nokkel, value: GenericRecord?): UniqueKafkaEventIdentifier {
        when (value) {
            null -> {
                val eventWithoutActualFnr = UniqueKafkaEventIdentifier.createEventWithoutValidFnr(
                    key.getEventId(),
                    key.getSystembruker()
                )
                log.warn("Kan ikke telle eventet, fordi kafka-value (Record) er null. Transformerer til et event med et dummy fødselsnummer: $eventWithoutActualFnr.")
                return eventWithoutActualFnr
            }
            else -> {
                return UniqueKafkaEventIdentifier(
                    key.getEventId(),
                    key.getSystembruker(),
                    value.get("fodselsnummer").toString()
                )
            }
        }
    }
}
