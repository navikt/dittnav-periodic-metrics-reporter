package no.nav.personbruker.dittnav.eventaggregator.beskjed

import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.eventaggregator.common.EventBatchProcessorService
import no.nav.personbruker.dittnav.eventaggregator.common.exceptions.NokkelNullException
import no.nav.personbruker.dittnav.eventaggregator.common.exceptions.UntransformableRecordException
import no.nav.personbruker.dittnav.eventaggregator.common.kafka.serializer.getNonNullKey
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BeskjedEventService(
        private val beskjedRepository: BeskjedRepository
) : EventBatchProcessorService<Beskjed> {

    fun initBeskjedMetrics() {
        runBlocking {
            beskjedRepository.getBeskjedMetricsState()
        }.let { lifetimeMetrics ->
            initMetrics(lifetimeMetrics)
        }
    }

    private val log: Logger = LoggerFactory.getLogger(BeskjedEventService::class.java)

    override suspend fun processEvents(events: ConsumerRecords<Nokkel, Beskjed>) {
        val successfullyTransformedEvents = mutableListOf<no.nav.personbruker.dittnav.eventaggregator.beskjed.Beskjed>()
        val problematicEvents = mutableListOf<ConsumerRecord<Nokkel, Beskjed>>()
        events.forEach { event ->
            try {
                registerMetrics(event)
                val internalEvent = BeskjedTransformer.toInternal(event.getNonNullKey(), event.value())
                successfullyTransformedEvents.add(internalEvent)
            } catch (e: NokkelNullException) {
                log.warn("Eventet manglet nøkkel. Topic: ${event.topic()}, Partition: ${event.partition()}, Offset: ${event.offset()}", e)
            } catch (e: Exception) {
                problematicEvents.add(event)
                log.warn("Transformasjon av beskjed-event fra Kafka feilet, fullfører batch-en før pollig stoppes.", e)
            }
        }

        beskjedRepository.writeEventsToCache(successfullyTransformedEvents)

        kastExceptionHvisMislykkedeTransformasjoner(problematicEvents)
    }

    private fun kastExceptionHvisMislykkedeTransformasjoner(problematicEvents: MutableList<ConsumerRecord<Nokkel, Beskjed>>) {
        if (problematicEvents.isNotEmpty()) {
            val message = "En eller flere eventer kunne ikke transformeres"
            val exception = UntransformableRecordException(message)
            exception.addContext("antallMislykkedeTransformasjoner", problematicEvents.size)
            throw exception
        }
    }
}
