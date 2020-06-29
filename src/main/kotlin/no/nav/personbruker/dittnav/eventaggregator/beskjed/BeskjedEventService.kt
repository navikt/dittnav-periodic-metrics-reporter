package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.EventBatchProcessorService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.BrukernotifikasjonPersistingService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.FieldValidationException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.NokkelNullException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.UntransformableRecordException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.serializer.getNonNullKey
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType.BESKJED
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BeskjedEventService(
        private val persistingService: BrukernotifikasjonPersistingService<no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.Beskjed>,
        private val metricsProbe: EventMetricsProbe
) : EventBatchProcessorService<Beskjed> {

    private val log: Logger = LoggerFactory.getLogger(BeskjedEventService::class.java)

    override suspend fun processEvents(events: ConsumerRecords<Nokkel, Beskjed>) {
        val successfullyTransformedEvents = mutableListOf<no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.Beskjed>()
        val problematicEvents = mutableListOf<ConsumerRecord<Nokkel, Beskjed>>()

        metricsProbe.runWithMetrics(eventType = BESKJED) {
            events.forEach { event ->
                try {
                    val internalEvent = BeskjedTransformer.toInternal(event.getNonNullKey(), event.value())
                    successfullyTransformedEvents.add(internalEvent)
                    countSuccessfulEventForProducer(event.systembruker)

                } catch (nne: NokkelNullException) {
                    countFailedEventForProducer("NoProducerSpecified")
                    log.warn("Eventet manglet nøkkel. Topic: ${event.topic()}, Partition: ${event.partition()}, Offset: ${event.offset()}", nne)

                } catch (fve: FieldValidationException) {
                    countFailedEventForProducer(event.systembruker)
                    val eventId = event.getNonNullKey().getEventId()
                    val systembruker = event.getNonNullKey().getSystembruker()
                    val msg = "Klarte ikke transformere eventet pga en valideringsfeil. EventId: $eventId, systembruker: $systembruker, ${fve.toString()}"
                    log.warn(msg, fve)

                } catch (e: Exception) {
                    countFailedEventForProducer(event.systembruker)
                    problematicEvents.add(event)
                    log.warn("Transformasjon av beskjed-event fra Kafka feilet, fullfører batch-en før pollig stoppes.", e)
                }
            }

            val result = persistingService.writeEventsToCache(successfullyTransformedEvents)

            countDuplicateKeyEvents(result)
        }

        kastExceptionHvisMislykkedeTransformasjoner(problematicEvents)
    }

    private fun EventMetricsSession.countDuplicateKeyEvents(result: ListPersistActionResult<no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.Beskjed>) {
        if (result.foundConflictingKeys()) {

            val constraintErrors = result.getConflictingEntities().size
            val totalEntities = result.getAllEntities().size

            result.getConflictingEntities()
                    .groupingBy { beskjed -> beskjed.systembruker }
                    .eachCount()
                    .forEach { (systembruker, duplicates) ->
                        countDuplicateEventKeysByProducer(systembruker, duplicates)
                    }

            log.warn("Traff $constraintErrors feil på duplikate eventId-er ved behandling av $totalEntities beskjed-eventer.")
        }
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
