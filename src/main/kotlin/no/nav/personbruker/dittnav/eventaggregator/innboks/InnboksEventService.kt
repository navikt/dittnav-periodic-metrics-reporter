package no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks

import no.nav.brukernotifikasjon.schemas.Innboks
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.EventBatchProcessorService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.BrukernotifikasjonPersistingService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.FieldValidationException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.NokkelNullException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.UntransformableRecordException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.serializer.getNonNullKey
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType.INNBOKS
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory

class InnboksEventService(
        private val persistingService: BrukernotifikasjonPersistingService<no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.Innboks>,
        private val metricsProbe: EventMetricsProbe
) : EventBatchProcessorService<Innboks> {

    private val log = LoggerFactory.getLogger(InnboksEventService::class.java)

    override suspend fun processEvents(events: ConsumerRecords<Nokkel, Innboks>) {
        val successfullyTransformedEvents = mutableListOf<no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.Innboks>()
        val problematicEvents = mutableListOf<ConsumerRecord<Nokkel, Innboks>>()

        metricsProbe.runWithMetrics(eventType = INNBOKS) {
            events.forEach { event ->
                try {
                    val internalEvent = InnboksTransformer.toInternal(event.getNonNullKey(), event.value())
                    successfullyTransformedEvents.add(internalEvent)
                    countSuccessfulEventForProducer(event.systembruker)

                } catch (e: NokkelNullException) {
                    countFailedEventForProducer("NoProducerSpecified")
                    log.warn("Eventet manglet nøkkel. Topic: ${event.topic()}, Partition: ${event.partition()}, Offset: ${event.offset()}", e)

                } catch (fve: FieldValidationException) {
                    countFailedEventForProducer(event.systembruker)
                    val eventId = event.getNonNullKey().getEventId()
                    val systembruker = event.getNonNullKey().getSystembruker()
                    val msg = "Eventet kan ikke brukes fordi det inneholder valideringsfeil, eventet vil bli forkastet. EventId: $eventId, systembruker: $systembruker, ${fve.toString()}"
                    log.warn(msg, fve)

                } catch (e: Exception) {
                    countFailedEventForProducer(event.systembruker)
                    problematicEvents.add(event)
                    log.warn("Transformasjon av innboks-event fra Kafka feilet, fullfører batch-en før pollig stoppes.", e)
                }
            }

            val result = persistingService.writeEventsToCache(successfullyTransformedEvents)

            countDuplicateKeyEvents(result)
        }

        kastExceptionHvisMislykkedeTransformasjoner(problematicEvents)
    }

    private fun EventMetricsSession.countDuplicateKeyEvents(result: ListPersistActionResult<no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.Innboks>) {
        if (result.foundConflictingKeys()) {

            val constraintErrors = result.getConflictingEntities().size
            val totalEntities = result.getAllEntities().size

            result.getConflictingEntities()
                    .groupingBy { innboks -> innboks.systembruker }
                    .eachCount()
                    .forEach { (systembruker, duplicates) ->
                        countDuplicateEventKeysByProducer(systembruker, duplicates)
                    }

            log.warn("Traff $constraintErrors feil på duplikate eventId-er ved behandling av $totalEntities innboks-eventer.")
        }
    }

    private fun kastExceptionHvisMislykkedeTransformasjoner(problematicEvents: MutableList<ConsumerRecord<Nokkel, Innboks>>) {
        if (problematicEvents.isNotEmpty()) {
            val message = "En eller flere eventer kunne ikke transformeres"
            val exception = UntransformableRecordException(message)
            exception.addContext("antallMislykkedeTransformasjoner", problematicEvents.size)
            throw exception
        }
    }
}