package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.Oppgave
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.EventBatchProcessorService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.BrukernotifikasjonPersistingService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.FieldValidationException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.NokkelNullException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.UntransformableRecordException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.serializer.getNonNullKey
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType.OPPGAVE
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory

class OppgaveEventService(
        private val persistingService: BrukernotifikasjonPersistingService<no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.Oppgave>,
        private val metricsProbe: EventMetricsProbe
) : EventBatchProcessorService<Oppgave> {

    private val log = LoggerFactory.getLogger(OppgaveEventService::class.java)

    override suspend fun processEvents(events: ConsumerRecords<Nokkel, Oppgave>) {
        val successfullyTransformedEvents = mutableListOf<no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.Oppgave>()
        val problematicEvents = mutableListOf<ConsumerRecord<Nokkel, Oppgave>>()

        metricsProbe.runWithMetrics(eventType = OPPGAVE) {
            events.forEach { event ->
                try {
                    val internalEvent = OppgaveTransformer.toInternal(event.getNonNullKey(), event.value())
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
                    log.warn("Transformasjon av oppgave-event fra Kafka feilet.", e)
                }
            }

            val result = persistingService.writeEventsToCache(successfullyTransformedEvents)

            countDuplicateKeyEvents(result)
        }

        kastExceptionHvisMislykkedeTransformasjoner(problematicEvents)
    }

    fun EventMetricsSession.countDuplicateKeyEvents(result: ListPersistActionResult<no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.Oppgave>) {
        if (result.foundConflictingKeys()) {

            val constraintErrors = result.getConflictingEntities().size
            val totalEntities = result.getAllEntities().size

            result.getConflictingEntities()
                    .groupingBy { oppgave -> oppgave.systembruker }
                    .eachCount()
                    .forEach { (systembruker, duplicates) ->
                        countDuplicateEventKeysByProducer(systembruker, duplicates)
                    }

            log.warn("Traff $constraintErrors feil på duplikate eventId-er ved behandling av $totalEntities oppgave-eventer.")
        }
    }

    private fun kastExceptionHvisMislykkedeTransformasjoner(problematicEvents: MutableList<ConsumerRecord<Nokkel, Oppgave>>) {
        if (problematicEvents.isNotEmpty()) {
            val message = "En eller flere eventer kunne ikke transformeres"
            val exception = UntransformableRecordException(message)
            exception.addContext("antallMislykkedeTransformasjoner", problematicEvents.size)
            throw exception
        }
    }

}
