package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import no.nav.brukernotifikasjon.schemas.Done
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.EventBatchProcessorService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.FieldValidationException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.NokkelNullException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.UntransformableRecordException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.serializer.getNonNullKey
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType.DONE
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.EventMetricsSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DoneEventService(
        private val donePersistingService: DonePersistingService,
        private val eventMetricsProbe: EventMetricsProbe
) : EventBatchProcessorService<Done> {

    private val log: Logger = LoggerFactory.getLogger(DoneEventService::class.java)

    override suspend fun processEvents(events: ConsumerRecords<Nokkel, Done>) {
        val successfullyTransformedEvents = mutableListOf<no.nav.personbruker.dittnav.metrics.periodic.reporter.done.Done>()
        val problematicEvents = mutableListOf<ConsumerRecord<Nokkel, Done>>()

        eventMetricsProbe.runWithMetrics(eventType = DONE) {

            events.forEach { event ->
                try {
                    val internalEvent = DoneTransformer.toInternal(event.getNonNullKey(), event.value())
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
                    log.warn("Transformasjon av done-event fra Kafka feilet.", e)
                }
            }
            val groupedDoneEvents = groupDoneEventsByAssociatedEventType(successfullyTransformedEvents)
            donePersistingService.writeDoneEventsForBeskjedToCache(groupedDoneEvents.foundBeskjed)
            donePersistingService.writeDoneEventsForOppgaveToCache(groupedDoneEvents.foundOppgave)
            donePersistingService.writeDoneEventsForInnboksToCache(groupedDoneEvents.foundInnboks)
            val writeEventsToCacheResult= donePersistingService.writeEventsToCache(groupedDoneEvents.notFoundEvents)
            countDuplicateKeyEvents(writeEventsToCacheResult)
        }

        kastExceptionHvisMislykkedeTransformasjoner(problematicEvents)
    }

    private suspend fun groupDoneEventsByAssociatedEventType(successfullyTransformedEvents: List<no.nav.personbruker.dittnav.metrics.periodic.reporter.done.Done>): DoneBatchProcessor {
        val eventIds = successfullyTransformedEvents.map { it.eventId }.distinct()
        val aktiveBrukernotifikasjoner = donePersistingService.fetchBrukernotifikasjonerFromViewForEventIds(eventIds)
        val batch = DoneBatchProcessor(aktiveBrukernotifikasjoner)
        batch.process(successfullyTransformedEvents)
        return batch
    }

    private fun EventMetricsSession.countDuplicateKeyEvents(result: ListPersistActionResult<no.nav.personbruker.dittnav.metrics.periodic.reporter.done.Done>) {
        if (result.foundConflictingKeys()) {

            val constraintErrors = result.getConflictingEntities().size
            val totalEntities = result.getAllEntities().size

            result.getConflictingEntities()
                    .groupingBy { done -> done.systembruker }
                    .eachCount()
                    .forEach { (systembruker, duplicates) ->
                        countDuplicateEventKeysByProducer(systembruker, duplicates)
                    }

            log.warn("Traff $constraintErrors feil på duplikate eventId-er ved behandling av $totalEntities done-eventer.")
        }
    }

    private fun kastExceptionHvisMislykkedeTransformasjoner(problematicEvents: MutableList<ConsumerRecord<Nokkel, Done>>) {
        if (problematicEvents.isNotEmpty()) {
            val message = "En eller flere eventer kunne ikke transformeres"
            val exception = UntransformableRecordException(message)
            exception.addContext("antallMislykkedeTransformasjoner", problematicEvents.size)
            throw exception
        }
    }
}
