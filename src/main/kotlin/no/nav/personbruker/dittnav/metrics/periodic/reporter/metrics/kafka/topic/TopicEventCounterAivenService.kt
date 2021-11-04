package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions

class TopicEventCounterAivenService<K>(
    val beskjedCounter: TopicEventTypeCounter<K>,
    val innboksCounter: TopicEventTypeCounter<K>,
    val oppgaveCounter: TopicEventTypeCounter<K>,
    val statusoppdateringCounter: TopicEventTypeCounter<K>,
    val doneCounter: TopicEventTypeCounter<K>,
    val feilresponsCounter: TopicEventTypeCounter<K>
    ) {

        suspend fun countAllEventTypesAsync(): CountingMetricsSessions = coroutineScope {

            val beskjeder = if(isOtherEnvironmentThanProd()) {
                 beskjedCounter.countEventsAsync()
            } else {
                async { TopicMetricsSession(EventType.BESKJED_INTERN) }
            }

            val oppgaver = if(isOtherEnvironmentThanProd()) {
                oppgaveCounter.countEventsAsync()
            } else {
                async { TopicMetricsSession(EventType.OPPGAVE_INTERN) }
            }

            val done = if(isOtherEnvironmentThanProd()) {
                doneCounter.countEventsAsync()
            } else {
                async { TopicMetricsSession(EventType.DONE_INTERN) }
            }

            val feilrespons = if(isOtherEnvironmentThanProd()) {
                feilresponsCounter.countEventsAsync()
            } else {
                async { TopicMetricsSession(EventType.FEILRESPONS) }
            }

            val innboks = async { TopicMetricsSession(EventType.INNBOKS_INTERN) }
            val statusoppdateringer = async { TopicMetricsSession(EventType.STATUSOPPDATERING_INTERN) }

            val sessions = CountingMetricsSessions()

            sessions.put(EventType.BESKJED_INTERN, beskjeder.await())
            sessions.put(EventType.DONE_INTERN, done.await())
            sessions.put(EventType.INNBOKS_INTERN, innboks.await())
            sessions.put(EventType.OPPGAVE_INTERN, oppgaver.await())
            sessions.put(EventType.STATUSOPPDATERING_INTERN, statusoppdateringer.await())
            sessions.put(EventType.FEILRESPONS, feilrespons.await())

            sessions
        }
}
