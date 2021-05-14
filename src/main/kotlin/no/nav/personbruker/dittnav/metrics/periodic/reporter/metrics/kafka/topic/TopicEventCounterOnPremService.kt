package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions

class TopicEventCounterOnPremService<K>(
        val beskjedCounter: TopicEventTypeCounter<K>,
        val innboksCounter: TopicEventTypeCounter<K>,
        val oppgaveCounter: TopicEventTypeCounter<K>,
        val statusoppdateringCounter: TopicEventTypeCounter<K>,
        val doneCounter: TopicEventTypeCounter<K>
) {

    suspend fun countAllEventTypesAsync(): CountingMetricsSessions = coroutineScope {

        val beskjeder = beskjedCounter.countEventsAsync()
        val oppgaver = oppgaveCounter.countEventsAsync()
        val done = doneCounter.countEventsAsync()

        val innboks = if (isOtherEnvironmentThanProd()) {
            innboksCounter.countEventsAsync()
        } else {
            async { TopicMetricsSession(EventType.INNBOKS) }
        }

        val statusoppdateringer = if(isOtherEnvironmentThanProd()) {
            statusoppdateringCounter.countEventsAsync()
        } else {
            async { TopicMetricsSession(EventType.STATUSOPPDATERING) }
        }

        val sessions = CountingMetricsSessions()

        sessions.put(EventType.BESKJED, beskjeder.await())
        sessions.put(EventType.DONE, done.await())
        sessions.put(EventType.INNBOKS, innboks.await())
        sessions.put(EventType.OPPGAVE, oppgaver.await())
        sessions.put(EventType.STATUSOPPDATERING, statusoppdateringer.await())

        sessions
    }
}
