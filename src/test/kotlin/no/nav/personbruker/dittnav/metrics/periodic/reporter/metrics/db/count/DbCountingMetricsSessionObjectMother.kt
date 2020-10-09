package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType

object DbCountingMetricsSessionObjectMother {

    fun giveMeBeskjedSessionWithOneCountedEvent(): DbCountingMetricsSession {
        val beskjedSession = DbCountingMetricsSession(EventType.BESKJED)
        beskjedSession.addEventsByProducer(mapOf("produsent1" to 1))
        return beskjedSession
    }

    fun giveMeDoneSessionWithTwoCountedEvents(): DbCountingMetricsSession {
        val doneSession = DbCountingMetricsSession(EventType.DONE)
        doneSession.addEventsByProducer(mapOf("produsent2" to 21))
        doneSession.addEventsByProducer(mapOf("produsent2" to 22))
        return doneSession
    }

    fun giveMeInnboksSessionWithThreeCountedEvents(): DbCountingMetricsSession {
        val innboksSession = DbCountingMetricsSession(EventType.INNBOKS)
        innboksSession.addEventsByProducer(mapOf("produsent3" to 31))
        innboksSession.addEventsByProducer(mapOf("produsent3" to 32))
        innboksSession.addEventsByProducer(mapOf("produsent3" to 33))
        return innboksSession
    }

    fun giveMeOppgaveSessionWithFourCountedEvents(): DbCountingMetricsSession {
        val oppgaveSession = DbCountingMetricsSession(EventType.OPPGAVE)
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 41))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 42))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 43))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 44))
        return oppgaveSession
    }

}
