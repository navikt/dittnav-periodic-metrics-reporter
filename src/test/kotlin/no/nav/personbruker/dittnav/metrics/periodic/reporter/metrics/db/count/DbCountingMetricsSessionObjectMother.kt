package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType

object DbCountingMetricsSessionObjectMother {

    fun giveMeBeskjedSessionWithOneCountedEvent(): DbCountingMetricsSession {
        val beskjedSession = DbCountingMetricsSession(EventType.BESKJED)
        beskjedSession.addEventsByProducer(mapOf("produsent1" to 1))
        return beskjedSession
    }

    fun giveMeBeskjedInternSessionWithOneCountedEvent(): DbCountingMetricsSession {
        val beskjedInternSession = DbCountingMetricsSession(EventType.BESKJED_INTERN)
        beskjedInternSession.addEventsByProducer(mapOf("produsent1" to 1))
        return beskjedInternSession
    }

    fun giveMeDoneSessionWithTwoCountedEvents(): DbCountingMetricsSession {
        val doneSession = DbCountingMetricsSession(EventType.DONE)
        doneSession.addEventsByProducer(mapOf("produsent2" to 21))
        doneSession.addEventsByProducer(mapOf("produsent2" to 22))
        return doneSession
    }

    fun giveMeDoneInternSessionWithTwoCountedEvents(): DbCountingMetricsSession {
        val doneInternSession = DbCountingMetricsSession(EventType.DONE_INTERN)
        doneInternSession.addEventsByProducer(mapOf("produsent2" to 21))
        doneInternSession.addEventsByProducer(mapOf("produsent2" to 22))
        return doneInternSession
    }

    fun giveMeInnboksSessionWithThreeCountedEvents(): DbCountingMetricsSession {
        val innboksSession = DbCountingMetricsSession(EventType.INNBOKS)
        innboksSession.addEventsByProducer(mapOf("produsent3" to 31))
        innboksSession.addEventsByProducer(mapOf("produsent3" to 32))
        innboksSession.addEventsByProducer(mapOf("produsent3" to 33))
        return innboksSession
    }

    fun giveMeInnboksInternSessionWithThreeCountedEvents(): DbCountingMetricsSession {
        val innboksInternSession = DbCountingMetricsSession(EventType.INNBOKS_INTERN)
        innboksInternSession.addEventsByProducer(mapOf("produsent3" to 31))
        innboksInternSession.addEventsByProducer(mapOf("produsent3" to 32))
        innboksInternSession.addEventsByProducer(mapOf("produsent3" to 33))
        return innboksInternSession
    }

    fun giveMeOppgaveSessionWithFourCountedEvents(): DbCountingMetricsSession {
        val oppgaveSession = DbCountingMetricsSession(EventType.OPPGAVE)
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 41))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 42))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 43))
        oppgaveSession.addEventsByProducer(mapOf("produsent4" to 44))
        return oppgaveSession
    }

    fun giveMeOppgaveInternSessionWithFourCountedEvents(): DbCountingMetricsSession {
        val oppgaveInternSession = DbCountingMetricsSession(EventType.OPPGAVE_INTERN)
        oppgaveInternSession.addEventsByProducer(mapOf("produsent4" to 41))
        oppgaveInternSession.addEventsByProducer(mapOf("produsent4" to 42))
        oppgaveInternSession.addEventsByProducer(mapOf("produsent4" to 43))
        oppgaveInternSession.addEventsByProducer(mapOf("produsent4" to 44))
        return oppgaveInternSession
    }

    fun giveMeStatusoppdateringSessionWithFourCountedEvents(): DbCountingMetricsSession {
        val statusoppdateringSession = DbCountingMetricsSession(EventType.STATUSOPPDATERING)
        statusoppdateringSession.addEventsByProducer(mapOf("produsent5" to 51))
        statusoppdateringSession.addEventsByProducer(mapOf("produsent5" to 52))
        statusoppdateringSession.addEventsByProducer(mapOf("produsent5" to 53))
        statusoppdateringSession.addEventsByProducer(mapOf("produsent5" to 54))
        return statusoppdateringSession
    }

    fun giveMeStatusoppdateringInternSessionWithFourCountedEvents(): DbCountingMetricsSession {
        val statusoppdateringInternSession = DbCountingMetricsSession(EventType.STATUSOPPDATERING_INTERN)
        statusoppdateringInternSession.addEventsByProducer(mapOf("produsent5" to 51))
        statusoppdateringInternSession.addEventsByProducer(mapOf("produsent5" to 52))
        statusoppdateringInternSession.addEventsByProducer(mapOf("produsent5" to 53))
        statusoppdateringInternSession.addEventsByProducer(mapOf("produsent5" to 54))
        return statusoppdateringInternSession
    }

}
