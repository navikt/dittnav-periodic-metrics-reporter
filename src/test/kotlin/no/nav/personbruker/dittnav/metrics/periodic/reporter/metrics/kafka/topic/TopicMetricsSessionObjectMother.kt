package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier

object TopicMetricsSessionObjectMother {

    fun giveMeBeskjedSessionWithOneCountedEvent(): TopicMetricsSession {
        val beskjedSession = TopicMetricsSession(EventType.BESKJED)
        beskjedSession.countEvent(UniqueKafkaEventIdentifier("1", "sysBruker", "123"))
        return beskjedSession
    }

    fun giveMeBeskjedSessionWithTwoCountedEvents(): TopicMetricsSession {
        val beskjedSession = TopicMetricsSession(EventType.BESKJED)
        beskjedSession.countEvent(UniqueKafkaEventIdentifier("21", "sysBruker", "123"))
        beskjedSession.countEvent(UniqueKafkaEventIdentifier("22", "sysBruker", "123"))
        return beskjedSession
    }

    fun giveMeBeskjedInternSessionWithOneCountedEvent(): TopicMetricsSession {
        val beskjedSession = TopicMetricsSession(EventType.BESKJED_INTERN)
        beskjedSession.countEvent(UniqueKafkaEventIdentifier("1", "sysBruker", "123"))
        return beskjedSession
    }

    fun giveMeBeskjedInternSessionWithTwoCountedEvents(): TopicMetricsSession {
        val beskjedInternSession = TopicMetricsSession(EventType.BESKJED_INTERN)
        beskjedInternSession.countEvent(UniqueKafkaEventIdentifier("21", "sysBruker", "123"))
        beskjedInternSession.countEvent(UniqueKafkaEventIdentifier("22", "sysBruker", "123"))
        return beskjedInternSession
    }

    fun giveMeDoneSessionWithOneCountedEvent(): TopicMetricsSession {
        val doneSession = TopicMetricsSession(EventType.DONE)
        doneSession.countEvent(UniqueKafkaEventIdentifier("21", "sysBruker", "123"))
        return doneSession
    }

    fun giveMeDoneSessionWithThreeCountedEvent(): TopicMetricsSession {
        val doneSession = TopicMetricsSession(EventType.DONE)
        doneSession.countEvent(UniqueKafkaEventIdentifier("31", "sysBruker", "123"))
        doneSession.countEvent(UniqueKafkaEventIdentifier("32", "sysBruker", "123"))
        doneSession.countEvent(UniqueKafkaEventIdentifier("33", "sysBruker", "123"))
        return doneSession
    }

    fun giveMeDoneInternSessionWithOneCountedEvent(): TopicMetricsSession {
        val doneSession = TopicMetricsSession(EventType.DONE_INTERN)
        doneSession.countEvent(UniqueKafkaEventIdentifier("21", "sysBruker", "123"))
        return doneSession
    }

    fun giveMeDoneInternSessionWithThreeCountedEvent(): TopicMetricsSession {
        val doneSession = TopicMetricsSession(EventType.DONE_INTERN)
        doneSession.countEvent(UniqueKafkaEventIdentifier("31", "sysBruker", "123"))
        doneSession.countEvent(UniqueKafkaEventIdentifier("32", "sysBruker", "123"))
        doneSession.countEvent(UniqueKafkaEventIdentifier("33", "sysBruker", "123"))
        return doneSession
    }

    fun giveMeInnboksSessionWithOneCountedEvent(): TopicMetricsSession {
        val innboksSession = TopicMetricsSession(EventType.INNBOKS)
        innboksSession.countEvent(UniqueKafkaEventIdentifier("31", "sysBruker", "123"))
        return innboksSession
    }

    fun giveMeInnboksSessionWithFourCountedEvent(): TopicMetricsSession {
        val innboksSession = TopicMetricsSession(EventType.INNBOKS)
        innboksSession.countEvent(UniqueKafkaEventIdentifier("41", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("42", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("43", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("44", "sysBruker", "123"))
        return innboksSession
    }

    fun giveMeInnboksInternSessionWithOneCountedEvent(): TopicMetricsSession {
        val innboksSession = TopicMetricsSession(EventType.INNBOKS_INTERN)
        innboksSession.countEvent(UniqueKafkaEventIdentifier("31", "sysBruker", "123"))
        return innboksSession
    }

    fun giveMeInnboksInternSessionWithFourCountedEvent(): TopicMetricsSession {
        val innboksSession = TopicMetricsSession(EventType.INNBOKS_INTERN)
        innboksSession.countEvent(UniqueKafkaEventIdentifier("41", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("42", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("43", "sysBruker", "123"))
        innboksSession.countEvent(UniqueKafkaEventIdentifier("44", "sysBruker", "123"))
        return innboksSession
    }

    fun giveMeOppgaveSessionWithOneCountedEvent(): TopicMetricsSession {
        val oppgaveSession = TopicMetricsSession(EventType.OPPGAVE)
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("41", "sysBruker", "123"))
        return oppgaveSession
    }

    fun giveMeOppgaveSessionWithFiveCountedEvent(): TopicMetricsSession {
        val oppgaveSession = TopicMetricsSession(EventType.OPPGAVE)
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("51", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("52", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("53", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("54", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("55", "sysBruker", "123"))
        return oppgaveSession
    }

    fun giveMeOppgaveInternSessionWithOneCountedEvent(): TopicMetricsSession {
        val oppgaveSession = TopicMetricsSession(EventType.OPPGAVE_INTERN)
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("41", "sysBruker", "123"))
        return oppgaveSession
    }

    fun giveMeOppgaveInternSessionWithFiveCountedEvent(): TopicMetricsSession {
        val oppgaveSession = TopicMetricsSession(EventType.OPPGAVE_INTERN)
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("51", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("52", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("53", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("54", "sysBruker", "123"))
        oppgaveSession.countEvent(UniqueKafkaEventIdentifier("55", "sysBruker", "123"))
        return oppgaveSession
    }

    fun giveMeStatusoppdateringSessionWithOneCountedEvent(): TopicMetricsSession {
        val statusoppdateringSession = TopicMetricsSession(EventType.STATUSOPPDATERING)
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("61", "sysBruker", "123"))
        return statusoppdateringSession
    }

    fun giveMeStatusoppdateringSessionWithFiveCountedEvent(): TopicMetricsSession {
        val statusoppdateringSession = TopicMetricsSession(EventType.STATUSOPPDATERING)
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("61", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("62", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("63", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("64", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("65", "sysBruker", "123"))
        return statusoppdateringSession
    }

    fun giveMeStatusoppdateringInternSessionWithOneCountedEvent(): TopicMetricsSession {
        val statusoppdateringSession = TopicMetricsSession(EventType.STATUSOPPDATERING_INTERN)
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("61", "sysBruker", "123"))
        return statusoppdateringSession
    }

    fun giveMeStatusoppdateringInternSessionWithFiveCountedEvent(): TopicMetricsSession {
        val statusoppdateringSession = TopicMetricsSession(EventType.STATUSOPPDATERING_INTERN)
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("61", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("62", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("63", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("64", "sysBruker", "123"))
        statusoppdateringSession.countEvent(UniqueKafkaEventIdentifier("65", "sysBruker", "123"))
        return statusoppdateringSession
    }

    fun giveMeFeilresponsSessionWithOneCountedEvent(): TopicMetricsSession {
        val feilresponsSession = TopicMetricsSession(EventType.FEILRESPONS)
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("71", "sysBruker",))
        return feilresponsSession
    }

    fun giveMeFeilresponsSessionWithFiveCountedEvent(): TopicMetricsSession {
        val feilresponsSession = TopicMetricsSession(EventType.FEILRESPONS)
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("71", "sysBruker"))
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("72", "sysBruker"))
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("73", "sysBruker"))
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("74", "sysBruker"))
        feilresponsSession.countEvent(UniqueKafkaEventIdentifier.createEventWithoutValidFnr("75", "sysBruker"))
        return feilresponsSession
    }
}
