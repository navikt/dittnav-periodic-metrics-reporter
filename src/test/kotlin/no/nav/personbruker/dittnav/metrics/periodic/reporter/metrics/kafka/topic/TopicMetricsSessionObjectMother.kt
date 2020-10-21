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

}
