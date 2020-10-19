package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsSessionObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsSessionObjectMother

object CountingMetricsSessionsObjectMother {

    fun giveMeDatabaseSessionsForAllEventTypes(): CountingMetricsSessions {
        return CountingMetricsSessions().apply {
            put(EventType.BESKJED, DbCountingMetricsSessionObjectMother.giveMeBeskjedSessionWithOneCountedEvent())
            put(EventType.DONE, DbCountingMetricsSessionObjectMother.giveMeDoneSessionWithTwoCountedEvents())
            put(EventType.INNBOKS, DbCountingMetricsSessionObjectMother.giveMeInnboksSessionWithThreeCountedEvents())
            put(EventType.OPPGAVE, DbCountingMetricsSessionObjectMother.giveMeOppgaveSessionWithFourCountedEvents())
        }
    }

    fun giveMeDatabaseSessionsForAllEventTypesExceptForInnboks(): CountingMetricsSessions {
        return CountingMetricsSessions().apply {
            put(EventType.BESKJED, DbCountingMetricsSessionObjectMother.giveMeBeskjedSessionWithOneCountedEvent())
            put(EventType.DONE, DbCountingMetricsSessionObjectMother.giveMeDoneSessionWithTwoCountedEvents())
            put(EventType.OPPGAVE, DbCountingMetricsSessionObjectMother.giveMeOppgaveSessionWithFourCountedEvents())
        }
    }

    fun giveMeTopicSessionsForAllEventTypes(): CountingMetricsSessions {
        return CountingMetricsSessions().apply {
            put(EventType.BESKJED, TopicMetricsSessionObjectMother.giveMeBeskjedSessionWithTwoCountedEvents())
            put(EventType.DONE, TopicMetricsSessionObjectMother.giveMeDoneSessionWithThreeCountedEvent())
            put(EventType.INNBOKS, TopicMetricsSessionObjectMother.giveMeInnboksSessionWithFourCountedEvent())
            put(EventType.OPPGAVE, TopicMetricsSessionObjectMother.giveMeOppgaveSessionWithFiveCountedEvent())
        }
    }

    fun giveMeTopicSessionsForAllEventTypesExceptForInnboks(): CountingMetricsSessions {
        return CountingMetricsSessions().apply {
            put(EventType.BESKJED, TopicMetricsSessionObjectMother.giveMeBeskjedSessionWithTwoCountedEvents())
            put(EventType.DONE, TopicMetricsSessionObjectMother.giveMeDoneSessionWithThreeCountedEvent())
            put(EventType.OPPGAVE, TopicMetricsSessionObjectMother.giveMeOppgaveSessionWithFiveCountedEvent())
        }
    }

    fun giveMeTopicSessionsWithSingleEventForAllEventTypes(): CountingMetricsSessions {
        return CountingMetricsSessions().apply {
            put(EventType.BESKJED, TopicMetricsSessionObjectMother.giveMeBeskjedSessionWithOneCountedEvent())
            put(EventType.DONE, TopicMetricsSessionObjectMother.giveMeDoneSessionWithOneCountedEvent())
            put(EventType.INNBOKS, TopicMetricsSessionObjectMother.giveMeInnboksSessionWithOneCountedEvent())
            put(EventType.OPPGAVE, TopicMetricsSessionObjectMother.giveMeOppgaveSessionWithOneCountedEvent())
        }
    }

}
