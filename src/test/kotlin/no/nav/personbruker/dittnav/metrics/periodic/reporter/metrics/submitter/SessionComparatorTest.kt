package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessionsObjectMother
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

internal class SessionComparatorTest {

    @Test
    fun `Should return all sessions from both sources, when they have the same session types`() {
        val allTopicSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypesExceptForInnboks()
        val allDbSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypesExceptForInnboks()

        val comparator = SessionComparator(allTopicSessions, allDbSessions)

        comparator.eventTypesWithSessionFromBothSources().size `should be equal to` allTopicSessions.getEventTypesWithSession().size
        comparator.eventTypesWithSessionFromBothSources().size `should be equal to` allDbSessions.getEventTypesWithSession().size
    }

    @Test
    fun `Should only return sessions present in both sources, if one topic session is missing`() {
        val oneTopicSessionMissing = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypesExceptForInnboks()
        val allDbSessions = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypes()

        val comparator = SessionComparator(oneTopicSessionMissing, allDbSessions)

        comparator.eventTypesWithSessionFromBothSources().size `should be equal to` oneTopicSessionMissing.getEventTypesWithSession().size
    }

    @Test
    fun `Should only return sessions present in both sources, if one database session is missing`() {
        val oneDbSessionMissing = CountingMetricsSessionsObjectMother.giveMeDatabaseSessionsForAllEventTypesExceptForInnboks()
        val allTopicSessions = CountingMetricsSessionsObjectMother.giveMeTopicSessionsForAllEventTypes()

        val comparator = SessionComparator(allTopicSessions, oneDbSessionMissing)

        comparator.eventTypesWithSessionFromBothSources().size `should be equal to` oneDbSessionMissing.getEventTypesWithSession().size
    }
}
