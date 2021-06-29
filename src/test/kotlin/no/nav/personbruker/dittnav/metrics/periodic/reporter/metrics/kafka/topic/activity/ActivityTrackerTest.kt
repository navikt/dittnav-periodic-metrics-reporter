package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity

import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class ActivityTrackerTest {

    @Test
    fun `Finding events less than 25 percent of the time should be considered low activity`() {
        val activityTracker = ActivityTracker(historyLength = 20)

        activityTracker.eventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.LOW
    }

    @Test
    fun `Finding events between 25 and 75 percent of the time should be considered moderate activity`() {
        val activityTracker = ActivityTracker(historyLength = 20)

        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.MODERATE
    }

    @Test
    fun `Finding events more than 75 percent of the time should be considered high activity`() {
        val activityTracker = ActivityTracker(historyLength = 20)

        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.noEventsFound()

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.HIGH
    }

    @Test
    fun `Events older than historyLength should not affect level of recent activity`() {
        val activityTracker = ActivityTracker(historyLength = 5)

        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()
        activityTracker.eventsFound()

        activityTracker.eventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.LOW
    }

    @Test
    fun `Should track noEventsFound streak past historyLength`() {
        val activityTracker = ActivityTracker(historyLength = 5)

        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()

        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()
        activityTracker.noEventsFound()

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.LOW
        activityTracker.getInactivityStreak() `should be equal to` 10
    }

    @Test
    fun `Should report low activity if no events have been recorded yet`() {
        val activityTracker = ActivityTracker(historyLength = 20)

        activityTracker.getLevelOfRecentActivity() `should be equal to` ActivityLevel.LOW
    }
}
