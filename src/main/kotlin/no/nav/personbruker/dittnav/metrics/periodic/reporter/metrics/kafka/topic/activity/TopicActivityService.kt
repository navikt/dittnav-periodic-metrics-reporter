package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity

class TopicActivityService(historyLength: Int) {
    private val activityCounter = ActivityTracker(historyLength)

    fun reportEventsFound() {
        activityCounter.eventsFound()
    }

    fun reportNoEventsFound() {
        activityCounter.noEventsFound()
    }

    fun getActivityState(): ActivityState {
        return ActivityState(
                activityCounter.getLevelOfRecentActivity(),
                activityCounter.getInactivityStreak()
        )
    }
}
