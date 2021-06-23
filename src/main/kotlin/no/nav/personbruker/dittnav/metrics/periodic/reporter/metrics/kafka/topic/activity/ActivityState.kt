package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity

data class ActivityState(
        val recentActivityLevel: ActivityLevel,
        val activityPercentage: Int,
        val inactivityStreak: Int
)
