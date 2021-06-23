package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity

class ActivityTracker(private val historyLength: Int) {
    private val recentCountHistory = FixedSizeBooleanQueue(historyLength)

    private var inactivityStreak = 0

    fun eventsFound() {
        inactivityStreak = 0
        recentCountHistory.add(true)
    }

    fun noEventsFound() {
        inactivityStreak++
        recentCountHistory.add(false)
    }

    fun getLevelOfRecentActivity(): ActivityLevel {
        val percentage = getActivityPercentage()
        return when {
            percentage < 25 -> ActivityLevel.LOW
            percentage > 75 -> ActivityLevel.HIGH
            else -> ActivityLevel.MODERATE
        }
    }

    fun getActivityPercentage(): Int {
        val percentage = (recentCountHistory.countTrueEntries() * 100.0) / historyLength

        return (percentage + 0.5).toInt()
    }

    fun getInactivityStreak(): Int = inactivityStreak
}

private class FixedSizeBooleanQueue(private val size: Int) {
    private var entries: Int = 0

    private var cursor: Int = 0

    private val array = BooleanArray(size) { true }

    fun add(entry: Boolean) {
        array[cursor] = entry

        entries++
        cursor = (cursor + 1) % size
    }

    fun countTrueEntries(): Int {
        return array.sumBy { entry ->
            if (entry) 1 else 0
        }
    }
}
