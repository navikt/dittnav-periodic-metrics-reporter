package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity

class ActivityTracker(historyLength: Int) {
    private val recentCountHistory = FixedLengthBooleanQueue(historyLength)

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
        val currentHistoryLength = recentCountHistory.getCurrentLength()

        return if (currentHistoryLength == 0) {
            0
        } else {
            val percentage = (recentCountHistory.countTrueEntries() * 100.0) / currentHistoryLength

            (percentage + 0.5).toInt()
        }
    }

    fun getInactivityStreak(): Int = inactivityStreak
}

private class FixedLengthBooleanQueue(private val maxLength: Int) {
    private var entries: Int = 0

    private var cursor: Int = 0

    private val array = BooleanArray(maxLength) { false }

    fun add(entry: Boolean) {
        array[cursor] = entry

        entries = (entries + 1).coerceAtMost(maxLength)
        cursor = (cursor + 1) % maxLength
    }

    fun countTrueEntries(): Int {
        return array.sumBy { entry ->
            if (entry) 1 else 0
        }
    }

    fun getCurrentLength() = entries
}
