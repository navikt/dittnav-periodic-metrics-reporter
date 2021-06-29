package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityLevel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityLevel.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityState
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class ActivityHealthDeciderTest {
    private val lowActivityThreshold = 100
    private val moderateActivityThreshold = 50
    private val highActivityThreshHold = 10

    private val decider = ActivityHealthDecider(
            lowActivityThreshold,
            moderateActivityThreshold,
            highActivityThreshHold
    )

    @Test
    fun `A sufficiently low inactivity streak should never be considered unhealthy`() {
        val streak = highActivityThreshHold - 5

        val lowState = ActivityState(LOW, streak)
        val moderateState = ActivityState(MODERATE, streak)
        val highState = ActivityState(HIGH, streak)

        decider.stateIsHealthy(lowState) `should be equal to` true
        decider.stateIsHealthy(moderateState) `should be equal to` true
        decider.stateIsHealthy(highState) `should be equal to` true
    }

    @Test
    fun `A sufficiently high inactivity streak should always be considered unhealthy`() {
        val streak = lowActivityThreshold + 5

        val lowState = ActivityState(LOW, streak)
        val moderateState = ActivityState(MODERATE, streak)
        val highState = ActivityState(HIGH, streak)

        decider.stateIsHealthy(lowState) `should be equal to` false
        decider.stateIsHealthy(moderateState) `should be equal to` false
        decider.stateIsHealthy(highState) `should be equal to` false
    }

    @Test
    fun `A streak just below the moderate threshold should only be considered unhealthy if activity is high`() {
        val streak = moderateActivityThreshold - 5

        val lowState = ActivityState(LOW, streak)
        val moderateState = ActivityState(MODERATE, streak)
        val highState = ActivityState(HIGH, streak)

        decider.stateIsHealthy(lowState) `should be equal to` true
        decider.stateIsHealthy(moderateState) `should be equal to` true
        decider.stateIsHealthy(highState) `should be equal to` false
    }

    @Test
    fun `A streak just above the moderate threshold should only be considered healthy if activity is low`() {
        val streak = moderateActivityThreshold + 5

        val lowState = ActivityState(LOW, streak)
        val moderateState = ActivityState(MODERATE, streak)
        val highState = ActivityState(HIGH, streak)

        decider.stateIsHealthy(lowState) `should be equal to` true
        decider.stateIsHealthy(moderateState) `should be equal to` false
        decider.stateIsHealthy(highState) `should be equal to` false
    }

}
