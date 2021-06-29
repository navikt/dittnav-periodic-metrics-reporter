package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityLevel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityState
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import org.slf4j.LoggerFactory

class ActivityHealthService(
        private val beskjedTopicActivityService: TopicActivityService,
        private val oppgaveTopicActivityService: TopicActivityService,
        private val innboksTopicActivityService: TopicActivityService,
        private val doneTopicActivityService: TopicActivityService,
        private val statusoppdateringTopicActivityService: TopicActivityService,
        private val activityHealthDecider: ActivityHealthDecider,
        private val monitoringToggles: ActivityMonitoringToggles
) {

    private val log = LoggerFactory.getLogger(ActivityHealthService::class.java)

    fun assertOnPremTopicActivityHealth(): Boolean {
        var healthy = true

        if (monitoringToggles.monitorOnPremBeskjedActivity && !assertServiceHealth(beskjedTopicActivityService, "on-prem beskjed")) {
            healthy = false
        }

        if (monitoringToggles.monitorOnPremOppgaveActivity && !assertServiceHealth(oppgaveTopicActivityService, "on-prem oppgave")) {
            healthy = false
        }

        if (monitoringToggles.monitorOnPremInnboksActivity && !assertServiceHealth(innboksTopicActivityService, "on-prem innboks")) {
            healthy = false
        }

        if (monitoringToggles.monitorOnPremDoneActivity && !assertServiceHealth(doneTopicActivityService, "on-prem done")) {
            healthy = false
        }

        if (monitoringToggles.monitorOnPremStatusoppdateringActivity && !assertServiceHealth(statusoppdateringTopicActivityService, "on-prem statusoppdatering")) {
            healthy = false
        }

        return healthy
    }

    private fun assertServiceHealth(service: TopicActivityService, topicSource: String): Boolean {
        val state = service.getActivityState()

        return if (activityHealthDecider.stateIsHealthy(state)) {
            true
        } else {
            log.warn("On-prem topic counting for $topicSource looks unhealthy. Recent activity is ${state.recentActivityLevel} and counted zero events ${state.inactivityStreak} times in a row.")
            false
        }
    }
}

class ActivityHealthDecider(
        private val lowActivityStreakThreshold: Int,
        private val moderateActivityStreakThreshold: Int,
        private val highActivityStreakThreshold: Int

) {
    fun stateIsHealthy(state: ActivityState): Boolean {
        return when(state.recentActivityLevel) {
            ActivityLevel.HIGH -> state.inactivityStreak < highActivityStreakThreshold
            ActivityLevel.MODERATE -> state.inactivityStreak < moderateActivityStreakThreshold
            ActivityLevel.LOW -> state.inactivityStreak < lowActivityStreakThreshold
        }
    }
}

data class ActivityMonitoringToggles(
        val monitorOnPremBeskjedActivity: Boolean,
        val monitorOnPremOppgaveActivity: Boolean,
        val monitorOnPremInnboksActivity: Boolean,
        val monitorOnPremDoneActivity: Boolean,
        val monitorOnPremStatusoppdateringActivity: Boolean
)
