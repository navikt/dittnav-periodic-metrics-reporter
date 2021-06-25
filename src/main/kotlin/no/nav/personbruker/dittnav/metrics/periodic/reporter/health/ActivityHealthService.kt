package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityLevel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.ActivityState
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import org.slf4j.LoggerFactory

class ActivityHealthService(
        val beskjedTopicActivityService: TopicActivityService,
        val oppgaveTopicActivityService: TopicActivityService,
        val innboksTopicActivityService: TopicActivityService,
        val doneTopicActivityService: TopicActivityService,
        val statusoppdateringTopicActivityService: TopicActivityService,
        val config: ActivityHealthServiceConfig
) {

    private val log = LoggerFactory.getLogger(ActivityHealthService::class.java)

    fun assertOnPremTopicActivityHealth(): Boolean {
        var healthy = true

        if (config.monitorOnPremBeskjedActivity && !assertServiceHealth(beskjedTopicActivityService, "on-prem beskjed")) {
            healthy = false
        }

        if (config.monitorOnPremOppgaveActivity && !assertServiceHealth(oppgaveTopicActivityService, "on-prem oppgave")) {
            healthy = false
        }

        if (config.monitorOnPremInnboksActivity && !assertServiceHealth(innboksTopicActivityService, "on-prem innboks")) {
            healthy = false
        }

        if (config.monitorOnPremDoneActivity && !assertServiceHealth(doneTopicActivityService, "on-prem done")) {
            healthy = false
        }

        if (config.monitorOnPremStatusOppdateringActivity && !assertServiceHealth(statusoppdateringTopicActivityService, "on-prem statusoppdatering")) {
            healthy = false
        }

        return healthy
    }

    private fun assertServiceHealth(service: TopicActivityService, topicSource: String): Boolean {
        val state = service.getActivityState()

        return if (stateIsHealthy(state)) {
            true
        } else {
            log.warn("On-prem topic counting for $topicSource looks unhealthy. Recent activity is ${state.recentActivityLevel} and counted zero events ${state.inactivityStreak} times in a row.")
            false
        }
    }

    private fun stateIsHealthy(state: ActivityState): Boolean {
        return when(state.recentActivityLevel) {
            ActivityLevel.LOW -> state.inactivityStreak < config.lowActivityStreakThreshold
            ActivityLevel.MODERATE -> state.inactivityStreak < config.moderateActivityStreakThreshold
            ActivityLevel.HIGH -> state.inactivityStreak < config.highActivityStreakThreshold
        }
    }
}

data class ActivityHealthServiceConfig(
        val lowActivityStreakThreshold: Int,
        val moderateActivityStreakThreshold: Int,
        val highActivityStreakThreshold: Int,
        val monitorOnPremBeskjedActivity: Boolean,
        val monitorOnPremOppgaveActivity: Boolean,
        val monitorOnPremInnboksActivity: Boolean,
        val monitorOnPremDoneActivity: Boolean,
        val monitorOnPremStatusOppdateringActivity: Boolean
)
