package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

interface EventId

data class EventIdString (
        val stringValue: String
): EventId

data class EventIdCustomUuid (
        val prefix: Char,
        val lowBits: Long,
        val highBits: Long
): EventId

data class EventIdUuid (
        val lowBits: Long,
        val highBits: Long
): EventId

data class EventIdUlid (
        val lowBits: Long,
        val highBits: Long
): EventId