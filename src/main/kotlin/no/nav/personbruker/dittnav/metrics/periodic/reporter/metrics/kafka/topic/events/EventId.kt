package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

data class EventId(
        val uuidUlid: UuidUlid? = null,
        val plainText: String? = null
)

data class UuidUlid (
        val lowBits: Long,
        val highBits: Long
)