package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

interface EventId {
    fun getFormat(): EventIdFormat
}

data class EventIdPlainText (
        val stringValue: String
): EventId {
    override fun getFormat() = EventIdFormat.PLAINTEXT
}

data class EventIdUuid (
        val lowBits: Long,
        val highBits: Long
): EventId {
    override fun getFormat() = EventIdFormat.UUID
}

data class EventIdPrefixedUuid (
        val prefix: Char,
        val lowBits: Long,
        val highBits: Long
): EventId {
    override fun getFormat() = EventIdFormat.PREFIXED_UUID
}

data class EventIdUlid (
        val lowBits: Long,
        val highBits: Long
): EventId {
    override fun getFormat() = EventIdFormat.ULID
}

enum class EventIdFormat {
    PLAINTEXT, UUID, PREFIXED_UUID, ULID
}
