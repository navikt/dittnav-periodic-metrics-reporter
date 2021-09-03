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

data class EventIdSuffixedUlid (
        val suffix: Int,
        val lowBits: Long,
        val highBits: Long
): EventId {
    override fun getFormat() = EventIdFormat.SUFFIXED_ULID
}

data class EventIdNumeric(
        val value: Long
): EventId {
    override fun getFormat() = EventIdFormat.NUMERIC
}

data class EventIdFodselsnummer(
        val value: Long
): EventId {
    override fun getFormat() = EventIdFormat.FODSELSNUMMER
}

enum class EventIdFormat {
    PLAINTEXT, UUID, PREFIXED_UUID, ULID, SUFFIXED_ULID, NUMERIC, FODSELSNUMMER
}
