package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

data class Fodselsnummer (
        val longValue: Long?,
        val stringValue: String?
) {

    companion object {

        fun fromString(fodselsnummerString: String): Fodselsnummer {
            val longValue = fodselsnummerString.toLongOrNull()

            val stringValue = if (longValue == null) {
                fodselsnummerString
            } else {
                null
            }

            return Fodselsnummer(longValue, stringValue)
        }

    }
}