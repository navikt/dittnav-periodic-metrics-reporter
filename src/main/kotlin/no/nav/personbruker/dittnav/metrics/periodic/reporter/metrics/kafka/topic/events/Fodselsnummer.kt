package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

interface Fodselsnummer {
    companion object {

        fun fromString(fodselsnummerString: String): Fodselsnummer {
            val longValue = fodselsnummerString.toLongOrNull()

            return if (longValue != null) {
                FodselsnummerNumeric(longValue)
            } else {
                FodselsnummerPlainText(fodselsnummerString)
            }
        }
    }
}

data class FodselsnummerPlainText(val stringValue: String): Fodselsnummer

data class FodselsnummerNumeric(val longValue: Long): Fodselsnummer