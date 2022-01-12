package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

interface Fodselsnummer

data class FodselsnummerPlainText(val stringValue: String): Fodselsnummer

data class FodselsnummerNumeric(val encodedValue: Int): Fodselsnummer
