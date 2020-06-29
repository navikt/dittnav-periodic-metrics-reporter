package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka

data class UniqueKafkaEventIdentifier(val eventId: String, val systembruker: String, val fodselsnummer: String)
