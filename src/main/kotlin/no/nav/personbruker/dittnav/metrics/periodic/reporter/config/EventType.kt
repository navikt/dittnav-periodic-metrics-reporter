package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

enum class EventType(val eventType: String) {
    OPPGAVE("oppgave"),
    BESKJED("beskjed"),
    INNBOKS("innboks"),
    STATUSOPPDATERING("statusoppdatering"),
    DONE("done")
}
