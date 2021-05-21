package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

enum class EventType(val eventType: String) {
    OPPGAVE("oppgave"),
    OPPGAVE_INTERN("oppgave_intern"),
    BESKJED("beskjed"),
    BESKJED_INTERN("beskjed_intern"),
    INNBOKS("innboks"),
    INNBOKS_INTERN("innboks_intern"),
    STATUSOPPDATERING("statusoppdatering"),
    STATUSOPPDATERING_INTERN("statusoppdatering_intern"),
    DONE("done"),
    DONE_INTERN("done_intern"),
    FEILRESPONS("feilrespons")
}
