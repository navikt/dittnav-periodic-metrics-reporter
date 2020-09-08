package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka

data class UniqueKafkaEventIdentifier(val eventId: String, val systembruker: String, val fodselsnummer: String) {

    companion object {

        private const val dummyFnr = "000"

        fun createInvalidEvent(): UniqueKafkaEventIdentifier {
            return UniqueKafkaEventIdentifier(
                "undeserializableEvent",
                "unknownProducer",
                dummyFnr
            )
        }

        fun createEventWithoutValidFnr(eventId: String, systembruker: String): UniqueKafkaEventIdentifier {
            return UniqueKafkaEventIdentifier(
                eventId,
                systembruker,
                dummyFnr
            )
        }
    }

    override fun toString(): String {
        return "UniqueKafkaEventIdentifier(eventId='$eventId', systembruker='$systembruker', fodselsnummer='***')"
    }

}
