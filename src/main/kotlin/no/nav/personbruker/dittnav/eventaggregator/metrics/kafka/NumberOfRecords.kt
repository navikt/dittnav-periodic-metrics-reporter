package no.nav.personbruker.dittnav.eventaggregator.metrics.kafka

data class NumberOfRecords(val beskjed: Long = 0,
                           val innboks: Long = 0,
                           val oppgaver: Long = 0,
                           val done: Long = 0) {

    private val totalt = beskjed + innboks + oppgaver + done

    override fun toString(): String {
        return "NumberOfRecords(beskjed=$beskjed, innboks=$innboks, oppgaver=$oppgaver, done=$done, totalt=$totalt)"
    }

}
