package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions

class AggregatorBatchUpdateException(message: String, cause: Throwable?) : RetriableDatabaseException(message, cause) {
    constructor(message: String) : this(message, null)
}
