package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions

open class RetriableDatabaseException(message: String, cause: Throwable?) : AbstractPersonbrukerException(message, cause) {
    constructor(message: String) : this(message, null)
}
