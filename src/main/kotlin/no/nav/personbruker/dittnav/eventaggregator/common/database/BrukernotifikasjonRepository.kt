package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database

interface BrukernotifikasjonRepository<T> {

    suspend fun createInOneBatch(entities: List<T>): ListPersistActionResult<T>

    suspend fun createOneByOneToFilterOutTheProblematicEvents(entities: List<T>): ListPersistActionResult<T>

}
