package no.nav.personbruker.dittnav.metrics.periodic.reporter.common

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistFailureReason

fun <T> successfulEvents(events: List<T>): ListPersistActionResult<T> {
    return events.map{ event ->
        event to PersistFailureReason.NO_ERROR
    }.let { entryList ->
        ListPersistActionResult.mapListOfIndividualResults(entryList)
    }
}

fun <T> emptyPersistResult(): ListPersistActionResult<T> = ListPersistActionResult.mapListOfIndividualResults(emptyList())