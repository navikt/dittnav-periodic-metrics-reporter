package no.nav.personbruker.dittnav.metrics.periodic.reporter.common

import de.huxhorn.sulky.ulid.ULID

fun createULID(): String {
    return ULID().nextULID()
}
