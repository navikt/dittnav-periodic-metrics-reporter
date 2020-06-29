package no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel

import no.nav.brukernotifikasjon.schemas.Nokkel

fun createNokkel(eventId: Int): Nokkel = Nokkel("dummySystembruker", eventId.toString())
