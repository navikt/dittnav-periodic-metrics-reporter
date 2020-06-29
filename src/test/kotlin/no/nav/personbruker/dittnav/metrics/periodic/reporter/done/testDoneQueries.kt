package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import java.sql.Connection

fun Connection.deleteAllDone() =
        prepareStatement("""DELETE FROM DONE""")
                .use {it.execute()}
