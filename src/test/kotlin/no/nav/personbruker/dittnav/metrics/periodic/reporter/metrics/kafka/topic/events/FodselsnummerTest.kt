package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.FodselsnummerParser
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should be in`
import org.amshove.kluent.`should be instance of`
import org.junit.jupiter.api.Test

internal class FodselsnummerTest {
    @Test
    fun `Should use numeric variable to store fodselsnummer if possible`() {
        val fodselsnummerText = "12345612345"
        val expected = 123456123

        val fodselsnummer = FodselsnummerParser.parse(fodselsnummerText)

        fodselsnummer `should be instance of` FodselsnummerNumeric::class
        (fodselsnummer as FodselsnummerNumeric).encodedValue `should be equal to` expected
    }

    @Test
    fun `Should use string variable to store fodselsnummer it is not possible to parse as number`() {
        val fodselsnummerText = "abc123"

        val fodselsnummer = FodselsnummerParser.parse(fodselsnummerText)

        fodselsnummer `should be instance of` FodselsnummerPlainText::class
        (fodselsnummer as FodselsnummerPlainText).stringValue `should be equal to` fodselsnummerText
    }

    @Test
    fun `Should work as expected in sets`() {
        val fodselsnummerNumeric = FodselsnummerParser.parse("123")
        val fodselsnummerNumericDuplicate = FodselsnummerParser.parse("123")
        val fodselsnummerString = FodselsnummerParser.parse("abc")
        val fodselsnummerStringDuplicate = FodselsnummerParser.parse("abc")

        val set = HashSet<Fodselsnummer>()

        set.add(fodselsnummerNumeric)
        set.add(fodselsnummerNumericDuplicate)
        set.add(fodselsnummerString)
        set.add(fodselsnummerStringDuplicate)

        set.size `should be equal to` 2
        fodselsnummerNumeric `should be in` set
        fodselsnummerString `should be in` set
    }
}
