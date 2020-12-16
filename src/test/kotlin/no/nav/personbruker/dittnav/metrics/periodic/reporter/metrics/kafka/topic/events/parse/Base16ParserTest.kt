package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should throw`
import org.amshove.kluent.invoking
import org.junit.jupiter.api.Test
import java.lang.RuntimeException

internal class Base16ParserTest {
    @Test
    fun `Can parse base-16 minimum from 128-bit wide input string`() {
        val encodedString = "00000000-0000-0000-0000-000000000000".stripHyphen()

        val expectedLow = 0L
        val expectedHigh = 0L

        val result = Base16Parser.parseNumericValueFromBase16(encodedString)

        result[0] `should be equal to` expectedLow
        result[1] `should be equal to` expectedHigh
    }
    @Test
    fun `Can parse base-16 maximum from 128-bit wide input value`() {
        val encodedString = "ffffffff-ffff-ffff-ffff-ffffffffffff".stripHyphen()

        val expectedLow = -1L
        val expectedHigh = -1L

        val result = Base16Parser.parseNumericValueFromBase16(encodedString)

        result[0] `should be equal to` expectedLow
        result[1] `should be equal to` expectedHigh
    }

    @Test
    fun `Can parse any base-16 value`() {
        val encodedString = "1234567890abcdef"

        val result = Base16Parser.parseNumericValueFromBase16(encodedString)

        val encodedBits = encodedString.length * 4
        val expectedLongs = (encodedBits + 63) / 64

        result.size `should be equal to` expectedLongs
    }

    @Test
    fun `Mathematics of parsed values should behave as expected`() {
        val baseValue = "ABC000"
        val incrementedValue1 = "ABC001"
        val incrementedValue2 = "ABC010"
        val incrementedValue3 = "ABC100"

        val baseNumeric = Base16Parser.parseNumericValueFromBase16(baseValue).first()
        val incrementedNumeric1 = Base16Parser.parseNumericValueFromBase16(incrementedValue1).first()
        val incrementedNumeric2 = Base16Parser.parseNumericValueFromBase16(incrementedValue2).first()
        val incrementedNumeric3 = Base16Parser.parseNumericValueFromBase16(incrementedValue3).first()

        (incrementedNumeric1 - baseNumeric) `should be equal to` 1
        (incrementedNumeric2 - baseNumeric) `should be equal to` 16
        (incrementedNumeric3 - baseNumeric) `should be equal to` (16 * 16)
    }

    @Test
    fun `Should fail on invalid characters`() {
        ('g'..'z').forEach { char ->
            invoking {
                Base16Parser.parseNumericValueFromBase16(char.toString())
            } `should throw` RuntimeException::class
        }
    }

    private fun String.stripHyphen() = replace("-", "")
}