package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should throw`
import org.amshove.kluent.invoking
import org.junit.jupiter.api.Test
import java.lang.RuntimeException

internal class Base32UlidParserTest {
    @Test
    fun `Can parse base-32 minimum from ULID string`() {
        val encodedString = "00000000000000000000000000"

        val expectedLow = 0L
        val expectedHigh = 0L

        val result = Base32UlidParser.parseNumericValueFromBase32Ulid(encodedString)

        result[0] `should be equal to` expectedLow
        result[1] `should be equal to` expectedHigh
    }
    @Test
    fun `Can parse base-32 maximum from ULID string`() {
        val encodedString = "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"

        val expectedLow = -1L
        val expectedHigh = -1L

        val result = Base32UlidParser.parseNumericValueFromBase32Ulid(encodedString)

        result[0] `should be equal to` expectedLow
        result[1] `should be equal to` expectedHigh
    }

    @Test
    fun `Can parse any base-32 value from ULID string`() {
        val encodedString = "1234567890ABCDEFGHJKMNPQRSTVWXYZ"

        val result = Base32UlidParser.parseNumericValueFromBase32Ulid(encodedString)

        val encodedBits = encodedString.length * 5
        val expectedLongs = (encodedBits + 63) / 64

        result.size `should be equal to` expectedLongs
    }

    @Test
    fun `Should fail on invalid characters`() {

        "ilouILOU".forEach { char ->
            invoking {
                Base32UlidParser.parseNumericValueFromBase32Ulid(char.toString())
            } `should throw` RuntimeException::class
        }
    }

    @Test
    fun `Mathematics of parsed values should behave as expected`() {
        val baseValue = "ABC000"
        val incrementedValue1 = "ABC001"
        val incrementedValue2 = "ABC010"
        val incrementedValue3 = "ABC100"

        val baseNumeric = Base32UlidParser.parseNumericValueFromBase32Ulid(baseValue).first()
        val incrementedNumeric1 = Base32UlidParser.parseNumericValueFromBase32Ulid(incrementedValue1).first()
        val incrementedNumeric2 = Base32UlidParser.parseNumericValueFromBase32Ulid(incrementedValue2).first()
        val incrementedNumeric3 = Base32UlidParser.parseNumericValueFromBase32Ulid(incrementedValue3).first()

        (incrementedNumeric1 - baseNumeric) `should be equal to` 1
        (incrementedNumeric2 - baseNumeric) `should be equal to` 32
        (incrementedNumeric3 - baseNumeric) `should be equal to` (32 * 32)
    }
}