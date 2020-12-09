package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base16Parser.parseNumericValueFromBase16
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base32UlidParser.parseNumericValueFromBase32Ulid
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class EventIdParseUtilKtTest {

    @Test
    fun `Can parse 16 bit encoded value from string`() {

        val encodedString = "7234567812345678"
        val expected = 8229297492814222968L

        val result = parseNumericValueFromBase16(encodedString)

        result[0] `should be equal to` expected
    }

    @Test
    fun `Can parse 16 bit encoded value from string and overflow into negative for last bit of Long`() {

        val encodedString = "8234567812345678"
        val expected = -9064525076288481672L

        val result = parseNumericValueFromBase16(encodedString)

        result[0] `should be equal to` expected
    }

    @Test
    fun `Can parse 16 bit encoded value from string and continue onto next Long when first is full`() {

        val encodedString = "18234567812345678"
        val expectedLowBits = -9064525076288481672L
        val expectedHighBits = 1L

        val result = parseNumericValueFromBase16(encodedString)

        result[0] `should be equal to` expectedLowBits
        result[1] `should be equal to` expectedHighBits
    }


    @Test
    fun `Can parse base-32 according to ULID standard`() {
        val encodedString = "ZZZZZZZZZZZZZZZZZZZZZZZZZZ"

        val expectedLow = -1L
        val expectedHigh = -1L
        val expectedHigher = 3L

        val result = parseNumericValueFromBase32Ulid(encodedString)

        result[0] `should be equal to` expectedLow
        result[1] `should be equal to` expectedHigh
        result[2] `should be equal to` expectedHigher
    }
}