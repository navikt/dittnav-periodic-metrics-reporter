package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.objectmother

import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel.AvroNokkelObjectMother.createNokkel
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

object ConsumerRecordsObjectMother {

    fun giveMeANumberOfBeskjedRecords(numberOfRecords: Int, topicName: String): ConsumerRecords<Nokkel, Beskjed> {
        val records = mutableMapOf<TopicPartition, List<ConsumerRecord<Nokkel, Beskjed>>>()
        val recordsForSingleTopic = createBeskjedRecords(topicName, numberOfRecords)
        records[TopicPartition(topicName, numberOfRecords)] = recordsForSingleTopic
        return ConsumerRecords(records)
    }

    private fun createBeskjedRecords(topicName: String, totalNumber: Int): List<ConsumerRecord<Nokkel, Beskjed>> {
        val allRecords = mutableListOf<ConsumerRecord<Nokkel, Beskjed>>()
        for (i in 0 until totalNumber) {
            val schemaRecord = AvroBeskjedObjectMother.createBeskjed(i)
            val nokkel = createNokkel(i)
            allRecords.add(ConsumerRecord(topicName, i, i.toLong(), nokkel, schemaRecord))
        }
        return allRecords
    }

    fun <T> createConsumerRecord(nokkel: Nokkel, actualEvent: T): ConsumerRecord<Nokkel, T> {
        return ConsumerRecord("dummyTopic", 1, 0, nokkel, actualEvent)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> createConsumerRecordWithoutNokkel(actualEvent: T): ConsumerRecord<Nokkel, T> {
        return ConsumerRecord("dummyTopic", 1, 0, null, actualEvent) as ConsumerRecord<Nokkel, T>
    }

    @Suppress("UNCHECKED_CAST")
    fun createConsumerRecordWithoutRecord(nokkel: Nokkel): ConsumerRecord<Nokkel, GenericRecord> {
        return ConsumerRecord("dummyTopic", 1, 0, nokkel, null) as ConsumerRecord<Nokkel, GenericRecord>
    }

}
