package no.nav.personbruker.dittnav.metrics.periodic.reporter.common

import no.nav.brukernotifikasjon.schemas.Nokkel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

interface EventBatchProcessorService<T> {

    suspend fun processEvents(events: ConsumerRecords<Nokkel, T>)

    val ConsumerRecord<Nokkel, T>.systembruker : String get() = key().getSystembruker()

}