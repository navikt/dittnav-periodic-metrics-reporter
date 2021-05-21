package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer

fun <K, V> KafkaConsumer<K, V>.resetTheGroupIdsOffsetToZero() {
    assignment().forEach { partition ->
        seek(partition, 0)
    }
}

fun <K, V> ConsumerRecords<K, V>.foundRecords(): Boolean {
    return !isEmpty
}
