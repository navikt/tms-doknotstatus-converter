package no.nav.tms.doknotstatus.converter

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.TopicPartition

object KafkaTestUtil {

    suspend fun <K, V> delayUntilCommittedOffset(
        consumer: MockConsumer<K, V>,
        topicName: String,
        offset: Long
    ) {
        val partition = TopicPartition(topicName, 0)
        withTimeout(1000) {
            while ((consumer.committed(setOf(partition))[partition]?.offset() ?: 0) < offset) {
                delay(10)
            }
        }
    }

    fun <K, V> loopbackRecords(producer: MockProducer<K, V>, consumer: MockConsumer<K, V>) {
        var offset = 0L
        producer.history().forEach { producerRecord ->
            if (producerRecord.topic() in consumer.subscription()) {
                val partition =
                    TopicPartition(
                        producerRecord.topic(),
                        consumer.assignment().first { it.topic() == producerRecord.topic() }.partition()
                    )
                val consumerRecord = ConsumerRecord(
                    producerRecord.topic(),
                    partition.partition(),
                    offset++,
                    producerRecord.key(),
                    producerRecord.value()
                )
                consumer.addRecord(consumerRecord)
            }
        }
    }

    fun <K, V> createMockConsumer(topicName: String): MockConsumer<K, V> {
        val partition = TopicPartition(topicName, 0)
        return MockConsumer<K, V>(OffsetResetStrategy.EARLIEST).also {
            it.subscribe(listOf(partition.topic()))
            it.rebalance(listOf(partition))
            it.updateBeginningOffsets(mapOf(partition to 0))
        }
    }

    fun <K, V> createMockProducer(): MockProducer<K, V> {
        return MockProducer(
            false,
            { _: String, _: K -> ByteArray(0) }, //Dummy serializers
            { _: String, _: V -> ByteArray(0) }
        )
    }
}
