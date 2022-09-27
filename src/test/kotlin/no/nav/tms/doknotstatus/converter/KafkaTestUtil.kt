package no.nav.tms.doknotstatus.converter

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.MockConsumer
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

    fun <K, V> createMockProducer(): MockProducer<K, V> {
        return MockProducer(
            false,
            { _: String, _: K -> ByteArray(0) }, //Dummy serializers
            { _: String, _: V -> ByteArray(0) }
        )
    }
}
