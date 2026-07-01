package no.nav.tms.doknotstatus.converter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus
import no.nav.tms.common.observability.traceVarsel
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.record.TimestampType
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.collections.set
import kotlin.coroutines.CoroutineContext

class DoknotifikasjonStatusConverter(
    private val consumer: Consumer<String, DoknotifikasjonStatus>,
    private val producer: Producer<String, String>,
    private val doknotifikasjonStatusTopic: String,
    private val brukervarselTopic: String,
    private val consumerGroupid: String
) : CoroutineScope {

    private val log = KotlinLogging.logger { }
    private val objectMapper = ObjectMapper()

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job
    private val job: Job = Job()

    fun isAlive() = job.isActive

    fun startPolling() {
        launch {
            run()
        }
    }

    suspend fun stopPolling() {
        job.cancelAndJoin()
    }

    private fun run() {
        producer.initTransactions()
        consumer.subscribe(listOf(doknotifikasjonStatusTopic))
        while (job.isActive) {
            onRecords(consumer.poll(Duration.ofSeconds(1)))
        }
    }

    private fun onRecords(records: ConsumerRecords<String, DoknotifikasjonStatus>) {
        if (records.isEmpty) {
            return // poll returns an empty collection in case of rebalancing
        }

        val transactionOffsets = transactionOffsets(records)

        producer.beginTransaction()

        try {
            records.forEach(::mapAndSendRecord)
            producer.sendOffsetsToTransaction(transactionOffsets, consumerGroupid)
            producer.commitTransaction()
        } catch (ke: KafkaException) {
            when (ke) {
                is ProducerFencedException, is OutOfOrderSequenceException, is AuthenticationException -> {
                    log.error { "Fatal feil ved videresending av doknotstatus. Avslutter polling." }
                    producer.close()
                    job.cancel()
                }
                else -> {
                    log.warn { "Midlertidig feil ved videresending av doknotstatusa. Ruller tilbake transaksjon og forsøker igjen" }
                    producer.abortTransaction()
                }
            }
        } catch (e: Exception) {
            log.error(e) { "Ukjent feil ved videresending av doknotstatus. Avslutter polling" }
            producer.close()
            job.cancel()
        }
    }

    private fun mapAndSendRecord(record: ConsumerRecord<String, DoknotifikasjonStatus>) {
        val varselId = record.value().getBestillingsId()

        traceVarsel(varselId) {
            val eksternVarslingStatus = EksternVarslingStatus(record.value())
            val valueNode = objectMapper.valueToTree<ObjectNode>(eksternVarslingStatus)
            valueNode.put("@event_name", "eksternVarslingStatus")
            valueNode.put("tidspunkt", record.eventTime().toString())
            valueNode.put("tidspunktZ", record.zonedEventTime().toString())

            log.info { "Konverterer doknot-status [${eksternVarslingStatus.status}] til internt event" }

            producer.send(
                ProducerRecord(
                    brukervarselTopic,
                    varselId,
                    valueNode.toString()
                )
            )
        }
    }

    private fun transactionOffsets(records: ConsumerRecords<String, DoknotifikasjonStatus>): Map<TopicPartition, OffsetAndMetadata> {
        val nextOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        records.forEach { record ->
            nextOffsets[record.topicPartition()] = OffsetAndMetadata(record.offset() + 1)
        }

        return nextOffsets
    }

    private fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(topic(), partition())


    private fun ConsumerRecord<*, *>.eventTime(): LocalDateTime {
        return if (timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
            return LocalDateTime.now(ZoneId.of("UTC"))
        } else {
            LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp()), ZoneId.of("UTC"))
        }
    }

    private fun ConsumerRecord<*, *>.zonedEventTime(): ZonedDateTime {
        return if (timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
            return ZonedDateTime.now(ZoneId.of("Z"))
        } else {
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp()), ZoneId.of("Z"))
        }
    }
}
