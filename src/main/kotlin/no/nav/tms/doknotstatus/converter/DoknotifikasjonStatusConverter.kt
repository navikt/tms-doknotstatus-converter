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
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.record.TimestampType
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.coroutines.CoroutineContext

class DoknotifikasjonStatusConverter(
    private val consumer: Consumer<String, DoknotifikasjonStatus>,
    private val producer: Producer<String, String>,
    private val doknotifikasjonStatusTopic: String,
    private val brukervarselTopic: String
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
        consumer.subscribe(listOf(doknotifikasjonStatusTopic))
        while (job.isActive) {
            onRecords(consumer.poll(Duration.ofSeconds(1)))
        }
    }

    private fun onRecords(records: ConsumerRecords<String, DoknotifikasjonStatus>) {
        if (records.isEmpty) {
            return // poll returns an empty collection in case of rebalancing
        }

        try {
            records.forEach(::mapAndSendRecord)
            consumer.commitSync()
        } catch (re: RetriableException) {
            log.warn(re) { "Polling mot Kafka feilet, ruller tilbake lokalt offset" }
            consumer.rollbackToLastCommitted()
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

    private fun <K, V> Consumer<K, V>.rollbackToLastCommitted() {
        val assignedPartitions = assignment()
        val partitionCommittedInfo = committed(assignedPartitions)
        partitionCommittedInfo.forEach { (partition, lastCommitted) ->
            seek(partition, lastCommitted.offset())
        }
    }

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
