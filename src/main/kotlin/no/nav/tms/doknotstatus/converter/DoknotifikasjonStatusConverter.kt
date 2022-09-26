package no.nav.tms.doknotstatus.converter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.RetriableException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class DoknotifikasjonStatusConverter(
    private val consumer: Consumer<String, DoknotifikasjonStatus>,
    private val producer: Producer<String, String>,
    private val doknotifikasjonStatusTopic: String,
    private val brukervarselTopic: String
) : CoroutineScope {

    private val logger: Logger = LoggerFactory.getLogger(DoknotifikasjonStatusConverter::class.java)
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
        if (records.isEmpty) return // poll returns an empty collection in case of rebalancing

        try {
            records.onEach { record ->
                val key = record.value().getBestillingsId()
                val eksternVarselStatus = EksternVarselStatus(record.value())
                val valueNode = objectMapper.valueToTree<ObjectNode>(eksternVarselStatus)
                valueNode.put("@event_name", "eksternvarselstatus")

                producer.send(
                    ProducerRecord(
                        brukervarselTopic,
                        key,
                        valueNode.toString()
                    )
                )
                logger.info("Sendt eksternvarselstatus for varsel $key")
            }
            consumer.commitSync()
        } catch (re: RetriableException) {
            logger.warn("Polling mot Kafka feilet, ruller tilbake lokalt offset", re)
            consumer.rollbackToLastCommitted()
        }
    }

    private fun <K, V> Consumer<K, V>.rollbackToLastCommitted() {
        val assignedPartitions = assignment()
        val partitionCommittedInfo = committed(assignedPartitions)
        partitionCommittedInfo.forEach { (partition, lastCommitted) ->
            seek(partition, lastCommitted.offset())
        }
    }
}