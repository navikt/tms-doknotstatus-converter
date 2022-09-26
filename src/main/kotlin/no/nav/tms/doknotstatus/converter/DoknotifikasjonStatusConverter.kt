package no.nav.tms.doknotstatus.converter

import com.fasterxml.jackson.databind.ObjectMapper
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
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class DoknotifikasjonStatusConverter(
    private val consumer: Consumer<String, DoknotifikasjonStatus>,
    private val producer: Producer<String, String>,
    private val brukervarselTopic: String
): CoroutineScope {
    private val logger: Logger = LoggerFactory.getLogger(DoknotifikasjonStatusConverter::class.java)
    val objectMapper = ObjectMapper()
    fun startPolling() {
        launch {
            run()
        }
    }

    suspend fun stopPolling() {
        job.cancelAndJoin()
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job
    private val job: Job = Job()

    fun isAlive() = job.isActive

    private fun run() {
        try {
            while (job.isActive) {
                onRecords(consumer.poll(Duration.ofSeconds(1)))
            }
        } catch (e: WakeupException) {
            if (job.isActive) throw e
        } catch (e: Exception) {
            logger.error( "Noe feil skjedde i consumeringen" , e)
            throw e
        } finally {
            closeResources()
        }
    }

    private fun onRecords(records: ConsumerRecords<String, DoknotifikasjonStatus>) {
        if (records.isEmpty) return // poll returns an empty collection in case of rebalancing

        try {
            records.onEach { record ->
                val key = record.value().getBestillingsId()
                val eksternVarselStatus = EksternVarselStatus(record.value())
                producer.send(ProducerRecord(
                    brukervarselTopic,
                    key,
                    objectMapper.writeValueAsString(eksternVarselStatus)
                ))
            }
            consumer.commitSync()
        } catch (err: Exception) {
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

    private fun closeResources() {
        tryAndLog(producer::close)
        tryAndLog(consumer::unsubscribe)
        tryAndLog(consumer::close)
    }

    private fun tryAndLog(block: () -> Unit) {
        try {
            block()
        } catch (err: Exception) {
            logger.error(err.message, err)
        }
    }
}

