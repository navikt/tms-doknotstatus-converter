package no.nav.tms.doknotstatus.converter

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test

class DoknotifikasjonStatusConverterTest {

    private val doknotTopicPartition = TopicPartition("beskjed", 0)
    private val doknotKafkaConsumer = MockConsumer<String, DoknotifikasjonStatus>(OffsetResetStrategy.EARLIEST).also {
        it.subscribe(listOf(doknotTopicPartition.topic()))
        it.rebalance(listOf(doknotTopicPartition))
        it.updateBeginningOffsets(mapOf(doknotTopicPartition to 0))
    }

    private val riverProducer = KafkaTestUtil.createMockProducer<String, String>()

    @Test
    fun `Konverter doknotstatus-melding til intern river-melding`() {
        val doknotifikasjonStatusConverter = DoknotifikasjonStatusConverter(
            consumer = doknotKafkaConsumer,
            producer = riverProducer,
            doknotifikasjonStatusTopic = "doknotifikasjonStatusTopic",
            brukervarselTopic = "brukerVarselTopic"
        )

        val doknotStatus = createDoknotifikasjonStatus("123")

        doknotKafkaConsumer.addRecord(
            ConsumerRecord(
                doknotTopicPartition.topic(),
                doknotTopicPartition.partition(),
                0,
                doknotStatus.getBestillerId(),
                doknotStatus
            )
        )

        runBlocking {
            doknotifikasjonStatusConverter.startPolling()
            KafkaTestUtil.delayUntilCommittedOffset(doknotKafkaConsumer, doknotTopicPartition.topic(), 1)
            doknotifikasjonStatusConverter.stopPolling()
        }

        riverProducer.history().size shouldBe 1
        val eksternVarselStatusJson = ObjectMapper().readTree(riverProducer.history().first().value())
        eksternVarselStatusJson.has("@event_name") shouldBe true
        eksternVarselStatusJson["@event_name"].asText() shouldBe "eksternvarselstatus"
        eksternVarselStatusJson["eventId"].asText() shouldBe doknotStatus.getBestillingsId()
        eksternVarselStatusJson["bestillerAppnavn"].asText() shouldBe doknotStatus.getBestillerId()
        eksternVarselStatusJson["status"].asText() shouldBe doknotStatus.getStatus()
        eksternVarselStatusJson["melding"].asText() shouldBe doknotStatus.getMelding()
        eksternVarselStatusJson["distribusjonsId"].asLong() shouldBe doknotStatus.getDistribusjonId()
        eksternVarselStatusJson["kanaler"].first().asText() shouldBe "MAIL"
        eksternVarselStatusJson["antallOppdateringer"].asInt() shouldBe 1
    }

    private fun createDoknotifikasjonStatus(
        bestillingsId: String,
        bestiller: String = "dummyBestiller",
        status: String = "INFO",
        melding: String = "notifikasjon sendt via mail",
        distribusjonsId: Long = 1L
    ): DoknotifikasjonStatus {
        return DoknotifikasjonStatus.newBuilder()
            .setBestillingsId(bestillingsId)
            .setBestillerId(bestiller)
            .setStatus(status)
            .setMelding(melding)
            .setDistribusjonId(distribusjonsId)
            .build()
    }
}