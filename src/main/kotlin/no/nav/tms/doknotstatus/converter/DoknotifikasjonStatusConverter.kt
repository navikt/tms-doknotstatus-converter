package no.nav.tms.doknotstatus.converter

import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

class DoknotifikasjonStatusConverter(
    val consumer: Consumer<String, DoknotifikasjonStatus>,
    val riverProducer: Producer<String, String>
) {
    fun startPolling() {

    }

    fun stopPolling() {

    }

}