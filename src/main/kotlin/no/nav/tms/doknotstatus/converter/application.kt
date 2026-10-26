package no.nav.tms.doknotstatus.converter

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.ApplicationStarted
import io.ktor.server.application.ApplicationStopPreparing
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.engine.*
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondOutputStream
import io.ktor.utils.io.writer
import io.prometheus.metrics.expositionformats.ExpositionFormats
import io.prometheus.metrics.model.registry.MetricNameFilter
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kotlinx.coroutines.runBlocking
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus
import no.nav.tms.kafka.producer.KafkaProducerBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig

private val log = KotlinLogging.logger {}

fun main() {
    val environment = Environment()
    val doknotConsumer = KafkaConsumer<String, DoknotifikasjonStatus>(environment.consumerProps())
    val brukerVarselProducer = KafkaProducerBuilder.stringProducer {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, environment.transactionalId)
    }

    val doknotifikasjonStatusConverter = DoknotifikasjonStatusConverter(
        consumer = doknotConsumer,
        producer = brukerVarselProducer,
        brukervarselTopic = environment.brukervarselTopic,
        doknotifikasjonStatusTopic = environment.doknotifikasjonStatusTopicName
    )

    embeddedServer(
        factory = Netty,
        configure = {
            connector {
                port = 8080
            }
        },
        module = {
            routing {
                get("/isAlive") {
                    if(doknotifikasjonStatusConverter.isAlive()) {
                        call.respondText(text = "ALIVE", contentType = ContentType.Text.Plain)
                    } else {
                        call.respondText(text = "NOTALIVE", contentType = ContentType.Text.Plain, HttpStatusCode.ServiceUnavailable)
                    }
                }

                get("/isReady") {
                    call.respondText(text = "READY", contentType = ContentType.Text.Plain)
                }

                val writer = ExpositionFormats.init().openMetricsTextFormatWriter

                get("/metrics") {
                    val requestedNames = call.request.queryParameters.getAll("name[]")?.toSet() ?: emptySet()

                    val filter = if (requestedNames.isNotEmpty()) {
                        MetricNameFilter.builder().nameMustBeEqualTo(requestedNames).build()
                    } else {
                        null
                    }

                    call.respondOutputStream(ContentType.parse(writer.contentType)) {
                        writer.write(this, PrometheusRegistry.defaultRegistry.scrape(filter))
                    }
                }
            }

            this.monitor.subscribe(ApplicationStarted) {
                doknotifikasjonStatusConverter.startPolling()
            }

            this.monitor.subscribe(ApplicationStopPreparing) {
                brukerVarselProducer.shutdown()

                runBlocking {
                    doknotifikasjonStatusConverter.stopPolling()
                }

            }
        }
    ).start(wait = true)
}

private fun KafkaProducer<String, String>.shutdown() {
    try {
        flush()
        close()
        log.info { "Produsent for kafka-eventer er flushet og lukket." }
    } catch (e: Exception) {
        log.warn { "Klarte ikke å flushe og lukke produsent. Det kan være eventer som ikke ble produsert." }
    }
}
