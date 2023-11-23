package no.nav.tms.doknotstatus.converter

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.netty.util.NetUtil
import no.nav.tms.common.util.config.StringEnvVar.getEnvVar
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.net.InetSocketAddress
import java.util.Properties

data class Environment(
    val applicationName: String = "tms-doknotstatus-converter",
    val aivenBrokers: String = getEnvVar("KAFKA_BROKERS"),
    val aivenSchemaRegistry: String = getEnvVar("KAFKA_SCHEMA_REGISTRY"),
    val doknotifikasjonStatusGroupId: String = getEnvVar("GROUP_ID_DOKNOTIFIKASJON_STATUS"),
    val doknotifikasjonStatusTopicName: String = getEnvVar("DOKNOTIFIKASJON_STATUS_TOPIC"),
    val brukervarselTopic: String = getEnvVar("BRUKERVARSEL_TOPIC"),
    val aivenTruststorePath: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    val aivenKeystorePath: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    val aivenCredstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
    val aivenSchemaRegistryUser: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_USER"),
    val aivenSchemaRegistryPassword: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_PASSWORD")
) {
    fun consumerProps(): Properties {
        return Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, aivenBrokers)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, aivenSchemaRegistry)
            put(ConsumerConfig.GROUP_ID_CONFIG, doknotifikasjonStatusGroupId)
            put(ConsumerConfig.CLIENT_ID_CONFIG, doknotifikasjonStatusGroupId + NetUtil.getHostname(InetSocketAddress(0)))
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SwallowSerializationErrorsAvroDeserializer::class.java)
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            putAll(credentialPropsAiven())
        }
    }

    fun producerProps(): Properties {
        return Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, aivenBrokers)
            put(ProducerConfig.CLIENT_ID_CONFIG, "tms-doknotstatus-converter" + NetUtil.getHostname(InetSocketAddress(0)))
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 40000)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            putAll(credentialPropsAiven())
        }
    }

    private fun credentialPropsAiven(): Properties {
        return Properties().apply {
            put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, "${aivenSchemaRegistryUser}:${aivenSchemaRegistryPassword}")
            put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
            put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
            put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, aivenTruststorePath)
            put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, aivenCredstorePassword)
            put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, aivenKeystorePath)
            put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, aivenCredstorePassword)
            put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, aivenCredstorePassword)
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        }
    }
}
