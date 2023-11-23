import default.DependencyGroup

object Avro: DependencyGroup {
    override val groupId get() = "io.confluent"
    override val version get() = "6.2.1"

    val avroSerializer get() = dependency("kafka-avro-serializer")
    val schemaRegistry get() = dependency("kafka-schema-registry")
}

object Doknotifikasjon: DependencyGroup {
    override val groupId get() = "no.nav.teamdokumenthandtering"
    override val version get() = "08c0b2d2"

    val schemas get() = dependency("teamdokumenthandtering-avro-schemas")
}
