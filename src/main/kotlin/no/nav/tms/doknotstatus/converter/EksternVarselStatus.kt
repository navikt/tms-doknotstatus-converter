package no.nav.tms.doknotstatus.converter

import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus

data class EksternVarselStatus(
    val eventId: String,
    val bestillerAppnavn: String,
    val status: String,
    val melding: String,
    val distribusjonsId: Long?,
    val kanaler: List<String>,
    val antallOppdateringer: Int = 1
) {
    constructor(doknotifikasjonStatus: DoknotifikasjonStatus) : this(
        eventId = doknotifikasjonStatus.getBestillingsId(),
        bestillerAppnavn = doknotifikasjonStatus.getBestillerId(),
        status = doknotifikasjonStatus.getStatus(),
        melding = doknotifikasjonStatus.getMelding(),
        distribusjonsId = doknotifikasjonStatus.getDistribusjonId(),
        kanaler = getKanaler(doknotifikasjonStatus)
    )

    companion object {
        private fun getKanaler(external: DoknotifikasjonStatus): List<String> {
            val kanal = parseKanal(external.getMelding())

            return if (kanal != null) {
                listOf(kanal)
            } else {
                emptyList()
            }
        }

        private val kanalMeldingPattern = "notifikasjon sendt via (\\w+)".toRegex()

        private fun parseKanal(melding: String): String? {
            return kanalMeldingPattern.find(melding)?.destructured?.let { (kanal) ->
                kanal.uppercase()
            }
        }
    }
}