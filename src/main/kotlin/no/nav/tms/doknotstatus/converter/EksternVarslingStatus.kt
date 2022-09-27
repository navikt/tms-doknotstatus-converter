package no.nav.tms.doknotstatus.converter

import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus

data class EksternVarslingStatus(
    val eventId: String,
    val bestillerAppnavn: String,
    val status: String,
    val melding: String,
    val distribusjonsId: Long?,
    val kanaler: List<String>
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
        private fun getKanaler(external: DoknotifikasjonStatus): List<String> =
            listOfNotNull(parseKanal(external.getMelding()))

        private fun parseKanal(melding: String): String? {
            val kanalMeldingPattern = "notifikasjon sendt via (\\w+)".toRegex()
            return kanalMeldingPattern.find(melding)?.destructured?.let { (kanal) ->
                kanal.uppercase()
            }
        }
    }
}