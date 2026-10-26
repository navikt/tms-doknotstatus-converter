package no.nav.tms.doknotstatus.converter

import no.nav.doknotifikasjon.schemas.DoknotifikasjonStatus

data class EksternVarslingStatus(
    val eventId: String,
    val bestillerAppnavn: String,
    val status: String,
    val melding: String,
    val distribusjonsId: Long?,
    val kanal: String?
) {
    constructor(doknotifikasjonStatus: DoknotifikasjonStatus) : this(
        eventId = doknotifikasjonStatus.getBestillingsId(),
        bestillerAppnavn = doknotifikasjonStatus.getBestillerId(),
        status = doknotifikasjonStatus.getStatus(),
        melding = doknotifikasjonStatus.getMelding(),
        distribusjonsId = doknotifikasjonStatus.getDistribusjonId(),
        kanal = doknotifikasjonStatus.kanal?.uppercase()
    )
}
