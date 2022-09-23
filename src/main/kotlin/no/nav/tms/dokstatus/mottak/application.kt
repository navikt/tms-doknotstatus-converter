package no.nav.tms.dokstatus.mottak

import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.DefaultHeaders
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

fun main() {
    embeddedServer(Netty, port = 8080) {
        install(DefaultHeaders)
        routing {
            get("/isAlive") {
                call.respondText(text = "ALIVE", contentType = ContentType.Text.Plain)
            }

            get("/isReady") {
                call.respondText(text = "READY", contentType = ContentType.Text.Plain)
            }
        }
    }.start(wait = true)

}