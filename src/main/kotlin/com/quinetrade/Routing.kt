package com.quinetrade

import io.ktor.server.application.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*

fun Application.configureRouting() = routing() {
  get("/bns/stream") {

  }
  webSocket("/ws/bns/stream") {
    send("test stream")
  }
}
