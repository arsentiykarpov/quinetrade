package com.quinetrade

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.plugins.logging.*

class NetClient() {

  var client: HttpClient

  init {
    client = HttpClient(CIO) {install(WebSockets)
      install(Logging)
    }

  }

}

