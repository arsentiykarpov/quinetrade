package com.quinetrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow
import kotlin.reflect.KClass


interface StreamSource<T : Any> {
  val type: KClass<T>
    fun observeStream(): Flow<T>
}


