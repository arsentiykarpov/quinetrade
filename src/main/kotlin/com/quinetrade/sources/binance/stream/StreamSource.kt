package com.quinetrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow


interface StreamSource<T> {
    fun observeStream(): Flow<T>
}
