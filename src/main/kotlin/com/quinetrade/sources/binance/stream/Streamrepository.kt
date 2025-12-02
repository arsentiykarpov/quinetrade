package com.quintrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow

interface StreamRepository<T> {

  fun stream(): Flow<T>
}
