package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.util.isGreaterThanOrEqual
import java.time.LocalDate
import java.time.LocalDateTime

class PartitionKey(
    val column: Column,
    val min: Any,
    val max: Any
) {

    val lastValue: Any = min

    fun nextRange(fetchSize: Int, last: Any?): Pair<Any, Any> {
        val lastValueToBeUsed = last ?: lastValue
        val nextValue: Any

        val (s, e) = when (lastValueToBeUsed) {
            is Long -> {
                nextValue = lastValueToBeUsed + fetchSize
                Pair(lastValueToBeUsed + 1, nextValue)
            }
            is Int -> {
                nextValue = lastValueToBeUsed + fetchSize
                Pair(lastValueToBeUsed + 1, nextValue)
            }
            is LocalDate -> {
                nextValue = lastValueToBeUsed.plusDays(fetchSize.toLong())
                Pair(lastValueToBeUsed.plusDays(1), nextValue)
            }
            is LocalDateTime -> {
                nextValue = lastValueToBeUsed.plusHours(fetchSize.toLong())
                Pair(lastValueToBeUsed, nextValue)
            }
            else -> throw IllegalArgumentException("Unable to create range for $lastValueToBeUsed")
        }
        val endOrMax = if (isGreaterThanOrEqual(e, max)) max else e
        return Pair(s, endOrMax)
    }

}