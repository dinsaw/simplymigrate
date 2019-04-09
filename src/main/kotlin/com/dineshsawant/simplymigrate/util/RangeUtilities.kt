package com.dineshsawant.simplymigrate.util

import mu.KotlinLogging
import java.lang.IllegalArgumentException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
private val logger = KotlinLogging.logger {}
fun divideRange(min: Any, max: Any, divideBy: Int): List<Pair<Any, Any>> {
    return when (min) {
        is Long -> {
            max as Long
            val groupLength = (max - min + 1) / divideBy
            val pairList = arrayListOf<Pair<Long, Long>>()
            for (i in min..max step groupLength) {
                pairList.add(Pair(i, i+groupLength-1))
            }
            pairList
        }
        is Int -> {
            max as Int
            val groupLength = (max - min + 1) / divideBy
            val pairList = arrayListOf<Pair<Int, Int>>()
            for (i in min..max step groupLength) {
                pairList.add(Pair(i, i+groupLength-1))
            }
            logger.debug { pairList }
            pairList
        }
        is LocalDate -> {
            max as LocalDate

            val groupLength = ChronoUnit.DAYS.between(min, max).plus(1) / divideBy
            val pairList = arrayListOf<Pair<LocalDate, LocalDate>>()
            var current: LocalDate = min
            while (current.isBefore(max)) {
                pairList.add(Pair(current, current.plusDays(groupLength-1)))
                current = current.plusDays(groupLength)
            }
            pairList
        }
        is LocalDateTime -> {
            max as LocalDateTime

            val groupLength = ChronoUnit.MINUTES.between(min, max).plus(1) / divideBy
            val pairList = arrayListOf<Pair<LocalDateTime, LocalDateTime>>()
            var current: LocalDateTime = min
            while (current.isBefore(max)) {
                pairList.add(Pair(current, current.plusMinutes(groupLength-1)))
                current = current.plusMinutes(groupLength)
            }
            pairList            }
        else -> throw IllegalArgumentException()
    }
}

fun isGreaterThanOrEqual(first: Any, second: Any): Boolean {
    return when (first) {
        is Long -> {
            second as Long
            first >= second
        }
        is Int -> {
            second as Int
            first >= second
        }
        is LocalDate -> {
            second as LocalDate
            first == second || first.isAfter(second)
        }
        is LocalDateTime -> {
            second as LocalDateTime
            first == second || first.isAfter(second)
        }
        else -> throw IllegalArgumentException()
    }
}
