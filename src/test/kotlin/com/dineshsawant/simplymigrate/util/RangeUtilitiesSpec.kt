package com.dineshsawant.simplymigrate.util

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import java.time.LocalDate
import java.time.LocalDateTime

object RangeUtilitiesSpec:Spek ({
    describe("divideRange") {
        listOf<List<Any>>(
            listOf(1, 100, 10, 1, 10, 91, 100),
            listOf(1L, 100L, 10, 1L, 10L, 91L, 100L),
            listOf(LocalDate.parse("2018-03-01"), LocalDate.parse("2018-06-08"), 10,
                LocalDate.parse("2018-03-01"), LocalDate.parse("2018-03-10"),
                LocalDate.parse("2018-05-30"), LocalDate.parse("2018-06-08"))
        ).forEach {
            describe("divide Range") {
                val pairs = divideRange(it[0], it[1], 10)
                it("returns the list with size of divideBy") {
                    assertEquals(10, pairs.size)
                }
                it("returns the first pair with first n elements") {
                    assertEquals(it[3], pairs.first().first)
                    assertEquals(it[4], pairs.first().second)
                }
                it("returns the lasr pair with last n elements") {
                    assertEquals(it[5], pairs.last().first)
                    assertEquals(it[6], pairs.last().second)
                }
            }
        }
    }
})