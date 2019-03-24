package com.dineshsawant.datamig

import com.dineshsawant.datamig.cli.Migrate
import java.time.Duration
import java.time.Instant

fun main(args: Array<String>) {
    val startedAt = Instant.now()
    Migrate().main(args)
    println("Total time taken in minutes = ${Duration.between(startedAt, Instant.now())}")
}