package com.dineshsawant.simplymigrate

import com.dineshsawant.simplymigrate.cli.Migrate
import java.time.Duration
import java.time.Instant

fun main(args: Array<String>) {
    val startedAt = Instant.now()
    Migrate().main(args)
    println("Total time taken = ${Duration.between(startedAt, Instant.now())}")
}
