package com.dineshsawant.simplymigrate

import com.dineshsawant.simplymigrate.cli.Migrate
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant

private val logger = KotlinLogging.logger {}
fun main(args: Array<String>) {
    val startedAt = Instant.now()
    Migrate().main(args)
    logger.info { "Total time taken = ${Duration.between(startedAt, Instant.now())}" }
}
