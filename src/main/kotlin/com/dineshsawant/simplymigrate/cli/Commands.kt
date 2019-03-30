package com.dineshsawant.simplymigrate.cli

import com.dineshsawant.simplymigrate.config.MigrationConfig
import com.dineshsawant.simplymigrate.driver.QueryToTableMigration
import com.dineshsawant.simplymigrate.driver.TableMigration
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.types.int
import com.uchuhimo.konf.Config
import java.io.File

class Migrate : CliktCommand() {
    val configFilePath: String by option("--configFile", help = "Config file path").prompt("Provide config file path")
    val sourceTable: String by option("--sourceTable", help = "Source Table").default("")
    val fetchQueryFilePath: String by option("--fetchQueryFilePath", help = "Fetch query file path").default("")
    val targetTable: String by option("--targetTable", help = "Sink Table").prompt("Provide Sink Table")

    val fetchSize: Int by option("--fetchSize", help = "Source fetch size").int().default(1000)
    val loadSize: Int by option("--loadSize", help = "Sink load size").int().default(1000)

    val partitionKey: String by option("--partitionKey", help = "Partition key/Columns").default("id")

    val boundBy: String by option("--boundBy", help = "Bound by column/alias").default("")
    val lower: String by option("--lower", help = "Lower bound value").default("")
    val upper: String by option("--upper", help = "Upper bound value").default("")

    override fun run() {

        println(
            "sourceTable = $sourceTable, targetTable = $targetTable, fetchSize = $fetchSize, loadSize = $loadSize" +
                    "boundBy = $boundBy, lower = $lower, upper = $upper"
        )
        if (sourceTable.isNullOrBlank() && fetchQueryFilePath.isNullOrBlank()) {
            throw IllegalArgumentException("Either sourceTable or fetchQueryFilePath should be provided")
        }

        println("Using config from location $configFilePath")

        val config = Config { addSpec(MigrationConfig) }.from.yaml.file(configFilePath)
        println("Configuration = ${config.toMap()}")

        val migratedCount = if (fetchQueryFilePath.isNotBlank()) {
            val fetchQuery = File(fetchQueryFilePath).readText()
            QueryToTableMigration(
                config[MigrationConfig.source],
                config[MigrationConfig.target],
                fetchQuery,
                targetTable,
                fetchSize,
                loadSize,
                partitionKey
            ).start()
        } else {
            TableMigration(
                config[MigrationConfig.source],
                config[MigrationConfig.target],
                sourceTable,
                targetTable,
                fetchSize,
                loadSize,
                partitionKey,
                boundBy,
                upper,
                lower
            ).start()
        }

        println("Migration completed. Total migrated = $migratedCount")
    }

}