package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.collections.ArrayBatchProcessor
import com.dineshsawant.simplymigrate.collections.BatchProcessor
import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Database
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import com.dineshsawant.simplymigrate.database.createDatabase
import java.lang.IllegalArgumentException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.*

abstract class MultiThreadedMigration(
    val sourceDbInfo: DatabaseInfo,
    val targetDbInfo: DatabaseInfo,
    val targetTable: String,
    val fetchSize: Int,
    val loadSize: Int,
    val partitionKeyName: String
) : DataMigration {

    protected val sourceDatabase: Database by lazy { createDatabase(sourceDbInfo) }
    protected val targetDatabase: Database by lazy { createDatabase(targetDbInfo) }

    protected var fetchCompleted = false

    private val executorService = Executors.newFixedThreadPool(2)

    override fun start() : Long  {
        val sourceTableMetaData = getSourceMetaData()
        println("Source table metadata = $sourceTableMetaData")

        val targetTableMetaData = getTargetMetaData()
        println("Target table metadata = $targetTableMetaData")


        val workers = distribute(sourceTableMetaData, targetTableMetaData)
        val futures = workers.map { executorService.submit(it) }.toList()

        var migrationCount = 0L
        futures.forEach {
            while (!it.isDone) {
                Thread.sleep(3000)
            }
            migrationCount+=it.get()
        }
        executorService.shutdown()
        return migrationCount
    }

    abstract fun distribute(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ): List<Callable<Long>>

    abstract fun getTargetMetaData(): QueryResultMetaData

    abstract fun getSourceMetaData(): QueryResultMetaData

    fun isFetchCompleted(end: Any, max: Any): Boolean {
        return when (end) {
            is Long -> {
                max as Long
                end > max
            }
            is Int -> {
                max as Int
                end > max
            }
            is LocalDate -> {
                max as LocalDate
                end.isAfter(max)
            }
            is LocalDateTime -> {
                max as LocalDateTime
                end.isAfter(max)
            }
            else -> throw IllegalArgumentException()
        }
    }
}