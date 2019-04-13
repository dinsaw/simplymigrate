package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Database
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import com.dineshsawant.simplymigrate.database.createDatabase
import com.dineshsawant.simplymigrate.util.ArrayBatchProcessor
import mu.KotlinLogging
import java.util.concurrent.*

private val logger = KotlinLogging.logger {}
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

    override fun start() : Long  {
        val sourceTableMetaData = getSourceMetaData()
        logger.debug { "Source table metadata = $sourceTableMetaData" }

        val targetTableMetaData = getTargetMetaData()
        logger.debug { "Target table metadata = $targetTableMetaData" }

        val threadCount = Runtime.getRuntime().availableProcessors().div(2)
        logger.info { "Using $threadCount threads for migration" }
        val executorService = Executors.newFixedThreadPool(threadCount)
        val workers = distribute(sourceTableMetaData, targetTableMetaData, threadCount)
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
        targetTableMetaData: QueryResultMetaData,
        numberOfParts: Int
    ): List<Callable<Long>>

    abstract fun getTargetMetaData(): QueryResultMetaData

    abstract fun getSourceMetaData(): QueryResultMetaData

    protected fun batchUpdater(targetTableMetaData: QueryResultMetaData): ArrayBatchProcessor<LinkedHashMap<String, Any>> {
        val upsertFunction: (List<LinkedHashMap<String, Any>>) -> Unit =
            { list -> targetDatabase.upsert(targetTableMetaData, list) }
        val arrayBatchProcessor: ArrayBatchProcessor<LinkedHashMap<String, Any>> =
            ArrayBatchProcessor(loadSize, upsertFunction)
        return arrayBatchProcessor
    }
}