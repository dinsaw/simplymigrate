package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Database
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import com.dineshsawant.simplymigrate.database.createDatabase
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
) : Migration {

    protected val sourceDatabase: Database by lazy { createDatabase(sourceDbInfo) }
    protected val targetDatabase: Database by lazy { createDatabase(targetDbInfo) }

    protected var fetchCompleted = false

    protected val blockingQueue: BlockingQueue<List<LinkedHashMap<String, Any>>> = LinkedBlockingQueue(2)

    private val executorService = Executors.newFixedThreadPool(2)

    override fun start() : Long  {
        val sourceTableMetaData = getSourceMetaData()
        println("Source table metadata = $sourceTableMetaData")

        val targetTableMetaData = getTargetMetaData()
        println("Target table metadata = $targetTableMetaData")

        val pusher = getPusher(targetTableMetaData)
        val fetcher = getFetcher(sourceTableMetaData, targetTableMetaData)

        val migrationCountFuture = executorService.submit(pusher)
        executorService.submit(fetcher)

        while (!migrationCountFuture.isDone) {
            Thread.sleep(3000)
        }

        val migrationCount = migrationCountFuture.get() as Long
        executorService.shutdown()
        executorService.awaitTermination(100, TimeUnit.MINUTES)
        return migrationCount
    }

    abstract fun getFetcher(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ): Runnable

    abstract fun getTargetMetaData(): QueryResultMetaData

    abstract fun getSourceMetaData(): QueryResultMetaData

    private fun getPusher(targetTableMetaData: QueryResultMetaData): Callable<Long> {
        println("Initializing pusher")
        return  Callable {
            var migrationCount = 0L
            try {
                while (!fetchCompleted || blockingQueue.isNotEmpty()) {
                    println("Looking for records")
                    val records = blockingQueue.take()
                    println("Took records with size = ${records.size}")
                    targetDatabase.upsert(targetTableMetaData, loadSize, records)
                    migrationCount += records.size
                    println("Migrated till now = $migrationCount")
                }
                migrationCount
            } catch (e: Exception) {
                println("Error ${e.message}")
                e.printStackTrace()
                System.exit(2)
                migrationCount
            }
        }
    }

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
            else -> throw UnsupportedOperationException()
        }
    }
}