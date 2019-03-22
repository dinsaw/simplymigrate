package com.dineshsawant.datamig.driver

import com.dineshsawant.datamig.config.DatabaseInfo
import com.dineshsawant.datamig.database.Database
import com.dineshsawant.datamig.database.PartitionKey
import com.dineshsawant.datamig.database.QueryResultMetaData
import java.time.LocalDate
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

class Migration(
    val sourceDbInfo: DatabaseInfo,
    val targetDbInfo: DatabaseInfo,
    val sourceTable: String,
    val targetTable: String,
    val fetchSize: Int,
    val loadSize: Int,
    val partitionKeyName: String
) {
    private val sourceDatabase: Database by lazy { Database(sourceDbInfo) }
    private val targetDatabase: Database by lazy { Database(targetDbInfo) }
    private var fetchCompleted = false

    val blockingQueue: BlockingQueue<List<LinkedHashMap<String, Any>>> = LinkedBlockingQueue(2)

    fun start() {
        val sourceTableMetaData = sourceDatabase.getTableMetadata(sourceTable)
        println("Source table metadata = $sourceTableMetaData")

        val targetTableMetaData = targetDatabase.getTableMetadata(targetTable)
        println("Target table metadata = $targetTableMetaData")

        startPusher(targetTableMetaData)
        startFetcher(sourceTableMetaData, targetTableMetaData)
    }

    private fun startPusher(targetTableMetaData: QueryResultMetaData) {
        println("Initializing pusher")
        val pusher: () -> Unit = {
            var migrationCount = 0
            while (!fetchCompleted || blockingQueue.isNotEmpty()) {
                println("Looking for records")
                val records = blockingQueue.take()
                println("Took records with size = ${records.size}")
                targetDatabase.upsert(targetTableMetaData, loadSize, records)
                migrationCount += records.size
                println("Migrated till now = $migrationCount")
            }
            println("Migration completed. Total migrated = $migrationCount")
        }
        val pusherThread = Thread(pusher)
        println("Starting pusher")
        pusherThread.start()
    }

    private fun startFetcher(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ) {
        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val (min, max) = sourceDatabase.getMinMax(sourceTable, partitionKeyColumn)
        println("Min = $min Max = $max")

        val fetcher: () -> Unit = {
            val partitionKey = PartitionKey(partitionKeyColumn, min, max)
            var last: Any? = null
            while (true) {
                val (start, end) = partitionKey.nextRange(fetchSize, last)
                val records =
                    sourceDatabase.selectRecords(partitionKey, start, end, sourceTable, targetTableMetaData.columnSet)
                if (records.isNotEmpty()) {
                    println("Putting records with size ${records.size}")
                    blockingQueue.put(records)
                }
                last = end

                if (fetchCompleted(end, max)) {
                    fetchCompleted = true
                    break
                }
            }
        }
        val fetchThread = Thread(fetcher)
        fetchThread.start()
        fetchThread.join()
    }

    private fun fetchCompleted(end: Any, max: Any): Boolean {
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
            else -> throw UnsupportedOperationException()
        }
    }

}
