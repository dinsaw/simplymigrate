package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.collections.ArrayBatchProcessor
import com.dineshsawant.simplymigrate.collections.BatchProcessor
import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Column
import com.dineshsawant.simplymigrate.database.PartitionKey
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import java.util.concurrent.Callable

class QueryToTableMigration(
    sourceDbInfo: DatabaseInfo,
    targetDbInfo: DatabaseInfo,
    private val fetchQuery: String,
    targetTable: String,
    fetchSize: Int,
    loadSize: Int,
    partitionKeyName: String
) : MultiThreadedMigration(sourceDbInfo, targetDbInfo, targetTable, fetchSize, loadSize, partitionKeyName) {


    override fun getTargetMetaData() = targetDatabase.getTableMetaData(targetTable)

    override fun getSourceMetaData() = sourceDatabase.getQueryMetaData(fetchQuery)

    override fun distribute(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData): List<Callable<Long>> {

        val upsertFunction : (List<LinkedHashMap<String, Any>>) -> Unit  = { list -> targetDatabase.upsert(targetTableMetaData, list) }
        val arrayBatchProcessor: ArrayBatchProcessor<LinkedHashMap<String, Any>> = ArrayBatchProcessor(loadSize, upsertFunction)

        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val (min, max) = sourceDatabase.getMinMaxForQuery(fetchQuery, partitionKeyColumn)
        println("Min = $min Max = $max")
        return arrayListOf(Worker(partitionKeyColumn, min, max, targetTableMetaData, arrayBatchProcessor))
    }

    inner class Worker(
        val partitionKeyColumn: Column,
        val min: Any,
        val max: Any,
        val targetTableMetaData: QueryResultMetaData,
        val batchUpdater: BatchProcessor<LinkedHashMap<String, Any>>
    ) : Callable<Long> {
        override fun call(): Long {
            try {
                val partitionKey = PartitionKey(partitionKeyColumn, min, max)
                var last: Any? = null
                while (!fetchCompleted) {
                    val (start, end) = partitionKey.nextRange(fetchSize, last)
                    val records = sourceDatabase.selectRecordsByQuery(
                        partitionKey, start, end, fetchQuery,
                        targetTableMetaData.columnSet
                    )
                    println("Putting records with size ${records.size}")
                    batchUpdater.enqueue(records)

                    last = end
                    fetchCompleted = isFetchCompleted(end, max)
                }
            } catch (e: Exception) {
                println("${e.message}")
                e.printStackTrace()
            }
            batchUpdater.flush()
            return batchUpdater.getFlushCount()
        }
    }
}
