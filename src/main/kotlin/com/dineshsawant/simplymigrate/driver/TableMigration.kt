package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.collections.ArrayBatchProcessor
import com.dineshsawant.simplymigrate.collections.BatchProcessor
import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Column
import com.dineshsawant.simplymigrate.database.PartitionKey
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import java.util.concurrent.Callable
import kotlin.streams.toList

class TableMigration(
    sourceDbInfo: DatabaseInfo,
    targetDbInfo: DatabaseInfo,
    private val sourceTable: String,
    targetTable: String,
    fetchSize: Int,
    loadSize: Int,
    partitionKeyName: String,
    private val boundBy: String,
    private val upper: String,
    private val lower: String
) : MultiThreadedMigration(sourceDbInfo, targetDbInfo, targetTable, fetchSize, loadSize, partitionKeyName) {

    override fun getTargetMetaData() = targetDatabase.getTableMetaData(targetTable)

    override fun getSourceMetaData() = sourceDatabase.getTableMetaData(sourceTable)

    override fun distribute(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ): List<Callable<Long>> {
        val upsertFunction : (List<LinkedHashMap<String, Any>>) -> Unit  = { list -> targetDatabase.upsert(targetTableMetaData, list) }
        val arrayBatchProcessor: ArrayBatchProcessor<LinkedHashMap<String, Any>> = ArrayBatchProcessor(loadSize, upsertFunction)

        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val columnList = boundBy.toLowerCase().split(",").toList()
        val boundByColumns = sourceTableMetaData.columnSet.stream()
            .filter { columnList.contains(it.label) }
            .toList()
        val (min, max) = sourceDatabase.getMinMax(sourceTable, partitionKeyColumn, lower, upper, boundByColumns)
        println("Min = $min Max = $max")
        return arrayListOf(Worker(partitionKeyColumn, min, max, targetTableMetaData, arrayBatchProcessor, boundByColumns))
    }
    inner class Worker(
        val partitionKeyColumn: Column,
        val min: Any,
        val max: Any,
        val targetTableMetaData: QueryResultMetaData,
        val batchUpdater: BatchProcessor<LinkedHashMap<String, Any>>,
        val boundByColumns: List<Column>
    ) : Callable<Long> {
        override fun call(): Long {
            try {
                val partitionKey = PartitionKey(partitionKeyColumn, min, max)
                var last: Any? = null
                while (!fetchCompleted) {
                    val (start, end) = partitionKey.nextRange(fetchSize, last)
                    val records =
                        sourceDatabase.selectRecords(
                            partitionKey, start, end, sourceTable,
                            targetTableMetaData.columnSet, lower, upper, boundByColumns
                        )
                    println("Putting records with size ${records.size}")
                    batchUpdater.enqueue(records)
                    last = end
                    fetchCompleted = isFetchCompleted(end, max)
                }
            } catch (e: Exception) {
                println("${e.message} ${e.stackTrace}")
                e.printStackTrace()
            }
            batchUpdater.flush()
            return batchUpdater.getFlushCount()
        }
    }

}
