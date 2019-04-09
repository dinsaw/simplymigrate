package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.util.BatchProcessor
import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.Column
import com.dineshsawant.simplymigrate.database.PartitionKey
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
import com.dineshsawant.simplymigrate.util.divideRange
import com.dineshsawant.simplymigrate.util.isGreaterThanOrEqual
import mu.KotlinLogging
import java.util.concurrent.Callable
import kotlin.streams.toList

private val logger = KotlinLogging.logger {}
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
        targetTableMetaData: QueryResultMetaData,
        numberOfParts: Int
    ): List<Callable<Long>> {
        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!

        val columnList = boundBy.toLowerCase().split(",").toList()
        val boundByColumns = sourceTableMetaData.columnSet.stream()
            .filter { columnList.contains(it.label) }
            .toList()
        val (min, max) = sourceDatabase.getMinMax(sourceTable, partitionKeyColumn, lower, upper, boundByColumns)

        logger.debug { "Min = $min Max = $max" }
        return divideRange(min, max, numberOfParts)
            .map { Worker(partitionKeyColumn, it.first, it.second, targetTableMetaData, batchUpdater(targetTableMetaData), boundByColumns) }
            .toList()
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
                    logger.debug { "Putting records with size ${records.size}" }
                    batchUpdater.enqueue(records)
                    last = end
                    fetchCompleted = isGreaterThanOrEqual(end, max)
                }
            } catch (e: Exception) {
                logger.error(e) { "Error occurred" }
            }
            batchUpdater.flush()
            return batchUpdater.getFlushCount()
        }
    }

}
