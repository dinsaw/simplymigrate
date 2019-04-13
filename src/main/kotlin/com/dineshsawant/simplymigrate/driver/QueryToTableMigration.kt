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

private val logger = KotlinLogging.logger {}
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
        targetTableMetaData: QueryResultMetaData,
        numberOfParts: Int
    ): List<Callable<Long>> {

        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val (min, max) = sourceDatabase.getMinMaxForQuery(fetchQuery, partitionKeyColumn)
        logger.debug { "Min = $min Max = $max" }

        val ranges = divideRange(min, max, numberOfParts)
        logger.debug { "Divided work in ranges = $ranges" }
        return ranges
            .map { Worker(partitionKeyColumn, it.first, it.second, targetTableMetaData, batchUpdater(targetTableMetaData)) }
            .toList()
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
