package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.PartitionKey
import com.dineshsawant.simplymigrate.database.QueryResultMetaData
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

    override fun getFetcher(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ): Runnable {
        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val columnList = boundBy.toLowerCase().split(",").toList()
        val boundByColumns = sourceTableMetaData.columnSet.stream()
            .filter { columnList.contains(it.label) }
            .toList()
        val (min, max) = sourceDatabase.getMinMax(sourceTable, partitionKeyColumn, lower, upper, boundByColumns)
        println("Min = $min Max = $max")

        return Runnable {
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
                    if (records.isNotEmpty()) {
                        println("Putting records with size ${records.size}")
                        blockingQueue.put(records)
                    }
                    last = end

                    if (isFetchCompleted(end, max)) {
                        fetchCompleted = true
                        break
                    }
                }
            } catch (e: Exception) {
                println("${e.message} ${e.stackTrace}")
                e.printStackTrace()
                System.exit(1)
            }
        }
    }

}
