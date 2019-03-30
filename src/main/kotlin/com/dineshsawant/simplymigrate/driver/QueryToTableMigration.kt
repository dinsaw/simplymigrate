package com.dineshsawant.simplymigrate.driver

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.dineshsawant.simplymigrate.database.PartitionKey
import com.dineshsawant.simplymigrate.database.QueryResultMetaData

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

    override fun getFetcher(
        sourceTableMetaData: QueryResultMetaData,
        targetTableMetaData: QueryResultMetaData
    ): Runnable {
        val partitionKeyColumn = sourceTableMetaData.getColumnByLabel(partitionKeyName)!!
        val (min, max) = sourceDatabase.getMinMaxForQuery(fetchQuery, partitionKeyColumn)
        println("Min = $min Max = $max")

        return Runnable {
            try {
                val partitionKey = PartitionKey(partitionKeyColumn, min, max)
                var last: Any? = null
                while (!fetchCompleted) {
                    val (start, end) = partitionKey.nextRange(fetchSize, last)
                    val records =
                        sourceDatabase.selectRecordsByQuery(
                            partitionKey, start, end, fetchQuery,
                            targetTableMetaData.columnSet
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
                println("${e.message}")
                e.printStackTrace()
                System.exit(1)
            }
        }
    }
}
