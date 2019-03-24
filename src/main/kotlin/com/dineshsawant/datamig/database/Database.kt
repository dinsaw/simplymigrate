package com.dineshsawant.datamig.database

import com.dineshsawant.datamig.config.DatabaseInfo

interface Database {
    fun getTableMetadata(table: String): QueryResultMetaData
    fun upsert(tableMetaData: QueryResultMetaData, loadSize: Int, records: List<LinkedHashMap<String, Any>>)
    fun getMinMax(table: String, column: Column, lower: String, upper: String, boundByColumns: List<Column>): Array<Any>
    fun selectRecords(
        partitionKey: PartitionKey,
        start: Any,
        end: Any,
        table: String,
        columnSet: MutableSet<Column>,
        lower: String,
        upper: String,
        boundByColumns: List<Column>
    ): List<LinkedHashMap<String, Any>>
}

fun createDatabase(databaseInfo: DatabaseInfo) : Database {
    return when (databaseInfo.database) {
        DatabaseVersion.MYSQL -> MySQLDatabase(databaseInfo)
        DatabaseVersion.SQLITE3 -> SQLiteDatabase(databaseInfo)
    }
}