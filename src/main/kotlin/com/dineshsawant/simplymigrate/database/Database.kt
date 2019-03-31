package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.config.DatabaseInfo

interface Database {
    fun getQueryMetaData(table: String): QueryResultMetaData

    fun upsert(tableMetaData: QueryResultMetaData, records: List<LinkedHashMap<String, Any>>)

    fun getMinMax(
        table: String,
        partitionColumn: Column,
        lower: String,
        upper: String,
        boundByColumns: List<Column>
    ): Array<Any>

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

    fun getTableMetaData(table: String): QueryResultMetaData

    fun getMinMaxForQuery(query: String, partitionColumn: Column): Array<Any>

    fun selectRecordsByQuery(
        partitionKey: PartitionKey,
        start: Any,
        end: Any,
        fetchQuery: String,
        columnSet: MutableSet<Column>
    ): List<LinkedHashMap<String, Any>>
}

fun createDatabase(databaseInfo: DatabaseInfo): Database {
    return when (databaseInfo.database) {
        DatabaseVersion.MYSQL -> MySQLDatabase(databaseInfo)
        DatabaseVersion.SQLITE3 -> SQLiteDatabase(databaseInfo)
    }
}