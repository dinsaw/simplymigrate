package com.dineshsawant.simplymigrate.database

import java.sql.ResultSetMetaData

class QueryResultMetaData(val rsmd: ResultSetMetaData) {
    var tableName: String
    val catalogName: String
    val schemaName: String
    var columnSet: MutableSet<Column> = mutableSetOf()
    var primaryKeyColumn: Column?
    private var columnLabelMap: MutableMap<String, Column> = mutableMapOf()

    init {
        this.catalogName = rsmd.getCatalogName(1)
        this.schemaName = rsmd.getSchemaName(1)
        this.tableName = rsmd.getTableName(1)
        columnSet = rsmd.toColumnSet()
        columnSet.forEach { columnLabelMap[it.label.toLowerCase()] = it }
        primaryKeyColumn = columnSet.firstOrNull { it.primaryKey }
    }

    override fun toString(): String {
        return "Table=$tableName, columnSet=$columnSet"
    }

    fun schemaTable(): String = when {
        schemaName.isNotBlank() -> "$schemaName.$tableName"
        catalogName.isNotBlank() -> "$catalogName.$tableName"
        else -> tableName
    }

    fun getColumnByLabel(columnName: String): Column? = this.columnLabelMap[columnName.toLowerCase()]

}

data class Column(
    val label: String,
    val columnType: Int,
    var primaryKey: Boolean = false,
    var uniqueKey: Boolean = false
)