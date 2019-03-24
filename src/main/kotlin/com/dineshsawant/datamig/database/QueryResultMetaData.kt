package com.dineshsawant.datamig.database

import java.sql.ResultSetMetaData

class QueryResultMetaData {
    var tableName: String
    val catalogName: String
    val schemaName: String
    val rsmd: ResultSetMetaData
    var columnSet: MutableSet<Column> = mutableSetOf()
    var primaryKeyColumn: Column?
    private var columnLabelMap: MutableMap<String, Column> = mutableMapOf()

    constructor(rsmd: ResultSetMetaData) {
        this.rsmd = rsmd
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

    fun schemaTable(): String = if (schemaName.isNotBlank()) {
        "$schemaName.$tableName"
    } else if (catalogName.isNotBlank()) {
        "$catalogName.$tableName"
    } else {
        tableName
    }

    fun getColumnByLabel(columnName: String): Column? = this.columnLabelMap[columnName.toLowerCase()]

}

data class Column(
    val label: String,
    val columnType: Int,
    var primaryKey: Boolean = false,
    var uniqueKey: Boolean = false
)