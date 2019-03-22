package com.dineshsawant.datamig.database

import java.sql.ResultSetMetaData

class QueryResultMetaData {
    var tableName: String
    var rsmd: ResultSetMetaData
    var columnSet: MutableSet<Column> = mutableSetOf()
    var primaryKeyColumn: Column?
    private var columnLabelMap: MutableMap<String, Column> = mutableMapOf()

    constructor(rsmd: ResultSetMetaData) {
        this.rsmd = rsmd
        val schema = rsmd.getCatalogName(1)
        this.tableName = "$schema.${rsmd.getTableName(1)}"
        columnSet = rsmd.toColumnSet()
        columnSet.forEach { columnLabelMap[it.label.toLowerCase()] = it }
        primaryKeyColumn = columnSet.firstOrNull { it.primaryKey }
    }

    override fun toString(): String {
        return "Table=$tableName, columnSet=$columnSet"
    }

    fun getColumnByLabel(columnName: String): Column? = this.columnLabelMap[columnName.toLowerCase()]

}

data class Column(
    val label: String,
    val columnType: Int,
    var primaryKey: Boolean = false,
    var uniqueKey: Boolean = false
)