package com.dineshsawant.datamig.database

import java.sql.PreparedStatement
import java.sql.ResultSetMetaData

fun ResultSetMetaData.toColumnSet() =
    (1..this.columnCount).map {
        var column = Column(
            this.getColumnLabel(it).toLowerCase(),
            this.getColumnType(it)
        )

    when (this) {
        is com.mysql.cj.jdbc.result.ResultSetMetaData -> {
            column.primaryKey = this.fields[it-1].isPrimaryKey
            column.uniqueKey = this.fields[it-1].isUniqueKey
        }
        else -> {}
    }
        column
    }.toMutableSet()

fun PreparedStatement.setSQLObject(i: Int, value: Any) {
    when (value) {
        // SQLite jdbc driver stores date as miliseconds. We dont want this behaviour
        is java.sql.Date -> this.setString(i, value.toLocalDate().toString())
        else -> this.setObject(i, value)
    }

}
