package com.dineshsawant.datamig.database

import java.sql.ResultSetMetaData

fun ResultSetMetaData.toColumnSet() =
    (1..this.columnCount).map {
        var column = Column(
            this.getColumnLabel(it),
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

/*
fun com.mysql.cj.jdbc.result.ResultSetMetaData.toColumnSet() =
    (1..this.columnCount).map {
        var column = Column(
            this.getColumnLabel(it),
            this.getColumnType(it)
        )
        column.primaryKey = this.fields[it-1].isPrimaryKey
        column.uniqueKey = this.fields[it-1].isUniqueKey
        column
    }.toMutableSet()*/
