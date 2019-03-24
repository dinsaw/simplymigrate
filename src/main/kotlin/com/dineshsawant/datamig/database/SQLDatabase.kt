package com.dineshsawant.datamig.database

import com.dineshsawant.datamig.config.DatabaseInfo
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.time.LocalDate

open class SQLDatabase(dbInfo: DatabaseInfo) : Database {
    protected val connection: Connection = DriverManager.getConnection(dbInfo.url, dbInfo.userId, dbInfo.password)

    override fun getTableMetaData(table: String): QueryResultMetaData {
        connection.createStatement().connection.createStatement().use { statement ->
            statement.execute("select * from $table where 1=0")
            return QueryResultMetaData(statement.resultSet.metaData)
        }
    }

    override fun getQueryMetaData(query: String): QueryResultMetaData {
        connection.createStatement().connection.createStatement().use { statement ->
            val sql = if (query.contains(" where ")) {
                "$query and 1=0"
            } else {
                "$query where 1=0"
            }
            println("executing sql=$sql")
            statement.execute(sql)
            return QueryResultMetaData(statement.resultSet.metaData)
        }
    }

    override fun getMinMax(
        table: String,
        partionColumn: Column,
        lower: String,
        upper: String,
        boundByColumns: List<Column>
    ): Array<Any> {
        connection.createStatement().connection.createStatement().use { statement ->
            val sql =
                "select min(${partionColumn.label}), max(${partionColumn.label}) from $table where ${boundCondition(
                    boundByColumns,
                    lower,
                    upper
                )}"
            println("minmax sql = $sql")
            statement.execute(sql)
            statement.resultSet.use { resultSet ->
                resultSet.next()
                val min = typedValue(resultSet.getObject(1))
                val max = typedValue(resultSet.getObject(2))
                return arrayOf(min, max)
            }
        }
    }

    override fun getMinMaxForQuery(
        query: String,
        partitionColumn: Column
    ): Array<Any> {
        connection.createStatement().connection.createStatement().use { statement ->
            val sql = minMaxSql(query, partitionColumn)
            println("minmax sql = $sql")
            statement.execute(sql)
            statement.resultSet.use { resultSet ->
                resultSet.next()
                val min = typedValue(resultSet.getObject(1))
                val max = typedValue(resultSet.getObject(2))
                return arrayOf(min, max)
            }
        }
    }

    private fun minMaxSql(
        query: String,
        partitionColumn: Column
    ): String {
        val fromToEnd = query.toLowerCase().substringAfter("from ")
        return "select min(${partitionColumn.label}), max(${partitionColumn.label}) from $fromToEnd"
    }


    private fun typedValue(value: Any?): Any {
        return when (value) {
            is String -> {
                try {
                    LocalDate.parse(value)
                } catch (e: Exception) {
                    value
                }!!
            }
            is Date -> {
                value.toLocalDate()
            }
            else -> value!!
        }
    }

    override fun selectRecords(
        partitionKey: PartitionKey, start: Any, end: Any, table: String,
        columnSet: MutableSet<Column>, lower: String, upper: String, boundByColumns: List<Column>
    ): List<LinkedHashMap<String, Any>> {
        println("Selecting from $start and $end")
        connection.createStatement().connection.createStatement().use { statement ->
            val sql =
                "select ${selectFields(columnSet)} from $table where ${partitionKey.column.label} between ${toSqlValue(
                    start
                )} and ${toSqlValue(end)}" +
                        " and (${boundCondition(boundByColumns, lower, upper)})"
            println("Prepared SQL = $sql")
            statement.execute(sql)
            statement.resultSet.use { rs ->
                val records = arrayListOf<LinkedHashMap<String, Any>>()
                while (rs.next()) {
                    var recordMap = linkedMapOf<String, Any>()
                    columnSet.forEach { recordMap[it.label] = rs.getObject(it.label) }
                    records.add(recordMap)
                }
                return records
            }
        }
    }

    override fun selectRecordsByQuery(
        partitionKey: PartitionKey,
        start: Any,
        end: Any,
        fetchQuery: String,
        columnSet: MutableSet<Column>
    ): List<LinkedHashMap<String, Any>> {
        println("Selecting from $start and $end")
        connection.createStatement().connection.createStatement().use { statement ->
            val sql = if (fetchQuery.contains(" where ")) {
                "$fetchQuery and ${partitionKey.column.label} between ${toSqlValue(start)} and ${toSqlValue(end)}"
            } else {
                "$fetchQuery where ${partitionKey.column.label} between ${toSqlValue(start)} and ${toSqlValue(end)}"
            }
            println("Prepared SQL = $sql")
            statement.execute(sql)
            statement.resultSet.use { rs ->
                val records = arrayListOf<LinkedHashMap<String, Any>>()
                while (rs.next()) {
                    var recordMap = linkedMapOf<String, Any>()
                    columnSet.forEach { recordMap[it.label] = rs.getObject(it.label) }
                    records.add(recordMap)
                }
                return records
            }
        }
    }

    private fun boundCondition(boundByColumns: List<Column>, lower: String, upper: String): String {
        if (boundByColumns.isEmpty()) {
            return "1=1"
        }
        return boundByColumns.joinToString(" OR ") { "${it.label} between '$lower' and '$upper'" }
    }

    private fun toSqlValue(value: Any): String {
        return when (value) {
            is Date -> "'${value.toLocalDate()}'"
            else -> "'$value'"
        }
    }

    private fun selectFields(columnSet: MutableSet<Column>): String {
        val builder = StringBuilder()
        columnSet.map { "`${it.label}`" }.toTypedArray().joinTo(builder, ",")
        return builder.toString()
    }

    override fun upsert(tableMetaData: QueryResultMetaData, loadSize: Int, records: List<LinkedHashMap<String, Any>>) {
        if (records.isEmpty()) return
        connection.autoCommit = false

        var sql = createUpsertQuery(tableMetaData, records[0].keys)
        println("Upsert sql = $sql")
        connection.prepareStatement(sql).use { preparedStatement ->
            records.forEach { record ->
                var i = 0
                record.forEach { _, v ->
                    preparedStatement.setSQLObject(++i, v)
                }
                preparedStatement.addBatch()
            }
            val results = preparedStatement.executeBatch()
            connection.commit()
            println("Upsert results = ${results.toList()}")
        }
    }

    private val INSERT_OR_UPDATE_SQL_FORMAT: String = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
    /**
     * Creates sql = INSERT INTO test.birthdays (`ID`,`birthday`,`Name`)
     * VALUES (?,?,?) ON DUPLICATE KEY UPDATE `birthday`=VALUES(`birthday`),`Name`=VALUES(`Name`)
     */
    protected open fun createUpsertQuery(tableMetaData: QueryResultMetaData, columns: Set<String>): String {
        val columnPart = columns.map { "`$it`" }.toList().joinToString(",")
        val valuesPart = columns.map { "?" }.toList().joinToString(",")
        val updatePart = columns.filter { !it.equals(tableMetaData.primaryKeyColumn?.label, ignoreCase = true) }
            .map { "`$it`=VALUES(`$it`)" }.toList().joinToString(",")

        return INSERT_OR_UPDATE_SQL_FORMAT.format(tableMetaData.schemaTable(), columnPart, valuesPart, updatePart)
    }

    protected fun finalize() {
        connection.close()
    }
}