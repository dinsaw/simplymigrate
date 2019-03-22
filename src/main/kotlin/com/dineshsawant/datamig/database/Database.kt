package com.dineshsawant.datamig.database

import com.dineshsawant.datamig.config.DatabaseInfo
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.time.LocalDate

class Database(dbInfo: DatabaseInfo) {
    private val connection: Connection = DriverManager.getConnection(dbInfo.url, dbInfo.userId, dbInfo.password)

    fun getTableMetadata(table: String): QueryResultMetaData {
        connection.createStatement().connection.createStatement().use { statement ->
            statement.execute("select * from $table where 1=0")
            return QueryResultMetaData(statement.resultSet.metaData)
        }
    }

    fun getMinMax(table:String, column: Column): Array<Any> {
        connection.createStatement().connection.createStatement().use { statement ->
            statement.execute("select min(${column.label}), max(${column.label}) from $table")
            statement.resultSet.use { resultSet ->
                resultSet.next()
                val min = process(resultSet.getObject(1))
                val max = process(resultSet.getObject(2))
                return arrayOf(min, max)
            }
        }
    }

    private fun process(value: Any?): Any {
        return when (value) {
            is String -> {
                try {
                    LocalDate.parse(value)
                } catch (e:Exception) {
                    value
                }!!
            }
            else -> value!!
        }
    }

    fun selectRecords(partitionKey: PartitionKey, start: Any, end: Any, table: String, columnSet: MutableSet<Column>): List<LinkedHashMap<String, Any>> {
        println("Selecting from $start and $end")
        connection.createStatement().connection.createStatement().use {statement ->
            val sql =  "select ${selectFields(columnSet)} from $table where ${partitionKey.column.label} between ${toSqlValue(start)} and ${toSqlValue(end)}"
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

    fun upsert(tableMetaData: QueryResultMetaData, loadSize: Int, records: List<LinkedHashMap<String, Any>>) {
        if (records.isEmpty()) return
        connection.autoCommit = false

        var sql = createUpsertQuery(tableMetaData, records[0].keys)
        println("Upsert sql = $sql")
        connection.prepareStatement(sql).use { preparedStatement ->
            records.forEach { record ->
                var i = 0
                record.forEach { _, v ->
                    preparedStatement.setObject(++i, v)
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
    private fun createUpsertQuery(tableMetaData: QueryResultMetaData, columns: Set<String>): String {
        val columnPart = columns.map { "`$it`" }.toList().joinToString(",")
        val valuesPart = columns.map { "?" }.toList().joinToString(",")
        val updatePart = columns.filter { !it.equals(tableMetaData.primaryKeyColumn?.label, ignoreCase = true) }
                                .map { "`$it`=VALUES(`$it`)" }.toList().joinToString(",")

        return INSERT_OR_UPDATE_SQL_FORMAT.format(tableMetaData.tableName, columnPart, valuesPart, updatePart)
    }

    protected fun finalize() {
        connection?.close()
    }
}
