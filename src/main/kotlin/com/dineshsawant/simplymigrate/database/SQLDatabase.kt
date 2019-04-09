package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE
import java.time.format.DateTimeFormatter.ISO_LOCAL_TIME
import java.time.format.DateTimeFormatterBuilder

private val logger = KotlinLogging.logger {}

open class SQLDatabase(dbInfo: DatabaseInfo) : Database {
    private val dataSource: HikariDataSource = HikariDataSource(hikariConfig(dbInfo))
    protected val connection: Connection
        get() = dataSource.connection

    protected open fun hikariConfig(dbInfo: DatabaseInfo): HikariConfig {
        val config = HikariConfig()
        config.jdbcUrl = dbInfo.url
        config.username = dbInfo.userId
        config.password = dbInfo.password
        config.isAutoCommit = false
        return config
    }

    override fun getTableMetaData(table: String): QueryResultMetaData {
        connection.use { conn ->
            conn.createStatement().use { statement ->
                statement.execute("select * from $table where 1=0")
                return QueryResultMetaData(statement.resultSet.metaData)
            }
        }
    }

    override fun getQueryMetaData(table: String): QueryResultMetaData {
        connection.use { conn ->
            conn.createStatement().use { statement ->
                val sql = if (table.contains(" where ")) {
                    "$table and 1=0"
                } else {
                    "$table where 1=0"
                }
                logger.debug { "executing sql=$sql" }
                statement.execute(sql)
                return QueryResultMetaData(statement.resultSet.metaData)
            }
        }
    }

    override fun getMinMax(
        table: String,
        partitionColumn: Column,
        lower: String,
        upper: String,
        boundByColumns: List<Column>
    ): Array<Any> {
        connection.use { conn ->
            conn.createStatement().use { statement ->
                val sql =
                    "select min(${partitionColumn.label}), max(${partitionColumn.label}) from $table" +
                            " where ${boundCondition(boundByColumns, lower, upper)}"
                logger.debug { "minmax sql = $sql" }
                statement.execute(sql)
                statement.resultSet.use { resultSet ->
                    resultSet.next()
                    val min = typedValue(resultSet.getObject(1))
                    val max = typedValue(resultSet.getObject(2))
                    return arrayOf(min, max)
                }
            }
        }
    }

    override fun getMinMaxForQuery(
        query: String,
        partitionColumn: Column
    ): Array<Any> {
        connection.use { conn ->
            conn.createStatement().use { statement ->
                val sql = minMaxSql(query, partitionColumn)
                logger.debug { "minmax sql = $sql" }
                statement.execute(sql)
                statement.resultSet.use { resultSet ->
                    resultSet.next()
                    val min = typedValue(resultSet.getObject(1))
                    val max = typedValue(resultSet.getObject(2))
                    return arrayOf(min, max)
                }
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


    val SQL_LOCAL_DATE_TIME: DateTimeFormatter = DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME).toFormatter()

    private fun typedValue(value: Any?): Any {
        return when (value) {
            is String -> {
                try {
                    LocalDate.parse(value)
                } catch (e: Exception) {
                    try {
                        LocalDateTime.parse(value, SQL_LOCAL_DATE_TIME)
                    } catch (e: Exception) {
                        value
                    }
                }!!
            }
            is Timestamp -> {
                value.toLocalDateTime()
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
        logger.debug { "Selecting from $start and $end" }
        connection.use { conn ->
            conn.createStatement().use { statement ->
                val sql = "select ${selectFields(columnSet)} from $table" +
                        " where ${partitionKey.column.label} between ${toSqlValue(start)}" +
                        " and ${toSqlValue(end)} and (${boundCondition(boundByColumns, lower, upper)})"
                logger.debug { "Prepared SQL = $sql" }

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
    }

    override fun selectRecordsByQuery(
        partitionKey: PartitionKey,
        start: Any,
        end: Any,
        fetchQuery: String,
        columnSet: MutableSet<Column>
    ): List<LinkedHashMap<String, Any>> {
        logger.debug { "Selecting from $start and $end" }
        connection.use { conn ->
            conn.createStatement().use { statement ->
                val sql = if (fetchQuery.contains(" where ")) {
                    "$fetchQuery and ${partitionKey.column.label} between ${toSqlValue(start)} and ${toSqlValue(end)}"
                } else {
                    "$fetchQuery where ${partitionKey.column.label} between ${toSqlValue(start)} and ${toSqlValue(end)}"
                }
                logger.debug { "Prepared SQL = $sql" }
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
            is LocalDateTime -> "'${SQL_LOCAL_DATE_TIME.format(value)}'"
            else -> "'$value'"
        }
    }

    private fun selectFields(columnSet: MutableSet<Column>): String {
        val builder = StringBuilder()
        columnSet.map { "`${it.label}`" }.toTypedArray().joinTo(builder, ",")
        return builder.toString()
    }

    override fun upsert(tableMetaData: QueryResultMetaData, records: List<LinkedHashMap<String, Any>>) {
        if (records.isEmpty()) return

        val sql = createUpsertQuery(tableMetaData, records[0].keys)
        logger.debug { "Upsert sql = $sql" }
        connection.use { conn ->
            conn.prepareStatement(sql).use { preparedStatement ->
                records.forEach { record ->
                    var i = 0
                    record.forEach { _, v ->
                        preparedStatement.setSQLObject(++i, v)
                    }
                    preparedStatement.addBatch()
                }
                val results = preparedStatement.executeBatch()
                connection.commit()
                logger.debug { "Upsert results = ${results.toList()}" }
            }
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
        dataSource.close()
    }
}