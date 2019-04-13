package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.zaxxer.hikari.HikariConfig
import java.sql.Connection

class SQLiteDatabase(dbInfo: DatabaseInfo) : SQLDatabase(dbInfo) {
    override fun hikariConfig(dbInfo: DatabaseInfo): HikariConfig {
         Class.forName("org.sqlite.JDBC")
         return super.hikariConfig(dbInfo)
    }

    override fun getTableMetaData(table: String): QueryResultMetaData {
        val metaData = super.getTableMetaData(table)

        if (metaData.primaryKeyColumn == null) {
            connection.use {
                it.createStatement().use { statement ->
                    statement.execute("PRAGMA table_info($table)")
                    statement.resultSet.use { rs ->
                        while (rs.next()) {
                            if (1 == rs.getInt("pk")) {
                                val pkColumn = metaData.columnSet.first { it.label == rs.getString("name").toLowerCase() }
                                metaData.primaryKeyColumn = pkColumn
                                break
                            }
                        }
                    }
                }
            }
        }
        return metaData
    }

    private val INSERT_OR_UPDATE_SQL_FORMAT: String = "INSERT INTO %s (%s) VALUES (%s)" +
            " ON CONFLICT(%s) DO UPDATE SET %s"

    /**
     * INSERT INTO phonebook(name,phonenumber) VALUES('Alice','704-555-1212')
     * ON CONFLICT(name) DO UPDATE SET phonenumber=excluded.phonenumber;
     */
    override fun createUpsertQuery(tableMetaData: QueryResultMetaData, columns: Set<String>): String {

        val columnPart = columns.map { "`$it`" }.toList().joinToString(",")
        val valuesPart = columns.map { "?" }.toList().joinToString(",")
        val primaryColumnLabel = tableMetaData.primaryKeyColumn?.label
        val updatePart = columns.filter { !it.equals(primaryColumnLabel, ignoreCase = true) }
            .map { "`$it`=excluded.$it" }.toList().joinToString(",")

        return INSERT_OR_UPDATE_SQL_FORMAT.format(
            tableMetaData.tableName, columnPart, valuesPart,
            primaryColumnLabel, updatePart
        )
    }
}