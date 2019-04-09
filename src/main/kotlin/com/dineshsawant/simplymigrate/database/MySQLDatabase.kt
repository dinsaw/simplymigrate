package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import com.zaxxer.hikari.HikariConfig
import java.sql.Connection

class MySQLDatabase(dbInfo: DatabaseInfo) : SQLDatabase(dbInfo) {
    override fun hikariConfig(dbInfo: DatabaseInfo): HikariConfig {
        Class.forName("com.mysql.cj.jdbc.Driver")
        return super.hikariConfig(dbInfo)
    }
}