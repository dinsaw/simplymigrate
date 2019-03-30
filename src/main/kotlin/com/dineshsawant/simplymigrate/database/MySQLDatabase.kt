package com.dineshsawant.simplymigrate.database

import com.dineshsawant.simplymigrate.config.DatabaseInfo
import java.sql.Connection

class MySQLDatabase(dbInfo: DatabaseInfo) : SQLDatabase(dbInfo) {
    override fun setupConnection(dbInfo: DatabaseInfo): Connection {
        Class.forName("com.mysql.jdbc.Driver")
        return super.setupConnection(dbInfo)
    }
}