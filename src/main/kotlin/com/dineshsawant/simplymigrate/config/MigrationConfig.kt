package com.dineshsawant.simplymigrate.config

import com.dineshsawant.simplymigrate.database.DatabaseVersion
import com.uchuhimo.konf.ConfigSpec
import java.io.Serializable

object MigrationConfig : ConfigSpec("migration") {
    val source by required<DatabaseInfo>()
    val target by required<DatabaseInfo>()
}

data class DatabaseInfo(
    val database: DatabaseVersion,
    val host: String,
    val port: String?,
    val userId: String?,
    val password: String?
) : Serializable {

    val url: String by lazy { "${database.urlPrefix}${hostPort()}" }

    private fun hostPort(): String {
        return if (port.isNullOrBlank()) {
            host
        } else {
            "$host:$port"
        }
    }
}