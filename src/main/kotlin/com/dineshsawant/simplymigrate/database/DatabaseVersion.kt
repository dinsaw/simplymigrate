package com.dineshsawant.simplymigrate.database

enum class DatabaseVersion(val databaseName: String, val urlPrefix: String) {
    MYSQL("MySQL", "jdbc:mysql://"),
    SQLITE3("SQLite", "jdbc:sqlite:")
}
