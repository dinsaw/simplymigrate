## SimplyMigrate
Utility to migrate data from one database to other.

### How to use

- Run following commands to build jar
- `./gradlew clean jar`
- Find jar in `build/libs` directory

### Features

## Migrate data with minimal configuration

SimplyMigrate requires following configuration to start working on your source and sink databases.

`config.yaml`
```
migration:
  source:
    database: SQLITE3
    host: /Users/dineshsawant/testsource.db
    userId:
    password:
  target:
    database: MYSQL
    host: 127.0.0.1
    port: 3306
    userId: root
```

Then run following command:

`java -jar simplymigrate.jar --configFile=./test-config.yaml --sourceTable=birthdays --targetTable=test.birthdays --partitionKey=id`

The above command will copy all records from `birthdays` table of SQLite database
to MySQL database's `test.birthdays` table.

Utility does not create table in target database.

## Choose set of records bounded by number or date or timestamp fields

Following command will migrate records which have `id` from 100 to 1000 (inclusive).

`java -jar simplymigrate.jar --configFile=./test-config.yaml
--sourceTable=birthdays --targetTable=test.birthdays --partitionKey=id --boundBy=id --lower=100 --upper=1000`

Following command will migrate records which have `birthday` from 1040-1-1 to 1940-1-1 (inclusive).

`java -jar simplymigrate.jar --configFile=./test-config.yaml --sourceTable=birthdays --targetTable=test.birthdays --partitionKey=id --boundBy=birthday --lower=1040-1-1 --upper=1940-1-1`

## Supported databases
- MYSQL
- SQLITE3