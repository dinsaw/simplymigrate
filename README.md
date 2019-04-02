## SimplyMigrate
Utility to migrate data from one database to other.

### How to use

- Run following commands to build jar
- `./gradlew clean jar`
- Find jar from `build/libs` directory.

### Features

#### Migrate data with minimal configuration

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

#### It is fast

The above command will copy all records from `birthdays` table of SQLite database
to MySQL database's `test.birthdays` table. 

Here partition key can be number or date or datetime type column. SimplyMigrate does not use OFFSET-LIMIT query provided by SQL to fetch data from source database . [Because OFFSET-LIMIT query takes more time as the offset value becomes higher](https://use-the-index-luke.com/sql/partial-results/fetch-next-page).

SimplyMigrate fetches data in ranges by increasing value of partition key. Hence significant performance boost is achieved.

#### Choose set of records bounded by number or date or datetime fields

Following command will migrate records which have `id` from 100 to 1000 (inclusive).

`java -jar simplymigrate.jar --configFile=./test-config.yaml --sourceTable=birthdays --targetTable=test.birthdays --partitionKey=id --boundBy=id --lower=100 --upper=1000`

Following command will migrate records which have `birthday` from 1040-1-1 to 1940-1-1 (inclusive).

`java -jar simplymigrate.jar --configFile=./test-config.yaml --sourceTable=birthdays --targetTable=test.birthdays --partitionKey=id --boundBy=birthday --lower=1040-1-1 --upper=1940-1-1`

#### Supported databases
- MYSQL
- SQLITE3
