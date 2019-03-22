## DataMig

### Features

## Convention based data migration with minimal configuration

DataMig requires following configuration to start working on your source and sink databases.

`config.yaml`
```
source
    database:
    host:
    port:
    userId:
    password
sink
    database:
    host:
    port:
    userId:
    password
```

Then run following command:

`java -jar migrate.jar --conf=config.yaml --sourceTable=schema.sourceTable --sinkTable=schema.sinkTable --fetchSize=100 --loadSize=100
--fromTime=2018-10-1 --toTime=2019-01-1`

## Move data from one relational/non-relational database to other relational/non-relational database
## Choose set of records to be moved according to timestamp fields
## It is fast.
