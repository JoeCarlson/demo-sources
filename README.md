
Demo Sources
===========

Background
----------

These files are some example demonstration InterMine data sources. There is an emphasis on direct loading: the ETL process
stores data directly in the production database without going through the intermediate item database. This speeds up the
loading process in the cases where you are entering predominately new records without needing to do extensive modifications
to existing records. For example, entering a new genome annotation (Genes, Transcripts, Proteins...) will probably get loaded
much faster. But a modification such as adding UniProt accession numbers to existing protein records will probably not
see any speedup.

A big concern is whether the data makes references to records already in the production mine. For example, entering a genomic
sequence for an organism in one operation, then entering the annotations in a second operation will require setting references
to the chromosomes in the gene records. This will get complicated and it may be much simpler to use the standard InterMine
merging process.

Usage
-----

The file direct loader reads a TSV file to load in the organism table. The corresponding source in the project XML file
is
```
    <source name="file-direct-demo-test" type="file-direct-demo">
      <property name="src.data.dir" location="/path/to/directory/of/file/" />
      <property name="file-direct-demo.includes" value="organisms.tsv" />
    </source>
```


The db direct loaded needs a source database:
```
    <source name="db-direct-demo-test" type="db-direct-demo">
      <property name="source.db.name" value="your-db" />
    </source>
```

The credentials for the source database can be stored in your <mine>.properties file.
```
db.your-db.datasource.databaseName=database_name
db.your-db.datasource.serverName=database_server
db.your-db.datasource.user=database_user
db.your-db.datasource.password=database_password
db.your-db.datasource.dataSourceName=database_source_name
db.your-db.datasource.class=org.postgresql.ds.PGPoolingDataSource
db.your-db.datasource.maxConnections=10
db.your-db.driver=org.postgresql.Driver
db.your-db.platform=PostgreSQL
```
or
```
db.your-db.datasource.class=com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource
db.your-db.datasource.dataSourceName=database_source_name
db.your-db.datasource.serverName=database_server
db.your-db.datasource.databaseName=database_name
db.your-db.datasource.maxConnections=10
db.your-db.driver=com.mysql.jdbc.Driver
db.your-db.platform=MySQL
```

