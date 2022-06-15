# F2T(File to Table)
A library to read 2 dimensional data from Excel / CSV file, to detect and determine corresponding table schema, to check whether it matches target database table(if existed), to create corresponding table automatically(if not existed), and to insert file data into database.  
It supports extracting data from .csv, .xls and .xlsx files, as well as parquet files; and dump data into popular databases including PostgreSQL, MariaDB and SQL Server, maybe Hive/Spark or some cloud native DW. Extending support of target database / store is easy.

It also provided a parquet reader that loads parquet content in memory instead of accessing parquet file from HDFS or local file system starting from v0.7.
