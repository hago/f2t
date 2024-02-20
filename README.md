# F2T(File to Table)
F2T is a library that
* Reads 2 dimensional data from Excel / CSV / Parquet files
* Detects and determines corresponding table schema
* Checks whether it matches target database table(if existed), creates corresponding table automatically(if not existed)
* Inserts file data into database

It supports extracting data from .csv, .xls and .xlsx files, as well as parquet files; and dump data into popular databases. At present supported databases includes
* PostgreSQL
* MySQL / MariaDB
* SQL Server
* SQLite
* Apache Derby

Adding support of a new database or data file format is easy.

It also provided a parquet reader that loads parquet content in memory instead of accessing parquet file from HDFS or local file system starting from v0.7.
