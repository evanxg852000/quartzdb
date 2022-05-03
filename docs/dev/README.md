# Architecture Documents


Database: is a folder

Table: is folder of apache parquet files called chunks
Table: is a measurement identified by name and have a set of fields=values

BufferPool: chronological and time series eviction policy that contains apache arrow in-memory format 

An active, writable chunk per table that is apache arrow  in-memory format 

A WAL for writes

Catalog management: Sqlite, plain Json files

Compactor: merge table chunk files

Garbage collector: delete old files based on retention policy and also delete series when no longer needed
