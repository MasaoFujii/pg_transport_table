# How to transport database objects
- Conditions
    - There are two servers, one is the production server where PostgreSQL is already running, and the other is the temporary server.
    - The major version of PostgreSQL must be larger than or equal to v12.
    - All the database objects to transport at the same time should be in the same tablespace. In other words,  if you want to transport the database objects in several tablespaces, you need to transport them separately for each tablespace.
1. Install PostgreSQL in the temporary server if not yet. Note that the following things must be the same between the production and temporary servers.
    - The major version of PostgreSQL (Also probably it's better to use the same minor version of PostgreSQL). For example, if PostgreSQL 12.2 is running in the production server, PostgreSQL 12.x should be installed in the temporary server.
    - The configure and compile options used when building PostgreSQL. Those options in the production server are viewable from the result of ```pg_config``` command.
1. Install pg_visibility contrib module in the temporary server if not yet.
1. Create the database cluster in the temporary server. Note that the settings (e.g., --encoding, --locale, --data-checksums, etc) specified when creating database cluster must be the same between the temporary and production servers.
1. Start PostgreSQL in the temporary server, with autovacuum disabled.
    - It's better to tune the configuration specially for high performance data bulkloading.
    - The parameter autovacuum must be set to false in the postgresql.conf.
1. Confirm that the results of the following SQL queries are the same between the production and temporary servers.
    ```
    SELECT
        pg_control_version,
        catalog_version_no
    FROM pg_control_system();

    SELECT
        max_data_alignment,
        database_block_size,
        blocks_per_segment,
        wal_block_size,
        bytes_per_wal_segment,
        max_identifier_length,
        max_index_columns,
        max_toast_chunk_size,
        large_object_chunk_size,
        float8_pass_by_value,
        data_page_checksum_version
    FROM pg_control_init();

    SELECT name, setting FROM pg_settings
    WHERE name IN ('block_size', 'data_checksums',
        'data_directory_mode', 'integer_datetimes',
        'max_function_args', 'max_identifier_length',
       'max_index_keys', 'segment_size', 'wal_block_size',
       'wal_segment_size')
    ORDER BY name;

    SELECT
        datname, pg_encoding_to_char(encoding),
        datcollate, datctype
    FROM pg_database
    WHERE datname = current_database();
    ```
1. Install the functions to use for transporting the tables, by executing pg_transport_table.sql, in both production and temporary servers if not installed yet.
1. Make pg_visibility contrib module available in the temporary server, by executing ```CREATE EXTENSION```. For example,
    ```
    [temp] $ psql
    =# CREATE EXTENSION pg_visibility;
    ```
1. Confirm that the latest checkpoint redo location in the production server is larger than the current WAL write location in the temporary server. Save and mark the latest checkpoint redo location in the production server, as ***[1]***.
    - If the latest checkpoint redo location in the production server is less than or equal to the current WAL write location in the temporary server, you need to back to the step that creates the database cluster. Or you need to wait until many transactions happen in the production server and its latest checkpoint redo location becomes enough large.
    - The latest checkpoint redo location is viewable from the result of ```pg_controldata``` command or ```pg_control_checkpoint()``` function.
        ```
        [prod] $ pg_controldata $PGDATA | grep "REDO location"
        [prod] $ psql
        =# SELECT redo_lsn FROM pg_control_checkpoint();
        ```
    - The current WAL write location is viewable from the result of ```pg_current_wal_lsn()``` function.
        ```
        [temp] $ psql
        =# SELECT pg_current_wal_lsn();
        ```
1. Create the tables to transport in the temporary server. Also create other database objects like indexes, partitions, etc related to the tables in the temporary server. For example,
    ```
    [temp] $ psql
    =# CREATE TABLE example (key BIGINT PRIMARY KEY, val TEXT);
    ```
1. Load data to the tables in the temporary server. For example,
    ```
    [temp] $ psql
    =# \copy example from /tmp/input_data.csv with csv
    ```
1. Create the database objects required for the tables that will be transported, in the temporary server, if necessary. For example, you might want to create indexes after data loading,
    ```
    [temp] $ psql
    =# CREATE INDEX example_val_idx ON example (val);
    ```
1. Execute ```VACUUM FREEZE``` on the tables to transport, in the temporary server. It's better to execute ```VACUUM FREEZE``` at least twice just in the case. For example,
    ```
    [temp] $ psql
    =# VACUUM FREEZE example;
    =# VACUUM FREEZE example;
    ```
    - Note that you must confirm that there are no concurent transactions while executing '''VACUUM FREEZE'''.
1. Confirm that all the pages in the tables to transport have already been marked as *frozen*. In other words, you need to confirm that the numbers of all-frozen pages and all the pages in the tables to transport are the same.
    - The number of all-frozen pages in the table can be calculated by ```pg_visibility_map_summary()``` function that pg_visibility contrib module provides. For example,
        ```
        [temp] $ psql
        =# SELECT all_frozen FROM pg_visibility_map_summary('example');
        ```
    - The number of all the pages in the table can be calculated by dividing the relation size by the block size. For example,
        ```
        [temp] $ psql
        =# SELECT pg_relation_size('example') / current_setting('block_size')::BIGINT;
        ```
    - Note that you must not execute any transactions accessing the tables to transport, in the temporary server, since the beginning of this step and until the tranportation succeeds.
1. Execute ```CHECKPOINT``` in the temporary server. It's better to execute ```CHECKPOINT``` at least twice just in the case. For example,
    ```
    [temp] psql
    =# CHECKPOINT;
    =# CHECKPOINT;
    ```
1. Create tables to transport in the production server. Also create other database objects like indexes, partitions, etc related to the tables in the production server. All the database objects related to the tables to transport must be created in the same way in both temporary and production servers.
1. Execute dump_relfilenodes() with each table to transport, in the production server. Write the output of the function into the file, and copy the output file from the production server to the temporary server. For example,
    ```
    [prod] $ psql
    =# \t
    =# \o /tmp/transport_example.sql
    =# SELECT dump_relfilenodes('example');
    =# \q

    [prod] $ scp /tmp/transport_example.sql xxx@temporary_server:/tmp/transport_example.sql
    ```
1. Execute the file copied from the production server, as SQL file, in the temporary server. Write the output of the execution of that function into the file.
1. Confirm that ***[1]*** is larger than the current WAL write location (i.e., pg_current_wal_lsn()) in the temporary server.
    - If ***[1]*** is less than or equal to the current WAL write location in the temporary server, you need to back to the step that creates the database cluster.
1. Shutdown PostgreSQL in the temporary server.
1. Move under the database cluster directory in the temporary server, and execute the file output by the above step, as the shell script. This shell script renames the files of the database objects to transport, so that the production server can handle them.
1. Copy all the renamed files from the temporary server to the production server.
