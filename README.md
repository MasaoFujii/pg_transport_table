# How to transport database objects
- Conditions
    - All the database objects to transport at the same time should be in the same tablespace. In other words,  if you want to transport the database objects in several tablespaces, you need to transport them separately for each tablespace.
    - There are two servers, one is the production server where PostgreSQL is already running, and the other is the temporary server.
1. Install PostgreSQL in the temporary server if not yet. Note that the following things must be the same between the production and temporary servers.
    - The major version of PostgreSQL (Also probably it's better to use the same minor version of PostgreSQL). For example, if PostgreSQL 12.2 is running in the production server, PostgreSQL 12.x should be installed in the temporary server.
    - The configure and compile options used when building PostgreSQL. Those options in the production server are viewable from the result of pg_config command.
    ```
    [prod] $ pg_config
    ```
1. Create the database cluster in the temporary server. Note that the settings (e.g., --encoding, --locale, --data-checksums, etc) specified when creating database cluster must be the same between the temporary and production servers.
1. Start PostgreSQL in the temporary server.
    - It's better to tune the configuration specially for high performance data bulkloading.
1. Install the functions to use for transporting the tables, by executing pg_transport_table.sql, in both production and temporary servers if not installed yet.
1. Confirm that the latest checkpoint redo location in the production server is larger than the current WAL write location (i.e., pg_current_wal_lsn()) in the temporary server. Save and mark the latest checkpoint redo location in the production server, as [1].
    - If the latest checkpoint redo location in the production server is less than or equal to the current WAL write location in the temporary server, you need to back to the step that creates the database cluster. Or you need to wait until many transactions happen in the production server and its latest checkpoint redo location becomes enough large.
    - The latest checkpoint redo location is viewable from the result of pg_controldata command.
1. Create the tables to transport in the temporary server. Also create other database objects like indexes, partitions, etc related to the tables in the temporary server.
1. Load data to the tables in the temporary server.
1. Execute ```VACUUM FREEZE``` on the tables in the temporary server. It's better to execute ```VACUUM FREEZE``` at least twice just in the case.
1. Execute ```CHECKPOINT``` in the temporary server. It's better to execute ```CHECKPOINT``` at least twice just in the case.
1. Create tables to transport in the production server. Also create other database objects like indexes, partitions, etc related to the tables in the production server. All the database objects related to the tables to transport must be created in the same way in both temporary and production servers.
1. Execute dump_relfilenodes() with each table to transport, in the production server. Write the output of the function into the file, and copy the output file from the production server to the temporary server.
1. Execute the file copied from the production server, as SQL file, in the temporary server. Write the output of the execution of that function into the file.
1. Confirm that [1] is larger than the current WAL write location (i.e., pg_current_wal_lsn()) in the temporary server.
    - If [1] is less than or equal to the current WAL write location in the temporary server, you need to back to the step that creates the database cluster.
1. Shutdown PostgreSQL in the temporary server.
1. Move under the database cluster directory in the temporary server, and execute the file output by the above step, as the shell script. This shell script renames the files of the database objects to transport, so that the production server can handle them.
1. Copy all the renamed files from the temporary server to the production server.
