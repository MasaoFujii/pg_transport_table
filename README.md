# How to transport table
0. PostgreSQL is already running in the destination server. Probably it's providing database service.
1. Install PostgreSQL in the source server. Note that the major version of PostgreSQL must be the same between the source and destination servers. Probably it's better to use the same minor version of PostgreSQL.
2. Create database cluster in the source server. Note that the settings (e.g., --encoding, --locale, etc) specified when creating database cluster must be the same between the source and destination servers.
3. Start PostgreSQL in the source server.
4. Create tablespace where the table to transport will be located in both the source and destination servers.
5. Create table to transport in the source server. Note that UNLOGGED and TABLESPACE options must be specified when creating table. Also create other database objects like index, partition, etc related to the table in the source server. All the database objects to transport must be located on the tablespace created in the step #4.
6. Load data to the table in the source server.
7. Execute ```VACUUM FREEZE``` on the table in the source server. It's better to execute ```VACUUM FREEZE``` at least twice just in the case.
8. Execute ```CHECKPOINT``` in the source server. It's better to execute ```CHECKPOINT``` at least twice just in the case.
9. Create table to transport in the destination server. Note that TABLESPACE option must be specified when creating table. Also create other database objects like index, partition, etc related to the table in the destination server. All the database objects related to the table to transport must be created in the same way in both source and destination servers.
10. Run pg_transport_table.sh in the source server, and save its output as the shell script. The connection strings to the source and destination servers must be specified, respectively.
11. Shutdown PostgreSQL in the source server.
12. Run the shell script created by the step #10, in the source server. It must be ran under the directory storing the file of table to transport. The shell script will rename the file of the database objects to transport so that the destination server can handle them.
13. Copy all the renamed files from the source's tablespace to the destination's. Instead, you can umount the disk space that the tablespace in the source server uses, and mount that so that we can access to it from the destination server.