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
    'max_index_keys', 'segment_size', 'server_version',
    'wal_block_size', 'wal_segment_size')
ORDER BY name;

SELECT
    datname, pg_encoding_to_char(encoding),
    datcollate, datctype
FROM pg_database
WHERE datname = current_database();

-- check the DDL of database objects to transport between two instances
-- by using pg_dump -s -t xxx

SELECT redo_lsn FROM pg_control_checkpoint();

SELECT pg_current_wal_lsn();

CREATE OR REPLACE FUNCTION dump_relfilenodes (tbl regclass)
    RETURNS SETOF text AS $$
DECLARE
    indexoid oid;
BEGIN
    RETURN QUERY
        SELECT 'SELECT print_transport_commands(''' ||
	    nsp.nspname || '.' ||
	    rel.relname || '''' || ', ' ||
	    rel.relfilenode || ', ' ||
	    COALESCE(pg_relation_filenode(rel.reltoastrelid), 0) || ',' ||
	    COALESCE(pg_relation_filenode(idx.indexrelid), 0) || ');'
        FROM pg_namespace nsp
	    JOIN pg_class rel ON nsp.oid = rel.relnamespace
	    LEFT JOIN pg_index idx ON rel.reltoastrelid = idx.indrelid
        WHERE rel.oid = tbl;

    RETURN QUERY
        SELECT 'SELECT print_transport_commands(''' ||
	    nsp.nspname || '.' ||
	    rel.relname || '''' || ', ' ||
	    rel.relfilenode || ');'
        FROM pg_namespace nsp
	    JOIN pg_class rel ON nsp.oid = rel.relnamespace
	    JOIN pg_index idx ON rel.oid = idx.indexrelid
        WHERE idx.indrelid = tbl;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;


CREATE OR REPLACE FUNCTION print_transport_commands (
    tbl regclass,
    newtblid bigint,
    newtoastid bigint DEFAULT 0,
    newtoastidxid bigint DEFAULT 0)
    RETURNS SETOF text AS $$
DECLARE
    basedir text;
    oldtblid bigint := 0;
    oldtoastid bigint := 0;
    oldtoastidxid bigint := 0;
    oldreltoastrelid bigint := 0;
    segno bigint;
    filepath text;
    fork text;
BEGIN
    IF newtblid = newtoastid THEN
        RAISE EXCEPTION 'newtblid must be different from newtoastid';
    ELSIF newtblid = newtoastidxid THEN
        RAISE EXCEPTION 'newtblid must be different from newtoastidxid';
    ELSIF newtoastid = 0 AND newtoastidxid <> 0 THEN
        RAISE EXCEPTION 'newtoastidxid must be specified together if newtoastid is specified';
    ELSIF newtoastid <> 0 AND newtoastidxid = 0 THEN
        RAISE EXCEPTION 'newtoastid must be specified together if newtoastidxid is specified';
    END IF;

    basedir := rtrim(pg_relation_filepath(tbl), '0123456789');
    oldtblid := pg_relation_filenode(tbl);
    RETURN NEXT 'mv ' || basedir || oldtblid || ' ' || basedir || newtblid;

    segno := 1;
    LOOP
        filepath := basedir || oldtblid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || basedir || newtblid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    FOR fork IN SELECT unnest(ARRAY['_fsm', '_vm', '_init']) LOOP
        filepath := basedir || oldtblid || fork;
	CONTINUE WHEN pg_stat_file(filepath, true) IS NULL;
        RETURN NEXT 'mv ' || filepath || ' ' || basedir || newtblid || fork;
    END LOOP;

    IF newtoastid = 0 THEN
        RETURN;
    END IF;

    SELECT reltoastrelid INTO oldreltoastrelid FROM pg_class WHERE oid = tbl;
    IF oldreltoastrelid = 0 THEN
        RAISE EXCEPTION 'TOAST must exist if newtoastid is specified';
    END IF;

    oldtoastid := pg_relation_filenode(oldreltoastrelid);
    SELECT pg_relation_filenode(indexrelid) INTO oldtoastidxid FROM pg_index
        WHERE indrelid = oldreltoastrelid;
    IF oldtoastidxid IS NULL THEN
        RAISE EXCEPTION 'TOAST index must exist if TOAST exists';
    END IF;

    RETURN NEXT 'mv ' || basedir || oldtoastid || ' ' || basedir || newtoastid;
    segno := 1;
    LOOP
        filepath := basedir || oldtoastid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || basedir || newtoastid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN NEXT 'mv ' || basedir || oldtoastidxid || ' ' || basedir || newtoastidxid;
    segno := 1;
    LOOP
        filepath := basedir || oldtoastidxid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || basedir || newtoastidxid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;
