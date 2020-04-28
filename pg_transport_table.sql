CREATE OR REPLACE FUNCTION pgtp_check_controlfile
    (conninfo text, col text, coltype text, rel text) RETURNS void AS $$
DECLARE
    sql text;
    diff boolean;
BEGIN
    sql := 'SELECT x <> ' || col || ' FROM dblink(''' || conninfo || ''',
        ''SELECT ' || col || ' FROM ' || rel || ''') AS t(x ' || coltype || '), ' || rel;
    EXECUTE sql INTO diff;
    IF diff THEN
        RAISE EXCEPTION '% must be the same between two servers', col;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION pgtp_check_presetguc
    (conninfo text, guc text) RETURNS void AS $$
DECLARE
    sql text;
    diff boolean;
BEGIN
    sql := 'SELECT current_setting(''' || guc || ''') <> x FROM dblink(''' ||
        conninfo || ''', ''SELECT current_setting(''''' || guc || ''''')'') AS t(x text)';
    EXECUTE sql INTO diff;
    IF diff THEN
        RAISE EXCEPTION '% must be the same between two servers', guc;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION pgtp_check_lsn (conninfo text)
    RETURNS void AS $$
DECLARE
    sql text;
    res boolean;
    msg text;
BEGIN
    sql := 'SELECT x <= pg_current_wal_lsn()  FROM dblink(''' || conninfo ||
        ''', ''SELECT redo_lsn FROM pg_control_checkpoint()'') AS t(x pg_lsn)';
    EXECUTE sql INTO res;
    IF res THEN
        msg := 'REDO lsn in prod server must be larger than latest lsn in temp server';
        RAISE EXCEPTION '%', msg;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION pgtp_check_conditions (conninfo text)
    RETURNS void AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT * FROM unnest(ARRAY['dblink', 'pg_visibility']) ext
        WHERE NOT EXISTS (SELECT * FROM pg_extension WHERE extname = ext) LOOP
        RAISE EXCEPTION 'required extension not found: %', r.ext;
    END LOOP;
    IF current_setting('autovacuum')::boolean THEN
        RAISE EXCEPTION 'autovacuum must be disabled';
    END IF;
    PERFORM pgtp_check_controlfile(
        conninfo, col, 'integer', 'pg_control_system()')
        FROM unnest(ARRAY['pg_control_version', 'catalog_version_no']) col;
    PERFORM pgtp_check_controlfile(
        conninfo, col, 'integer', 'pg_control_init()')
        FROM unnest(ARRAY['max_data_alignment', 'database_block_size',
            'blocks_per_segment', 'wal_block_size', 'bytes_per_wal_segment',
            'max_identifier_length', 'max_index_columns',
            'max_toast_chunk_size', 'large_object_chunk_size',
            'data_page_checksum_version']) col;
    PERFORM pgtp_check_controlfile(conninfo, 'float8_pass_by_value',
        'boolean', 'pg_control_init()');
    PERFORM pgtp_check_presetguc(conninfo, guc) FROM
        unnest(ARRAY['block_size', 'data_checksums', 'data_directory_mode',
        'integer_datetimes', 'max_function_args', 'max_identifier_length',
        'max_index_keys', 'segment_size', 'wal_block_size', 'wal_segment_size']) guc;
    PERFORM pgtp_check_lsn(conninfo);
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION pgtp_rename (base text, src text, dst text)
    RETURNS SETOF text AS $$
DECLARE
    segno bigint;
BEGIN
    IF pg_stat_file(base || src, true) IS NULL THEN
        RETURN;
    END IF;
    RETURN NEXT 'mv ' || src || ' ' || dst;
    segno := 1;
    LOOP
        EXIT WHEN pg_stat_file(base || src || '.' || segno, true) IS NULL;
        RETURN NEXT 'mv ' || src || '.' || segno || ' ' || dst || '.' || segno;
        segno := segno + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE VIEW pgtp_tblinfo AS
    SELECT rel.oid AS rel,
        'pgtp.' || nsp.nspname || '.' || rel.relname || '.' AS label,
        relfilenode::text tblid,
        pg_relation_filenode(rel.reltoastrelid)::text AS toast,
        pg_relation_filenode(idx.indexrelid)::text AS toastidx
    FROM pg_namespace nsp
        JOIN pg_class rel ON nsp.oid = rel.relnamespace
        LEFT JOIN pg_index idx ON rel.reltoastrelid = idx.indrelid;

CREATE OR REPLACE FUNCTION pgtp_create_manifest (tbl regclass)
    RETURNS SETOF text AS $$
DECLARE
    base text;
    r record;
BEGIN
    base := rtrim(pg_relation_filepath(tbl), '0123456789');
    SELECT * INTO r FROM pgtp_tblinfo WHERE rel = tbl;
    RETURN QUERY SELECT pgtp_rename(base, r.tblid, r.label || 'table');
    RETURN QUERY SELECT pgtp_rename(base, r.toast, r.label || 'toast');
    RETURN QUERY SELECT pgtp_rename(base, r.toastidx, r.label || 'toastidx');
    RETURN QUERY SELECT pgtp_rename(base, r.tblid || '_' || fork, r.label || fork)
        FROM unnest(ARRAY['fsm', 'vm', 'init']) fork;
    RETURN QUERY SELECT pgtp_rename(base, rel.relfilenode::text, r.label || 'index.' || relname)
        FROM pg_class rel JOIN pg_index idx ON rel.oid = idx.indexrelid
        WHERE idx.indrelid = tbl;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION pgtp_apply_manifest (tbl regclass, base text)
    RETURNS SETOF text AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM pgtp_tblinfo WHERE rel = tbl;
    RETURN QUERY SELECT pgtp_rename(base, r.label || 'table', r.tblid);
    RETURN QUERY SELECT pgtp_rename(base, r.label || 'toast', r.toast);
    RETURN QUERY SELECT pgtp_rename(base, r.label || 'toastidx', r.toastidx);
    RETURN QUERY SELECT pgtp_rename(base, r.label || fork, r.tblid || '_' || fork)
        FROM unnest(ARRAY['fsm', 'vm', 'init']) fork;
    RETURN QUERY SELECT pgtp_rename(base, r.label || 'index.' || relname, rel.relfilenode::text)
        FROM pg_class rel JOIN pg_index idx ON rel.oid = idx.indexrelid
        WHERE idx.indrelid = tbl;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;
