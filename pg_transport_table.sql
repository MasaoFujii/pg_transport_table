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
