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


CREATE OR REPLACE FUNCTION dump_relfilenodes (label text, tbl regclass)
    RETURNS SETOF text AS $$
DECLARE
    indexoid oid;
    newdir_sql text;
BEGIN
    newdir_sql := 'rtrim(pg_relation_filepath(''' || tbl || '''), ''0123456789'') || ''' || label || ''';';
    RETURN NEXT 'SELECT ''rm -rf '' || ' || newdir_sql;
    RETURN NEXT 'SELECT ''mkdir '' || ' || newdir_sql;

    RETURN QUERY
        SELECT 'SELECT print_transport_commands(''' ||
            label || ''', ''' ||
            nsp.nspname || '.' ||
	    rel.relname || '''' || ', ' ||
	    rel.relfilenode || ', ' ||
	    COALESCE(pg_relation_filenode(rel.reltoastrelid), 0) || ', ' ||
	    COALESCE(pg_relation_filenode(idx.indexrelid), 0) || ');'
        FROM pg_namespace nsp
	    JOIN pg_class rel ON nsp.oid = rel.relnamespace
	    LEFT JOIN pg_index idx ON rel.reltoastrelid = idx.indrelid
        WHERE rel.oid = tbl;

    RETURN QUERY
        SELECT 'SELECT print_transport_commands(''' ||
            label || ''', ''' ||
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
    label text,
    tbl regclass,
    newtblid bigint,
    newtoastid bigint DEFAULT 0,
    newtoastidxid bigint DEFAULT 0)
    RETURNS SETOF text AS $$
DECLARE
    base text;
    newdir text;
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

    base := rtrim(pg_relation_filepath(tbl), '0123456789');
    newdir := base || label || '/';
    oldtblid := pg_relation_filenode(tbl);
    RETURN NEXT 'mv ' || base || oldtblid || ' ' || newdir || newtblid;

    segno := 1;
    LOOP
        filepath := base || oldtblid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtblid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    FOR fork IN SELECT unnest(ARRAY['_fsm', '_vm', '_init']) LOOP
        filepath := base || oldtblid || fork;
	CONTINUE WHEN pg_stat_file(filepath, true) IS NULL;
        RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtblid || fork;
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

    RETURN NEXT 'mv ' || base || oldtoastid || ' ' || newdir || newtoastid;
    segno := 1;
    LOOP
        filepath := base || oldtoastid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtoastid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN NEXT 'mv ' || base || oldtoastidxid || ' ' || newdir || newtoastidxid;
    segno := 1;
    LOOP
        filepath := base || oldtoastidxid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtoastidxid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;
