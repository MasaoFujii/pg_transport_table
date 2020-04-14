CREATE OR REPLACE FUNCTION dump_relfilenodes (label text, tbl regclass)
    RETURNS SETOF text AS $$
DECLARE
    indexoid oid;
BEGIN
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
    basedir text;
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

    basedir := rtrim(pg_relation_filepath(tbl), '0123456789');
    newdir := basedir || label || '/';
    oldtblid := pg_relation_filenode(tbl);
    RETURN NEXT 'mv ' || basedir || oldtblid || ' ' || newdir || newtblid;

    segno := 1;
    LOOP
        filepath := basedir || oldtblid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtblid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    FOR fork IN SELECT unnest(ARRAY['_fsm', '_vm', '_init']) LOOP
        filepath := basedir || oldtblid || fork;
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

    RETURN NEXT 'mv ' || basedir || oldtoastid || ' ' || newdir || newtoastid;
    segno := 1;
    LOOP
        filepath := basedir || oldtoastid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtoastid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN NEXT 'mv ' || basedir || oldtoastidxid || ' ' || newdir || newtoastidxid;
    segno := 1;
    LOOP
        filepath := basedir || oldtoastidxid || '.' || segno;
	EXIT WHEN pg_stat_file(filepath, true) IS NULL;
	RETURN NEXT 'mv ' || filepath || ' ' || newdir || newtoastidxid || '.' || segno;
	segno := segno + 1;
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;
