#!/bin/sh

PROGNAME=$(basename ${0})
CURDIR=$(pwd)
RETFILE=$(mktemp)
TMPFILE=$(mktemp)

set -E
trap elog ERR

PSQL=$(which psql || echo "$CURDIR/bin/psql")" -X -At"

SRC=
DST=
ROOT=
TBL=
OUTPUT=

elog ()
{
  [ -z "$1" ] || echo "$PROGNAME: ERROR: $1" 1>&2
  rm -f $RETFILE $TMPFILE
  exit 1
}

usage ()
{
  cat <<EOF
$PROGNAME outputs lists of commands that transport
the specified table from source to destination server.

Usage:
  $PROGNAME [OPTIONS] TABLENAME

Options:
  -s CONNINFO    connection string to source server
  -d CONNINFO    connection string to destination server
  -o FILEPATH    output file path (default: output to stdout)
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    "-?"|--help)
      usage
      exit 0;;
    -s)
      SRC="$2"
      shift;;
    -d)
      DST="$2"
      shift;;
    -o)
      OUTPUT="$2"
      shift;;
    -*)
      elog "invalid option: $1";;
    *)
      if [ -z "$ROOT" ]; then
	ROOT="$1"
      else
	elog "too many arguments"
      fi
      ;;
  esac
  shift
done

[ ! -z "$ROOT" ] || elog "name of table to transport must be specified"

RETSRC=
psql_src ()
{
  RETSRC=$($PSQL -d "$SRC" -c "${1:-$SQL}")
}

RETDST=
psql_dst ()
{
  RETDST=$($PSQL -d "$DST" -c "${1:-$SQL}")
}

psql_both ()
{
  psql_src "${1:-$SQL}"
  psql_dst "${1:-$SQL}"
}

check_sysid ()
{
  psql_both "SELECT system_identifier FROM pg_control_system()"
  [ "$RETSRC" != "$RETDST" ] ||
    elog "source and destination must be differenet servers"
}

rename_multiple_files ()
{
  FILESRC="$1"
  FILEDST="$2"
  MAXNUMFILES=100000
  cat <<EOF
for num in \$(seq 1 $MAXNUMFILES); do
  if [ -f "${FILESRC}.\${num}" ]; then
    mv ${FILESRC}.\${num} ${FILEDST}.\${num}
  else
    break
  fi
done
EOF
}

rename_template ()
{
  RELSRC="$1"
  RELDST="$2"
  echo "mv $RELSRC $RELDST"
  echo "rm -f ${RELSRC}_init"
  rename_multiple_files "$RELSRC" "$RELDST"
  echo "$RELSRC" >> $TMPFILE
  echo "$RELDST" >> $TMPFILE
}

check_table ()
{
  TBL="${1:-$TBL}"
  psql_both "SELECT relfilenode FROM pg_class WHERE relname = '$TBL'"
  [ ! -z "$RETSRC" ] || elog "table \"$TBL\" doesn't exist in source server"
  [ ! -z "$RETDST" ] || elog "table \"$TBL\" doesn't exist in destination server"
}

rename_table ()
{
  check_table
  rename_template "$RETSRC" "$RETDST"
  echo "mv ${RETSRC}_fsm ${RETDST}_fsm"
  echo "mv ${RETSRC}_vm ${RETDST}_vm"
}

rename_indexes ()
{
  psql_src "SELECT indexname FROM pg_indexes WHERE tablename = '$TBL'"
  IDXLIST="$RETSRC"
  for idx in $IDXLIST; do
    psql_both "SELECT relfilenode FROM pg_class WHERE relname = '$idx'"
    [ ! -z "$RETSRC" ] || elog "index \"$idx\" doesn't exist in source server"
    [ ! -z "$RETDST" ] || elog "index \"$idx\" doesn't exist in destination server"
    rename_template "$RETSRC" "$RETDST"
  done
}

rename_toast ()
{
  psql_both "SELECT reltoastrelid FROM pg_class WHERE relname = '$TBL'"
  [ $RETSRC -ne 0 ] || return 0
  [ $RETDST -ne 0 ] || elog "table \"$TBL\" has no TOAST table in destination server"
  rename_template "$RETSRC" "$RETDST"
}

rename_toast_index ()
{
  psql_both "SELECT indexrelid FROM pg_class, pg_index WHERE relname = '$TBL' AND reltoastrelid = indrelid"
  [ ! -z "$RETSRC" ] || return 0
  [ ! -z "$RETDST" ] || elog "table \"\$TBL has no TOAST index in destination server "
  rename_template "$RETSRC" "$RETDST"
}

rename_relation ()
{
  TBL="${1:-$TBL}"
  rename_table
  rename_indexes
  rename_toast
  rename_toast_index
}

check_duplicate_filename ()
{
  RET=$(sort $TMPFILE | uniq -d)
  [ -z "$RET" ] || elog "duplicate file names: $(echo $RET | tr -d '\n')"
}

rename_partition ()
{
  check_table "$ROOT"
  psql_both "SELECT relid FROM (SELECT * FROM pg_partition_tree('$ROOT')) ppt WHERE ppt.isleaf"
  [ "$RETSRC" == "$RETDST" ] ||
    elog "definitions of partitions are not the same between source and destination servers"
  TBLLIST="${RETSRC:-$ROOT}"
  for tbl in $TBLLIST; do
    rename_relation "$tbl"
  done
}

check_sysid
rename_partition >> $RETFILE
check_duplicate_filename

if [ -z "$OUTPUT" ]; then
  cat $RETFILE
else
  mv $RETFILE $OUTPUT
fi

rm -f $RETFILE $TMPFILE
