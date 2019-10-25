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
TBL=

elog ()
{
  [ ! -z "$1" ] && echo "$PROGNAME: ERROR: $1" 1>&2
  rm -f $RETFILE $TMPFILE
  exit 1
}

usage ()
{
  cat <<EOF
$PROGNAME transports the specified table from source to destination server.

Usage:
  $PROGNAME [OPTIONS] TABLENAME

Options:
  -d CONNINFO    connection string to destination server
  -s CONNINFO    connection string to source server
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    "-?"|--help)
      usage
      exit 0;;
    -d)
      DST="$2"
      shift;;
    -s)
      SRC="$2"
      shift;;
    -*)
      elog "invalid option: $1";;
    *)
      if [ -z "$TBL" ]; then
	TBL="$1"
      else
	elog "too many arguments"
      fi
      ;;
  esac
  shift
done

if [ -z "$TBL" ]; then
  elog "name of table to transport must be specified"
fi

RETSRC=
psql_src ()
{
  [ ! -z "$1" ] && SQL="$1"
  RETSRC=$($PSQL -d "$SRC" -c "$SQL")
}

RETDST=
psql_dst ()
{
  [ ! -z "$1" ] && SQL="$1"
  RETDST=$($PSQL -d "$DST" -c "$SQL")
}

psql_both ()
{
  [ ! -z "$1" ] && SQL="$1"
  psql_src
  psql_dst
}

check_sysid ()
{
  psql_both "SELECT system_identifier FROM pg_control_system()"
  if [ "$RETSRC" == "$RETDST" ]; then
    elog "source and destination servers must be differenet"
  fi
}
check_sysid

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

rename_table ()
{
  psql_both "SELECT relfilenode FROM pg_class WHERE relname = '$TBL'"
  [ -z "$RETSRC" ] && elog "table \"$TBL\" doesn't exist in source server"
  [ -z "$RETDST" ] && elog "table \"$TBL\" doesn't exist in destination server"
  rename_template "$RETSRC" "$RETDST"
  echo "mv ${RETSRC}_fsm ${RETDST}_fsm"
  echo "mv ${RETSRC}_vm ${RETDST}_vm"
}
rename_table >> $RETFILE

rename_indexes ()
{
  psql_src "SELECT indexname FROM pg_indexes WHERE tablename = '$TBL'"
  IDXLIST="$RETSRC"
  for idx in $IDXLIST; do
    psql_both "SELECT relfilenode FROM pg_class WHERE relname = '$idx'"
    [ -z "$RETSRC" ] && elog "index \"$idx\" doesn't exist in source server"
    [ -z "$RETDST" ] && elog "index \"$idx\" doesn't exist in destination server"
    rename_template "$RETSRC" "$RETDST"
  done
}
rename_indexes >> $RETFILE

rename_toast ()
{
  psql_both "SELECT reltoastrelid FROM pg_class WHERE relname = '$TBL'"
  [ $RETSRC -eq 0 ] && return 0
  [ $RETDST -eq 0 ] && elog "table \"$TBL\" doesn't have TOAST table in destination server"
  rename_template "$RETSRC" "$RETDST"
}
rename_toast >> $RETFILE

rename_toast_index ()
{
  psql_both "SELECT indexrelid FROM pg_class, pg_index WHERE relname = '$TBL' AND reltoastrelid = indrelid"
  rename_template "$RETSRC" "$RETDST"
}
rename_toast_index >> $RETFILE

check_duplicate_filename ()
{
  RET=$(sort $TMPFILE | uniq -d)
  [ ! -z "$RET" ] && elog "duplicate file names: $(echo $RET | tr -d '\n')"
}
check_duplicate_filename

cat $RETFILE

rm -f $RETFILE $TMPFILE
