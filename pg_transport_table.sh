#!/bin/sh

PROGNAME=$(basename ${0})
CURDIR=$(pwd)

PSQL=$(which psql || echo "$CURDIR/bin/psql")" -X -At"

SRC=
DST=
TBL=

elog ()
{
  echo "$PROGNAME: ERROR: $1" 1>&2
  exit 1
}

exit_on_error ()
{
  if [ $? -ne 0 ]; then
    exit 1
  fi
}

usage ()
{
  cat <<EOF
$PROGNAME transports the specified table from source to destination server.

Usage:
  $PROGNAME [OPTIONS] TABLENAME

Options:
  -d CONNINFO    connection string to connect to destination server
  -s CONNINFO    connection string to connect to source server
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

SSYSID=$($PSQL "$SRC" -c "SELECT system_identifier FROM pg_control_system()")
exit_on_error
DSYSID=$($PSQL "$DST" -c "SELECT system_identifier FROM pg_control_system()")
exit_on_error

if [ "$SSYSID" == "$DSYSID" ]; then
  elog "source and destination servers must be differenet"
fi

STBL=$($PSQL "$SRC" -c "SELECT relfilenode FROM pg_class WHERE relname = '$TBL'")
exit_on_error
if [ -z "$STBL" ]; then
  elog "table \"$TBL\" doesn't exist in source server"
fi
DTBL=$($PSQL "$DST" -c "SELECT relfilenode FROM pg_class WHERE relname = '$TBL'")
exit_on_error
if [ -z "$DTBL" ]; then
  elog "table \"$TBL\" doesn't exist in destination server"
fi
echo "mv $STBL $DTBL"
echo "mv ${STBL}_fsm ${DTBL}_fsm"
echo "mv ${STBL}_vm ${DTBL}_vm"
cat <<EOF
for num in \$(seq 1 100000); do
  if [ -f "${STBL}.\${num}" ]; then
    mv ${STBL}.\${num} ${DTBL}.\${num}
  else
    break
  fi
done
EOF

IDXLIST=$($PSQL "$SRC" -c "SELECT indexname FROM pg_indexes WHERE tablename = '$TBL'")
exit_on_error
for idx in $IDXLIST; do
  SIDX=$($PSQL "$SRC" -c "SELECT relfilenode FROM pg_class WHERE relname = '$idx'")
  exit_on_error
  if [ -z "$SIDX" ]; then
    elog "index \"$idx\" doesn't exist in source server"
  fi
  DIDX=$($PSQL "$DST" -c "SELECT relfilenode FROM pg_class WHERE relname = '$idx'")
  exit_on_error
  if [ -z "$DIDX" ]; then
    elog "index \"$idx\" doesn't exist in destination server"
  fi
  echo "mv $SIDX $DIDX"
  cat <<EOF
for num in \$(seq 1 100000); do
  if [ -f "${SIDX}.\${num}" ]; then
    mv ${SIDX}.\${num} ${DIDX}.\${num}
  else
    break
  fi
done
EOF
done

STOAST=$($PSQL "$SRC" -c "SELECT reltoastrelid FROM pg_class WHERE relname = '$TBL'")
exit_on_error
DTOAST=$($PSQL "$DST" -c "SELECT reltoastrelid FROM pg_class WHERE relname = '$TBL'")
exit_on_error
if [ $STOAST -ne 0 ]; then
  if [ $DTOAST -eq 0 ]; then
    elog "table \"$TBL\" doesn't have TOAST table in destination server"
  fi
  echo "mv $STOAST $DTOAST"
  cat <<EOF
for num in \$(seq 1 100000); do
  if [ -f "${STOAST}.\${num}" ]; then
    mv ${STOAST}.\${num} ${DTOAST}.\${num}
  else
    break
  fi
done
EOF

  STOASTIDX=$($PSQL "$SRC" -c "SELECT indexrelid FROM pg_index WHERE indrelid = $STOAST")
  exit_on_error
  DTOASTIDX=$($PSQL "$DST" -c "SELECT indexrelid FROM pg_index WHERE indrelid = $DTOAST")
  exit_on_error
  echo "mv $STOASTIDX $DTOASTIDX"
  cat <<EOF
for num in \$(seq 1 100000); do
  if [ -f "${STOASTIDX}.\${num}" ]; then
    mv ${STOASTIDX}.\${num} ${DTOASTIDX}.\${num}
  else
    break
  fi
done
EOF
fi
