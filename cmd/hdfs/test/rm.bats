#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/rm/dir
  $HDFS touch /_test_cmd/rm/a
  $HDFS touch /_test_cmd/rm/b
  $HDFS touch /_test_cmd/rm/dir/c
  $HDFS touch /_test_cmd/rm/d
  $HDFS touch /_test_cmd/rm/e
  $HDFS touch /_test_cmd/rm/f
  $HDFS touch /_test_cmd/rm/preserveTs-1
  $HDFS touch /_test_cmd/rm/preserveTs-2
  $HDFS touch /_test_cmd/rm/preserveTs-3
}

@test "rm" {
  run $HDFS rm /_test_cmd/rm/a
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/rm/a
  assert_failure
}

@test "rm dir" {
  run $HDFS rm -r /_test_cmd/rm/dir
  assert_success
  assert_output ""

  run $HDFS ls /_test_cmd/rm/dir
  assert_failure
}

@test "rm dir without -r" {
  run $HDFS rm /_test_cmd/rm/dir
  assert_failure
  assert_output "remove /_test_cmd/rm/dir: file is a directory"
}

@test "rm dir without -r, but with -f" {
  run $HDFS rm -f /_test_cmd/rm/dir
  assert_failure
  assert_output "remove /_test_cmd/rm/dir: file is a directory"
}

@test "rm nonexistent" {
  run $HDFS rm /_test_cmd/nonexistent /_test_cmd/nonexistent2
  assert_failure
  assert_output <<OUT
remove /_test_cmd/nonexistent: file does not exist
remove /_test_cmd/nonexistent2: file does not exist
OUT
}

@test "rm nonexistent with -f" {
  run $HDFS rm -f /_test_cmd/nonexistent /_test_cmd/nonexistent2
  assert_success
  assert_output ""
}

@test "rm with trash" {
  run $HDFS rm -f --forceTrash --skipTrash /_test_cmd/rm/d
  assert_success
  assert_output ""

  run $HDFS rm -f --forceTrash /_test_cmd/rm/e
  assert_success
  case "$output" in
  "Moved: 'hdfs://"*"/_test_cmd/rm/e' to trash at: hdfs://"*"/user/$(whoami)/.Trash/Current/_test_cmd/rm/e"*) pattern=$output ;;
  "Moved: 'hdfs://"*"/_test_cmd/rm/e' to trash at: hdfs://"*"/user/$(whoami)/"*"/.Trash/Current/_test_cmd/rm/e"*) pattern=$output ;; # kerberos mode
  esac
  assert_output "$pattern"

  run $HDFS rm -f --forceTrash /_test_cmd/rm/f
  assert_success
  run $HDFS rm -f --forceTrash /user/$(whoami)/.Trash/Current/_test_cmd/rm/f
  assert_success
  assert_output ""

  run $HDFS rm -f -r --forceTrash /user/$(whoami)
  assert_failure
  assert_output "Cannot move \"/user/$(whoami)\" to the trash, as it contains the trash"
}

@test "rm with preserve timestamp" {
  run bash -c "$HADOOP_FS -stat %Y /_test_cmd/rm 2>/dev/null"
  ts1="$output"

  sleep 1
  run $HDFS rm -f -r --forceTrash /_test_cmd/rm/preserveTs-1
  assert_success
  run bash -c "$HADOOP_FS -stat %Y /_test_cmd/rm 2>/dev/null"
  ts2="$output"
  run bash -c "(( $ts1 < $ts2 ))"
  assert_success

  sleep 1
  run $HDFS rm -f -r --forceTrash --preserveDirTs /_test_cmd/rm/preserveTs-2
  assert_success
  run bash -c "$HADOOP_FS -stat %Y /_test_cmd/rm 2>/dev/null"
  ts3="$output"
  run bash -c "(( $ts2 == $ts3 ))"
  assert_success

  sleep 1
  run $HDFS rm -f -r --skipTrash --preserveDirTs /_test_cmd/rm/preserveTs-3
  assert_success
  run bash -c "$HADOOP_FS -stat %Y /_test_cmd/rm 2>/dev/null"
  ts4="$output"
  run bash -c "(( $ts2 == $ts4 ))"
  assert_success
}

teardown() {
  $HDFS rm -r /_test_cmd/rm
}
