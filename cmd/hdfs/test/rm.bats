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

teardown() {
  $HDFS rm -r /_test_cmd/rm
}
