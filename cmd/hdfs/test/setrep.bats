#!/usr/bin/env bats

load helper

setup() {
  $HDFS mkdir -p /_test_cmd/setrep/empty-dir
  $HDFS touch /_test_cmd/setrep/single

  $HDFS mkdir -p /_test_cmd/setrep/one-level
  $HDFS touch /_test_cmd/setrep/one-level/1
  $HDFS touch /_test_cmd/setrep/one-level/2

  $HDFS mkdir -p /_test_cmd/setrep/deep/inner
  $HDFS touch /_test_cmd/setrep/deep/1
  $HDFS touch /_test_cmd/setrep/deep/inner/2
  $HDFS touch /_test_cmd/setrep/deep/inner/3
}

@test "setrep nonexistent and empty-dir" {
  run $HDFS setrep 2 /_test_cmd/nonexistent
  assert_success
  assert_output ""

  run $HDFS setrep 2 /_test_cmd/empty-dir
  assert_success
  assert_output ""
}

@test "setrep single" {
  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/single 2>/dev/null"
  assert_success
  assert_output 3

  run $HDFS setrep 2 /_test_cmd/setrep/single
  assert_success
  assert_output "Replication 2 set: hdfs://localhost:9000/_test_cmd/setrep/single"

  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/single 2>/dev/null"
  assert_success
  assert_output 2

  # increase rep
  run $HDFS setrep 3 /_test_cmd/setrep/single
  assert_success
  assert_output "Replication 3 set: hdfs://localhost:9000/_test_cmd/setrep/single"

  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/single 2>/dev/null"
  assert_success
  assert_output 3
}

@test "setrep one-level" {
  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/one-level/1 /_test_cmd/setrep/one-level/2 2>/dev/null | xargs"
  assert_success
  assert_output "3 3"

  run $HDFS setrep 1 /_test_cmd/setrep/one-level
  assert_success
  assert_output <<-OUT
	Replication 1 set: hdfs://localhost:9000/_test_cmd/setrep/one-level/1
	Replication 1 set: hdfs://localhost:9000/_test_cmd/setrep/one-level/2
	OUT

  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/one-level/1 /_test_cmd/setrep/one-level/2 2>/dev/null | xargs"
  assert_success
  assert_output "1 1"
}

@test "setrep deep" {
  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/deep/1 /_test_cmd/setrep/deep/inner/2 /_test_cmd/setrep/deep/inner/3 2>/dev/null | xargs"
  assert_success
  assert_output "3 3 3"

  run $HDFS setrep 2 /_test_cmd/setrep/deep
  assert_success
  assert_output <<-OUT
	Replication 2 set: hdfs://localhost:9000/_test_cmd/setrep/deep/1
	Replication 2 set: hdfs://localhost:9000/_test_cmd/setrep/deep/inner/2
	Replication 2 set: hdfs://localhost:9000/_test_cmd/setrep/deep/inner/3
	OUT

  run bash -c "$HADOOP_FS -stat %r /_test_cmd/setrep/deep/1 /_test_cmd/setrep/deep/inner/2 /_test_cmd/setrep/deep/inner/3 2>/dev/null | xargs"
  assert_success
  assert_output "2 2 2"
}

teardown() {
  $HDFS rm -r /_test_cmd/setrep
}
