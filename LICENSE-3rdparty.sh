#!/bin/sh -e

rm -f cargo.lock > /dev/null
cargo install cargo-license > /dev/null
cargo license --all-features -a -j --no-deps -d | jq -r '(["Component","Origin","License","Copyright"]) as $cols | map(. as $row | ["name", "repository", "license", "authors"] | map($row[.])) as $rows | $cols, $rows[] | @csv'