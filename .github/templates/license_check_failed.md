Greetings @{{ .user }}!

It looks like your PR added a new or changed an existing dependency, and CI has failed to validate your changes.
Some possible reasons this could happen:
* One of the dependencies you added uses a restricted license. See `deny.toml` for a list of licenses we allow;
* One of the dependencies you added has a known security vulnerability;
* You added or updated a dependency and didn't update the `LICENSE-3rdparty.csv` file. To do so, run the following and commit the changes:
```
$ cargo install cargo-license
$ cargo license --all-features -a -j --no-deps -d | jq -r '(["Component","Origin","License","Copyright"]) as $cols | map(. as $row | ["name", "repository", "license", "authors"] | map($row[.])) as $rows | $cols, $rows[] | @csv' > LICENSE-3rdparty.csv
```

Thank you!