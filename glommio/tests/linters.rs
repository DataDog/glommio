// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020
// Datadog, Inc.
#[cfg(test)]
mod tests {
    use std::process::Command;
    #[test]
    fn check_formatting() {
        let status = Command::new("cargo")
            .args(["fmt", "--all", "--", "--check"])
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn check_clippy() {
        let status = Command::new("cargo")
            .args(["clippy", "--all-targets", "--", "-D", "warnings"])
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn check_dependencies_sorted() {
        let status = Command::new("cargo")
            .args(["sort", "-w", "-c"])
            .status()
            .unwrap();
        assert!(
            status.success(),
            "cargo-sort not installed or cargo.toml dependencies not sorted"
        );
    }
}
