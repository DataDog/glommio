use std::env;
use std::fs;
use std::path::*;
use std::process::Command;

use cc::Build;

fn main() {
    let project = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .unwrap();
    let liburing = project.join("liburing");

    Command::new("git")
        .arg("submodule")
        .arg("update")
        .arg("--init")
        .status()
        .unwrap();
    // Run the configure script in OUT_DIR to get `compat.h`
    let configured_include = configure(&liburing);

    let src = liburing.join("src");

    // liburing
    Build::new()
        .file(src.join("setup.c"))
        .file(src.join("queue.c"))
        .file(src.join("syscall.c"))
        .file(src.join("register.c"))
        .include(src.join("include"))
        .include(&configured_include)
        .extra_warnings(false)
        .compile("uring");

    // (our additional, linkable C bindings)
    Build::new()
        .file(project.join("rusturing.c"))
        .include(src.join("include"))
        .include(&configured_include)
        .compile("rusturing");
}

fn configure(liburing: &Path) -> PathBuf {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap())
        .canonicalize()
        .unwrap();
    fs::copy(liburing.join("configure"), out_dir.join("configure")).unwrap();
    fs::create_dir_all(out_dir.join("src/include/liburing")).unwrap();
    Command::new("./configure")
        .current_dir(&out_dir)
        .output()
        .expect("configure script failed");
    out_dir.join("src/include")
}
