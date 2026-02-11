use std::{env, path::*, process::Command};

use cc::Build;

fn main() {
    let project = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .unwrap();

    let liburing = match env::var("GLOMMIO_LIBURING_DIR") {
        Ok(path) => PathBuf::from(path).canonicalize().unwrap(),
        Err(_) => {
            Command::new("git")
                .arg("submodule")
                .arg("update")
                .arg("--init")
                .status()
                .unwrap();

            project.join("liburing")
        }
    };

    // Run the configure script to get `compat.h`
    Command::new("./configure")
        .current_dir(&liburing)
        .output()
        .expect("configure script failed");

    let configured_include = liburing.join("src/include");
    let src = liburing.join("src");

    // liburing
    Build::new()
        .file(src.join("setup.c"))
        .file(src.join("queue.c"))
        .file(src.join("syscall.c"))
        .file(src.join("register.c"))
        .flag("-D_GNU_SOURCE")
        .include(&configured_include)
        .extra_warnings(false)
        .compile("uring");

    // (our additional, linkable C bindings)
    Build::new()
        .file(project.join("rusturing.c"))
        .flag("-D_GNU_SOURCE")
        .include(&configured_include)
        .compile("rusturing");
}
