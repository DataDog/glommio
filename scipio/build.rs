use std::env;
use std::path::PathBuf;

use cc::Build;

fn main() {
    let project = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).canonicalize().unwrap();

    // liburing
    let liburing = project.join("liburing").join("src");
    Build::new().file(liburing.join("setup.c"))
                .file(liburing.join("queue.c"))
                .file(liburing.join("syscall.c"))
                .file(liburing.join("register.c"))
                .include(liburing.join("include"))
                .compile("uring");

    // (our additional, linkable C bindings)
    Build::new().file(project.join("rusturing.c")).include(liburing.join("include"))
                .compile("rusturing");
}
