use std::{
    io,
    process::{Command, ExitStatus},
};

#[test]
fn check_formatting() {
    let out = output("rustfmt --version").unwrap();
    if !out.contains("stable") {
        panic!(
            "Failed to run rustfmt from toolchain 'stable'. \
             Please run `rustup component add rustfmt --toolchain stable` to install it.",
        )
    }
    run("cargo fmt --all -- --check").unwrap();
}

fn run(command: &str) -> io::Result<()> {
    let output = cmd(command).status()?;
    check_status(command, output)
}

fn output(command: &str) -> io::Result<String> {
    let output = cmd(command).output()?;
    check_status(command, output.status)?;
    String::from_utf8(output.stdout).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn cmd(command: &str) -> Command {
    let args = command.split_ascii_whitespace().collect::<Vec<_>>();
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..]);
    cmd
}

fn check_status(command: &str, status: ExitStatus) -> io::Result<()> {
    if status.success() {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::Other,
        format!("`{}` exited with non-zero status: {}", command, status),
    ))
}
