use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn announces_the_grpc_endpoint_on_stdout() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_threadweave-api"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to run the threadweave binary");
    let stdout = child.stdout.take().expect("stdout must be captured");
    let line = BufReader::new(stdout)
        .lines()
        .next()
        .expect("ready line must be present")
        .expect("ready line must be valid UTF-8");
    let ready: serde_json::Value = serde_json::from_str(&line).expect("ready line must be JSON");
    assert_eq!(ready["type"], "ready");
    assert_eq!(ready["transport"], "tcp");
    assert_eq!(ready["protocol"], "grpc");
    assert!(
        ready["endpoint"]
            .as_str()
            .is_some_and(|endpoint| endpoint.starts_with("http://127.0.0.1:"))
    );
    child.kill().expect("server must stop");
    child.wait().expect("server must be reaped");
}

#[test]
fn emits_startup_information_as_json_without_using_stderr() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_threadweave-api"))
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run the threadweave binary");
    let stdout = child.stdout.take().expect("stdout must be captured");
    let line = BufReader::new(stdout)
        .lines()
        .next()
        .expect("startup line must be present")
        .expect("startup line must be valid UTF-8");
    let startup: serde_json::Value =
        serde_json::from_str(&line).expect("startup line must be JSON");

    assert_eq!(startup["type"], "startup");
    assert_eq!(startup["name"], "ThreadWeave");
    assert_eq!(startup["component"], "api");
    assert_eq!(startup["broker"]["transport"], "redis");
    assert_eq!(startup["result_backend"]["transport"], "redis");

    child.kill().expect("server must stop");
    let output = child.wait_with_output().expect("server must be reaped");
    assert!(output.stderr.is_empty(), "stderr must remain unused");
}
