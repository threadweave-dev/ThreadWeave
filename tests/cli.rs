use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn announces_the_grpc_endpoint_on_stdout() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_threadweave"))
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
