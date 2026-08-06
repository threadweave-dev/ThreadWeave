use std::process::Command;

#[test]
fn prints_the_project_name() {
    let output = Command::new(env!("CARGO_BIN_EXE_threadweave"))
        .output()
        .expect("failed to run the threadweave binary");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "ThreadWeave"
    );
    assert!(output.stderr.is_empty());
}
