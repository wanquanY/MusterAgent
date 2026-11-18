//! Real signals are sent only to an owned child, never the test runner.
#![cfg(unix)]

use std::io::Write as _;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};

#[test]
fn signal_child() {
    if std::env::var_os("MUSTERAGENT_TEST_SIGNAL_CHILD").is_none() {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let mut signal =
                musteragent_cloud_lifecycle::ShutdownSignal::install().expect("install");
            println!("SIGNAL_READY");
            std::io::stdout().flush().expect("flush");
            // Exercise arrival during startup, before the role selects recv().
            tokio::time::sleep(Duration::from_millis(150)).await;
            signal.recv().await.expect("shutdown");
            println!("DRAIN_COMPLETED");
        });
}

#[tokio::test]
async fn sigterm_and_sigint_enter_normal_shutdown() {
    for signal in ["-TERM", "-INT"] {
        let mut child =
            tokio::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "signal_child",
                    "--nocapture",
                    "--format",
                    "terse",
                ])
                .env("MUSTERAGENT_TEST_SIGNAL_CHILD", "1")
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()
                .expect("spawn owned child");
        let mut output = BufReader::new(child.stdout.take().expect("stdout")).lines();
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(line) = output.next_line().await.expect("read ready") {
                if line == "SIGNAL_READY" {
                    return;
                }
            }
            panic!("child exited before installing handlers");
        })
        .await
        .expect("ready deadline");
        let status = tokio::process::Command::new("kill")
            .args([signal, &child.id().expect("child PID").to_string()])
            .status()
            .await
            .expect("send signal");
        assert!(status.success());
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("shutdown deadline")
            .expect("wait child");
        assert!(
            status.success(),
            "{signal} must reach normal exit, not OS termination"
        );
        let mut drained = false;
        while let Some(line) = output.next_line().await.expect("read drain") {
            drained |= line == "DRAIN_COMPLETED";
        }
        assert!(drained, "{signal} did not reach application drain");
    }
}
