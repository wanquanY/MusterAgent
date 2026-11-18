//! OS shutdown notification shared by Cloud roles. Role-specific draining stays
//! with each application; this module never exits or kills a process.
#![forbid(unsafe_code)]

use std::io;

/// Install before application startup so a termination request during startup
/// remains pending until the application's normal shutdown path can handle it.
pub struct ShutdownSignal {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    interrupt: tokio::signal::windows::CtrlC,
    #[cfg(windows)]
    terminate: tokio::signal::windows::CtrlBreak,
}

impl ShutdownSignal {
    pub fn install() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(windows)]
        {
            Ok(Self {
                interrupt: tokio::signal::windows::ctrl_c()?,
                terminate: tokio::signal::windows::ctrl_break()?,
            })
        }
    }

    /// Cancel-safe: callers can select this alongside role task completion.
    pub async fn recv(&mut self) -> io::Result<()> {
        let received = tokio::select! {
            value = self.interrupt.recv() => value,
            value = self.terminate.recv() => value,
        };
        received.ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "shutdown signal stream closed")
        })
    }
}
