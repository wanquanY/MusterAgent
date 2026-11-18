//! One PostgreSQL listener fans out account hints, with durable replay on loss.
use crate::keyed_signal::{KeyedSignalHub, KeyedSignalSubscription};
use musteragent_identity::OwnerSpaceId;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    AccountChangeFeed, AccountError, AccountEventWatcher, DialogueStoreError,
};
use sqlx::{PgPool, postgres::PgListener};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
const CHANNEL: &str = "musteragent_account";
const HANDOFF_CHANNEL: &str = "musteragent_dialogue_handoff";
#[derive(Debug, Clone)]
pub struct PostgresAccountChangeFeed {
    signals: KeyedSignalHub<OwnerSpaceId>,
    available: watch::Sender<bool>,
    handoff: watch::Sender<u64>,
    pool: Arc<std::sync::Mutex<Option<PgPool>>>,
}
impl Default for PostgresAccountChangeFeed {
    fn default() -> Self {
        Self::new()
    }
}
impl PostgresAccountChangeFeed {
    #[must_use]
    pub fn new() -> Self {
        let (available, _) = watch::channel(false);
        Self {
            signals: KeyedSignalHub::default(),
            available,
            handoff: watch::channel(0).0,
            pool: Arc::new(std::sync::Mutex::new(None)),
        }
    }
    /// A handoff hint only wakes durable claim readers. It never identifies an account.
    pub fn subscribe_handoff(&self) -> watch::Receiver<u64> {
        self.handoff.subscribe()
    }

    fn wake_handoff(&self) {
        self.handoff
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn replay_after_gap(&self) -> Result<(), DialogueStoreError> {
        self.wake_handoff();
        self.signals
            .notify_all()
            .map_err(|_| DialogueStoreError::Infrastructure)
    }

    /// Call before accepting subscriptions. PostgreSQL queues notifications on
    /// this connection until run starts, closing the startup registration gap.
    pub async fn connect(&self, pool: &PgPool) -> Result<PgListener, AccountError> {
        let mut listener = PgListener::connect_with(pool)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        listener.eager_reconnect(true);
        listener
            .listen_all([CHANNEL, HANDOFF_CHANNEL])
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        *self
            .pool
            .lock()
            .map_err(|_| DialogueStoreError::Infrastructure)? = Some(pool.clone());
        self.available.send_replace(true);
        self.replay_after_gap()?;
        Ok(listener)
    }
    pub async fn run(
        &self,
        listener: PgListener,
        cancellation: CancellationToken,
    ) -> Result<(), AccountError> {
        let pool = self
            .pool
            .lock()
            .map_err(|_| DialogueStoreError::Infrastructure)?
            .clone()
            .ok_or(DialogueStoreError::Infrastructure)?;
        let result = self.run_reconnecting(listener, &pool, cancellation).await;
        self.available.send_replace(false);
        result
    }
    async fn run_reconnecting(
        &self,
        mut listener: PgListener,
        pool: &PgPool,
        cancellation: CancellationToken,
    ) -> Result<(), AccountError> {
        loop {
            match self.run_inner(&mut listener, cancellation.clone()).await {
                Ok(()) => return Ok(()),
                Err(AccountError::Store(DialogueStoreError::Infrastructure)) => {}
                Err(error) => return Err(error),
            }
            // Only this shared listener retries connections. Clients never poll
            // account contents, and all watchers replay after LISTEN is restored.
            drop(listener);
            let mut delay = std::time::Duration::from_millis(50);
            loop {
                let result = tokio::select! {biased;()=cancellation.cancelled()=>return Ok(()), result=self.connect(pool)=>result};
                match result {
                    Ok(value) => {
                        listener = value;
                        break;
                    }
                    Err(_) => {
                        tokio::select! {biased;()=cancellation.cancelled()=>return Ok(()),()=tokio::time::sleep(delay)=>{}}
                        delay = (delay * 2).min(std::time::Duration::from_secs(5));
                    }
                }
            }
        }
    }
    async fn run_inner(
        &self,
        listener: &mut PgListener,
        cancellation: CancellationToken,
    ) -> Result<(), AccountError> {
        loop {
            tokio::select! {
                biased;
                ()=cancellation.cancelled()=>return Ok(()),
                notification=listener.try_recv()=>{
                    match notification.map_err(|_|DialogueStoreError::Infrastructure)?{
                        Some(notification) if notification.channel() == HANDOFF_CHANNEL => self.wake_handoff(),
                        Some(notification)=>{
                            let space_id=notification.payload().parse().map_err(|_|DialogueStoreError::Corrupt)?;
                            self.signals.notify(space_id).map_err(|_|DialogueStoreError::Infrastructure)?;
                        },
                        // sqlx signals a reconnect gap with None. Every subscribed
                        // account must replay its durable cursor after that gap.
                        None=>self.replay_after_gap()?,
                    }
                }
            }
        }
    }
}
struct AccountWatcher {
    signal: KeyedSignalSubscription<OwnerSpaceId>,
    available: Mutex<watch::Receiver<bool>>,
}
impl AccountEventWatcher for AccountWatcher {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), AccountError>> {
        Box::pin(async move {
            let mut available = self.available.lock().await;
            if !*available.borrow_and_update() {
                return Err(DialogueStoreError::Infrastructure.into());
            }
            tokio::select! {
                result=self.signal.wait_for_change()=>result.map_err(|_|DialogueStoreError::Infrastructure.into()),
                result=available.changed()=>{result.map_err(|_|DialogueStoreError::Infrastructure)?;if !*available.borrow_and_update(){Err(DialogueStoreError::Infrastructure.into())}else{Ok(())}}
            }
        })
    }
}
impl AccountChangeFeed for PostgresAccountChangeFeed {
    fn subscribe(
        &self,
        space_id: OwnerSpaceId,
    ) -> Result<Arc<dyn AccountEventWatcher>, AccountError> {
        if !*self.available.borrow() {
            return Err(DialogueStoreError::Infrastructure.into());
        }
        let signal = self
            .signals
            .subscribe(space_id)
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(Arc::new(AccountWatcher {
            signal,
            available: Mutex::new(self.available.subscribe()),
        }))
    }
}
