use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex as StdMutex, Weak},
};
use tokio::sync::{Mutex, watch};

/// Process-local, key-addressed notification fan-out.
///
/// A watch channel intentionally coalesces duplicate hints. Every consumer
/// reconciles its durable Cursor after waking, so retaining an unbounded event
/// queue would add memory pressure without adding correctness. Weak topic
/// entries ensure idle Conversation and tenant keys do not accumulate.
#[derive(Debug, Clone)]
pub(crate) struct KeyedSignalHub<K> {
    topics: Arc<StdMutex<HashMap<K, Weak<KeyedSignalTopic>>>>,
}

impl<K> Default for KeyedSignalHub<K> {
    fn default() -> Self {
        Self {
            topics: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
struct KeyedSignalTopic {
    revision: watch::Sender<u64>,
}

impl<K> KeyedSignalHub<K>
where
    K: Copy + Eq + Hash,
{
    pub(crate) fn subscribe(&self, key: K) -> Result<KeyedSignalSubscription<K>, ()> {
        let mut topics = self.topics.lock().map_err(|_| ())?;
        let topic = topics.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
            let (revision, _) = watch::channel(0);
            let topic = Arc::new(KeyedSignalTopic { revision });
            topics.insert(key, Arc::downgrade(&topic));
            topic
        });
        let receiver = topic.revision.subscribe();
        Ok(KeyedSignalSubscription {
            key,
            topic: Some(topic),
            topics: Arc::clone(&self.topics),
            receiver: Mutex::new(receiver),
        })
    }

    pub(crate) fn notify(&self, key: K) -> Result<(), ()> {
        let topic = {
            let mut topics = self.topics.lock().map_err(|_| ())?;
            match topics.get(&key).and_then(Weak::upgrade) {
                Some(topic) => topic,
                None => {
                    topics.remove(&key);
                    return Ok(());
                }
            }
        };
        topic
            .revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        let weak = Arc::downgrade(&topic);
        drop(topic);
        remove_dead_topic(&self.topics, key, &weak);
        Ok(())
    }

    pub(crate) fn notify_all(&self) -> Result<(), ()> {
        let keys = self
            .topics
            .lock()
            .map_err(|_| ())?
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for key in keys {
            self.notify(key)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn active_key_count(&self) -> usize {
        self.topics
            .lock()
            .map(|topics| {
                topics
                    .values()
                    .filter(|topic| topic.strong_count() > 0)
                    .count()
            })
            .unwrap_or_default()
    }
}

pub(crate) struct KeyedSignalSubscription<K>
where
    K: Copy + Eq + Hash,
{
    key: K,
    topic: Option<Arc<KeyedSignalTopic>>,
    topics: Arc<StdMutex<HashMap<K, Weak<KeyedSignalTopic>>>>,
    receiver: Mutex<watch::Receiver<u64>>,
}

impl<K> KeyedSignalSubscription<K>
where
    K: Copy + Eq + Hash,
{
    pub(crate) async fn wait_for_change(&self) -> Result<(), ()> {
        self.receiver.lock().await.changed().await.map_err(|_| ())
    }
}

impl<K> Drop for KeyedSignalSubscription<K>
where
    K: Copy + Eq + Hash,
{
    fn drop(&mut self) {
        if let Some(topic) = self.topic.take() {
            let weak = Arc::downgrade(&topic);
            drop(topic);
            remove_dead_topic(&self.topics, self.key, &weak);
        }
    }
}

fn remove_dead_topic<K>(
    topics: &Arc<StdMutex<HashMap<K, Weak<KeyedSignalTopic>>>>,
    key: K,
    expected: &Weak<KeyedSignalTopic>,
) where
    K: Copy + Eq + Hash,
{
    if expected.strong_count() > 0 {
        return;
    }
    let Ok(mut topics) = topics.lock() else {
        return;
    };
    if topics
        .get(&key)
        .is_some_and(|current| current.strong_count() == 0 && Weak::ptr_eq(current, expected))
    {
        topics.remove(&key);
    }
}
