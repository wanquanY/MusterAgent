//! Reusable HTTP pools keyed by the effective registry connection budget.
use super::*;

const MAX_CLIENT_BUDGETS: usize = 64;

pub(super) struct ProviderHttpClients {
    config: ProviderAdapterConfig,
    clients: Mutex<BTreeMap<(Duration, bool), reqwest::Client>>,
}

impl ProviderHttpClients {
    pub(super) fn new(config: ProviderAdapterConfig) -> Result<Self, ProviderError> {
        let clients = Self {
            config,
            clients: Mutex::new(BTreeMap::new()),
        };
        clients.for_connect_timeout(clients.config.connect_timeout)?;
        Ok(clients)
    }

    pub(super) fn for_connect_timeout(
        &self,
        budget: Duration,
    ) -> Result<reqwest::Client, ProviderError> {
        self.for_transport(budget, false)
    }

    pub(super) fn for_transport(
        &self,
        budget: Duration,
        private_transport: bool,
    ) -> Result<reqwest::Client, ProviderError> {
        self.pool(budget, private_transport)
    }

    fn pool(
        &self,
        budget: Duration,
        private_transport: bool,
    ) -> Result<reqwest::Client, ProviderError> {
        if budget.is_zero() {
            return Err(provider_error(
                ProviderErrorKind::InvalidRequest,
                ProviderErrorCode::RouteConfigurationInvalid,
                false,
                None,
            ));
        }
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(client) = clients.get(&(budget, private_transport)) {
            return Ok(client.clone());
        }
        let builder = reqwest::Client::builder()
            .connect_timeout(budget)
            .pool_max_idle_per_host(self.config.max_idle_per_host)
            .pool_idle_timeout(self.config.pool_idle_timeout)
            .user_agent(&self.config.user_agent);
        let builder = if private_transport {
            builder
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
        } else {
            builder
        };
        let client = builder.build().map_err(|error| transport_error(&error))?;
        if clients.len() >= MAX_CLIENT_BUDGETS {
            // In-flight requests own clones; eviction cannot cancel their I/O.
            clients.pop_first();
        }
        clients.insert((budget, private_transport), client.clone());
        Ok(client)
    }
}

pub(crate) fn transport_error(error: &reqwest::Error) -> ProviderError {
    let reason = if error.is_timeout() && error.is_connect() {
        "connect_timeout"
    } else if error.is_timeout() {
        "transport_timeout"
    } else if error.is_connect() {
        "connect_failed"
    } else if error.is_body() {
        "body_failed"
    } else {
        "transport_failed"
    };
    tracing::warn!(provider.failure = reason, "Model transport failed");
    if error.is_timeout() {
        timeout_error()
    } else {
        provider_error(
            ProviderErrorKind::Transport,
            ProviderErrorCode::TransportFailed,
            true,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_connect_budget_overrides_default_and_reuses_pool() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept");
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let clients = ProviderHttpClients::new(ProviderAdapterConfig {
            connect_timeout: Duration::from_secs(3),
            ..ProviderAdapterConfig::default()
        })
        .expect("clients");
        let budget = Duration::from_millis(100);
        let client = clients
            .for_connect_timeout(budget)
            .expect("registry budget");
        clients.for_connect_timeout(budget).expect("reuse");
        assert_eq!(clients.clients.lock().expect("lock").len(), 2);
        let started = Instant::now();
        let error = client
            .get(format!("https://{addr}/"))
            .send()
            .await
            .expect_err("TLS handshake stalls");
        assert!(error.is_timeout());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must not use default three seconds"
        );
        assert_eq!(
            transport_error(&error).safe_code,
            ProviderErrorCode::DeadlineExceeded
        );
        server.abort();
    }
}
