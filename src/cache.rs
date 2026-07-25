use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use dashmap::{DashMap, mapref::entry::Entry};
use moka::{Expiry, future::Cache};
use serde_json::Value;
use tokio::sync::Notify;

use crate::{
    classify::{CacheKey, CachePlan},
    config::Config,
};

#[derive(Clone, Debug)]
pub struct CachedResponse {
    template: Value,
    ttl: Duration,
    weight: u32,
}

impl CachedResponse {
    pub fn from_success(response: &Value, ttl: Duration) -> Option<Arc<Self>> {
        let mut template = response.clone();
        let object = template.as_object_mut()?;
        if !object.contains_key("result") || object.contains_key("error") {
            return None;
        }
        object.remove("id");
        let weight = serde_json::to_vec(&template)
            .ok()?
            .len()
            .max(1)
            .min(u32::MAX as usize) as u32;
        Some(Arc::new(Self {
            template,
            ttl,
            weight,
        }))
    }

    pub fn with_id(&self, id: Value) -> Value {
        let mut response = self.template.clone();
        if let Some(object) = response.as_object_mut() {
            object.insert("id".to_owned(), id);
        }
        response
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }
}

struct PerEntryExpiry;

impl Expiry<CacheKey, Arc<CachedResponse>> for PerEntryExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &Arc<CachedResponse>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }

    fn expire_after_update(
        &self,
        _key: &CacheKey,
        value: &Arc<CachedResponse>,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

#[derive(Clone)]
enum FlightResult {
    Success(Arc<CachedResponse>),
    Failed,
}

struct Flight {
    result: Mutex<Option<FlightResult>>,
    notify: Notify,
}

impl Flight {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    fn complete(&self, result: FlightResult) {
        *lock(&self.result) = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Option<Arc<CachedResponse>> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = lock(&self.result).clone() {
                return match result {
                    FlightResult::Success(response) => Some(response),
                    FlightResult::Failed => None,
                };
            }
            notified.await;
        }
    }
}

pub enum CacheLookup {
    Hit(Arc<CachedResponse>),
    Leader(FlightLeader),
    Follower(FlightFollower),
}

pub struct FlightFollower {
    flight: Arc<Flight>,
}

impl FlightFollower {
    pub async fn wait(self) -> Option<Arc<CachedResponse>> {
        self.flight.wait().await
    }
}

pub struct FlightLeader {
    key: CacheKey,
    flight: Arc<Flight>,
    flights: Arc<DashMap<CacheKey, Arc<Flight>>>,
    completed: bool,
}

impl FlightLeader {
    pub fn complete_success(mut self, response: Arc<CachedResponse>) {
        self.flight.complete(FlightResult::Success(response));
        self.flights.remove(&self.key);
        self.completed = true;
    }

    pub fn complete_failure(mut self) {
        self.flight.complete(FlightResult::Failed);
        self.flights.remove(&self.key);
        self.completed = true;
    }
}

impl Drop for FlightLeader {
    fn drop(&mut self) {
        if !self.completed {
            self.flight.complete(FlightResult::Failed);
            self.flights.remove(&self.key);
        }
    }
}

pub struct ResponseCache {
    entries: Cache<CacheKey, Arc<CachedResponse>>,
    flights: Arc<DashMap<CacheKey, Arc<Flight>>>,
}

impl ResponseCache {
    pub fn new(config: &Config) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.cache.max_bytes)
            .weigher(|_key: &CacheKey, value: &Arc<CachedResponse>| value.weight())
            .expire_after(PerEntryExpiry)
            .build();
        Self {
            entries,
            flights: Arc::new(DashMap::new()),
        }
    }

    pub async fn lookup(&self, plan: CachePlan) -> CacheLookup {
        if let Some(response) = self.entries.get(&plan.key).await {
            return CacheLookup::Hit(response);
        }
        match self.flights.entry(plan.key) {
            Entry::Occupied(entry) => CacheLookup::Follower(FlightFollower {
                flight: Arc::clone(entry.get()),
            }),
            Entry::Vacant(entry) => {
                let flight = Arc::new(Flight::new());
                entry.insert(Arc::clone(&flight));
                CacheLookup::Leader(FlightLeader {
                    key: plan.key,
                    flight,
                    flights: Arc::clone(&self.flights),
                    completed: false,
                })
            }
        }
    }

    pub async fn insert(&self, plan: CachePlan, response: Arc<CachedResponse>) {
        self.entries.insert(plan.key, response).await;
    }

    pub fn entry_count(&self) -> u64 {
        self.entries.entry_count()
    }

    pub fn weighted_size(&self) -> u64 {
        self.entries.weighted_size()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::classify::CacheClass;

    use super::*;

    fn plan(key: u8, ttl: Duration) -> CachePlan {
        CachePlan {
            key: [key; 32],
            class: CacheClass::Tip,
            ttl,
        }
    }

    #[test]
    fn response_template_rewrites_request_id_and_rejects_errors() {
        let cached = CachedResponse::from_success(
            &json!({"jsonrpc":"2.0","id":1,"result":"0xabc"}),
            Duration::from_secs(1),
        )
        .expect("cacheable result");
        assert_eq!(cached.with_id(json!(99))["id"], 99);
        assert_eq!(cached.with_id(json!(99))["result"], "0xabc");
        assert!(
            CachedResponse::from_success(
                &json!({"jsonrpc":"2.0","id":1,"error":{"code":3}}),
                Duration::from_secs(1)
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn cache_uses_per_entry_ttl() {
        let config = Config {
            cache: crate::config::CacheConfig {
                max_bytes: 1024,
                immutable_ttl_seconds: 3600,
            },
            ..Config::default()
        };
        let cache = ResponseCache::new(&config);
        let plan = plan(1, Duration::from_millis(20));
        let response =
            CachedResponse::from_success(&json!({"jsonrpc":"2.0","id":1,"result":"0x1"}), plan.ttl)
                .expect("response");
        cache.insert(plan, response).await;
        assert!(matches!(cache.lookup(plan).await, CacheLookup::Hit(_)));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!matches!(cache.lookup(plan).await, CacheLookup::Hit(_)));
    }

    #[tokio::test]
    async fn followers_receive_success_but_not_failure() {
        let cache = ResponseCache::new(&Config::default());
        let cache_plan = plan(2, Duration::from_secs(1));
        let CacheLookup::Leader(leader) = cache.lookup(cache_plan).await else {
            panic!("first lookup must lead");
        };
        let CacheLookup::Follower(follower) = cache.lookup(cache_plan).await else {
            panic!("second lookup must follow");
        };
        let response = CachedResponse::from_success(
            &json!({"jsonrpc":"2.0","id":1,"result":"0x2"}),
            cache_plan.ttl,
        )
        .expect("response");
        leader.complete_success(Arc::clone(&response));
        assert_eq!(
            follower.wait().await.expect("shared").with_id(json!(8))["id"],
            8
        );

        let failure_plan = plan(3, Duration::from_secs(1));
        let CacheLookup::Leader(leader) = cache.lookup(failure_plan).await else {
            panic!("failure leader");
        };
        let CacheLookup::Follower(follower) = cache.lookup(failure_plan).await else {
            panic!("failure follower");
        };
        leader.complete_failure();
        assert!(follower.wait().await.is_none());
    }
}
