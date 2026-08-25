use std::{collections::HashMap, time::Duration};

use serde_json::{Value, value::RawValue};

use crate::config::Config;

pub type CacheKey = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheClass {
    Immutable,
    ImmutableByResponse { finalized_before: u64 },
    Tip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachePlan {
    pub key: CacheKey,
    pub class: CacheClass,
    pub ttl: Duration,
}

#[derive(Clone, Copy)]
struct ChainCacheSettings {
    confirmation_depth: u64,
    tip_ttl: Duration,
}

pub struct Classifier {
    chains: HashMap<u64, ChainCacheSettings>,
    default_settings: ChainCacheSettings,
    immutable_ttl: Duration,
}

impl Classifier {
    pub fn new(config: &Config) -> Self {
        let chains = config
            .chains
            .iter()
            .copied()
            .map(|chain_id| {
                (
                    chain_id,
                    ChainCacheSettings {
                        confirmation_depth: config.confirmation_depth(chain_id),
                        tip_ttl: Duration::from_millis(config.tip_ttl_ms(chain_id)),
                    },
                )
            })
            .collect();
        // 默认参数：64 块确认深度，tip TTL = min(block_time, 2s)，block_time 未知按 2s。
        let default_block_time_ms = 2_000u64;
        let default_tip_ttl = Duration::from_millis(default_block_time_ms.min(2_000));
        Self {
            chains,
            default_settings: ChainCacheSettings {
                confirmation_depth: 64,
                tip_ttl: default_tip_ttl,
            },
            immutable_ttl: Duration::from_secs(config.cache.immutable_ttl_seconds),
        }
    }

    pub fn cache_plan(
        &self,
        chain_id: u64,
        method: &str,
        params: Option<&RawValue>,
        head: u64,
    ) -> Option<CachePlan> {
        let settings = self.chains.get(&chain_id).unwrap_or(&self.default_settings);
        let params_value = parse_params(params)?;
        let class = classify_method(method, &params_value, head, settings.confirmation_depth)?;
        let canonical_params = serde_json::to_vec(&params_value).ok()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&chain_id.to_be_bytes());
        hasher.update(&[0]);
        hasher.update(method.as_bytes());
        hasher.update(&[0]);
        hasher.update(&canonical_params);
        match class {
            CacheClass::Immutable => {
                hasher.update(&[1]);
            }
            CacheClass::ImmutableByResponse { .. } => {
                hasher.update(&[3]);
            }
            CacheClass::Tip => {
                hasher.update(&[2]);
                hasher.update(&head.to_be_bytes());
            }
        };
        Some(CachePlan {
            key: *hasher.finalize().as_bytes(),
            class,
            ttl: match class {
                CacheClass::Immutable | CacheClass::ImmutableByResponse { .. } => {
                    self.immutable_ttl
                }
                CacheClass::Tip => settings.tip_ttl,
            },
        })
    }

    pub fn is_read_only(&self, method: &str) -> bool {
        matches!(
            method,
            "eth_blockNumber"
                | "eth_chainId"
                | "eth_gasPrice"
                | "eth_feeHistory"
                | "eth_call"
                | "eth_getBalance"
                | "eth_getCode"
                | "eth_getStorageAt"
                | "eth_getBlockByNumber"
                | "eth_getBlockByHash"
                | "eth_getTransactionByHash"
                | "eth_getTransactionReceipt"
                | "eth_getTransactionByBlockHashAndIndex"
                | "eth_getBlockTransactionCountByHash"
                | "eth_getUncleCountByBlockHash"
        )
    }
}

fn parse_params(params: Option<&RawValue>) -> Option<Value> {
    match params {
        Some(params) => serde_json::from_str(params.get()).ok(),
        None => Some(Value::Array(Vec::new())),
    }
}

fn classify_method(
    method: &str,
    params: &Value,
    head: u64,
    confirmations: u64,
) -> Option<CacheClass> {
    if method == "eth_sendRawTransaction"
        || method.contains("Filter")
        || method.contains("filter")
        || method.contains("subscribe")
        || method.contains("Subscription")
    {
        return None;
    }
    match method {
        "eth_chainId" => Some(CacheClass::Immutable),
        "eth_blockNumber" | "eth_gasPrice" | "eth_feeHistory" => Some(CacheClass::Tip),
        "eth_getBlockByHash"
        | "eth_getTransactionByBlockHashAndIndex"
        | "eth_getBlockTransactionCountByHash"
        | "eth_getUncleCountByBlockHash" => Some(CacheClass::Immutable),
        "eth_getTransactionByHash" | "eth_getTransactionReceipt" => {
            Some(CacheClass::ImmutableByResponse {
                finalized_before: head.saturating_sub(confirmations),
            })
        }
        "eth_getBlockByNumber" => classify_block_param(params, 0, head, confirmations, false),
        "eth_call" | "eth_getBalance" | "eth_getCode" => {
            classify_last_block_param(params, head, confirmations, false)
        }
        "eth_getStorageAt" => classify_block_param(params, 2, head, confirmations, false),
        "eth_getTransactionCount" => classify_last_block_param(params, head, confirmations, true),
        _ => None,
    }
}

fn classify_last_block_param(
    params: &Value,
    head: u64,
    confirmations: u64,
    latest_is_uncacheable: bool,
) -> Option<CacheClass> {
    let values = params.as_array()?;
    let index = values.len().checked_sub(1)?;
    classify_block_param(params, index, head, confirmations, latest_is_uncacheable)
}

fn classify_block_param(
    params: &Value,
    index: usize,
    head: u64,
    confirmations: u64,
    latest_is_uncacheable: bool,
) -> Option<CacheClass> {
    let tag = params.as_array().and_then(|values| values.get(index));
    let Some(tag) = tag else {
        return (!latest_is_uncacheable).then_some(CacheClass::Tip);
    };
    let Some(tag) = tag.as_str() else {
        return Some(CacheClass::Tip);
    };
    match tag {
        "latest" | "pending" if latest_is_uncacheable => None,
        "latest" | "pending" | "safe" | "finalized" => Some(CacheClass::Tip),
        "earliest" => Some(CacheClass::Immutable),
        value => parse_block_number(value).map(|block| {
            if block < head.saturating_sub(confirmations) {
                CacheClass::Immutable
            } else {
                CacheClass::Tip
            }
        }),
    }
}

fn parse_block_number(value: &str) -> Option<u64> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    u64::from_str_radix(digits, 16).ok()
}

#[cfg(test)]
mod tests {
    use serde_json::{json, value::to_raw_value};

    use super::*;

    fn classifier() -> Classifier {
        Classifier::new(&Config::default())
    }

    fn raw(value: Value) -> Box<RawValue> {
        to_raw_value(&value).expect("raw params")
    }

    #[test]
    fn classifies_immutable_tip_and_uncacheable_buckets() {
        let classifier = classifier();
        let old = raw(json!(["0x100", false]));
        let recent = raw(json!(["0x3f0", false]));
        let latest = raw(json!(["0xabc", "latest"]));
        assert_eq!(
            classifier
                .cache_plan(1, "eth_getBlockByNumber", Some(&old), 1_024)
                .expect("old plan")
                .class,
            CacheClass::Immutable
        );
        assert_eq!(
            classifier
                .cache_plan(1, "eth_getBlockByNumber", Some(&recent), 1_024)
                .expect("recent plan")
                .class,
            CacheClass::Tip
        );
        assert_eq!(
            classifier
                .cache_plan(1, "eth_getBalance", Some(&latest), 1_024)
                .expect("latest plan")
                .class,
            CacheClass::Tip
        );
        assert!(
            classifier
                .cache_plan(1, "eth_getTransactionCount", Some(&latest), 1_024)
                .is_none()
        );
        assert!(
            classifier
                .cache_plan(1, "eth_sendRawTransaction", None, 1_024)
                .is_none()
        );
        assert!(
            classifier
                .cache_plan(1, "unknown_method", None, 1_024)
                .is_none()
        );
    }

    #[test]
    fn tip_key_changes_with_head_but_immutable_key_does_not() {
        let classifier = classifier();
        let old = raw(json!(["0x100", false]));
        let tip_a = classifier
            .cache_plan(1, "eth_blockNumber", None, 100)
            .expect("tip a");
        let tip_b = classifier
            .cache_plan(1, "eth_blockNumber", None, 101)
            .expect("tip b");
        let old_a = classifier
            .cache_plan(1, "eth_getBlockByNumber", Some(&old), 1_024)
            .expect("old a");
        let old_b = classifier
            .cache_plan(1, "eth_getBlockByNumber", Some(&old), 2_048)
            .expect("old b");
        assert_ne!(tip_a.key, tip_b.key);
        assert_eq!(old_a.key, old_b.key);
        assert!(old_a.ttl >= Duration::from_secs(60 * 60));
    }

    #[test]
    fn canonical_params_ignore_whitespace_but_preserve_object_order() {
        let classifier = classifier();
        let first = RawValue::from_string(r#"[{"to":"0x1","data":"0x"},"latest"]"#.to_owned())
            .expect("first raw");
        let spaced =
            RawValue::from_string(r#"[ { "to" : "0x1", "data" : "0x" }, "latest" ]"#.to_owned())
                .expect("spaced raw");
        let reordered = RawValue::from_string(r#"[{"data":"0x","to":"0x1"},"latest"]"#.to_owned())
            .expect("reordered raw");
        let first = classifier
            .cache_plan(1, "eth_call", Some(&first), 100)
            .expect("first");
        let spaced = classifier
            .cache_plan(1, "eth_call", Some(&spaced), 100)
            .expect("spaced");
        let reordered = classifier
            .cache_plan(1, "eth_call", Some(&reordered), 100)
            .expect("reordered");
        assert_eq!(first.key, spaced.key);
        assert_ne!(first.key, reordered.key);
    }

    #[test]
    fn chain_specific_tip_ttl_uses_block_time_cap() {
        let classifier = classifier();
        assert_eq!(
            classifier
                .cache_plan(1, "eth_blockNumber", None, 1)
                .expect("eth")
                .ttl,
            Duration::from_secs(2)
        );
        assert_eq!(
            classifier
                .cache_plan(143, "eth_blockNumber", None, 1)
                .expect("monad")
                .ttl,
            Duration::from_millis(400)
        );
    }

    #[test]
    fn transaction_hash_reads_are_deferred_until_response_finality_is_known() {
        let classifier = classifier();
        let params = raw(json!(["0xabc"]));
        let plan = classifier
            .cache_plan(1, "eth_getTransactionReceipt", Some(&params), 1_000)
            .expect("receipt plan");
        assert_eq!(
            plan.class,
            CacheClass::ImmutableByResponse {
                finalized_before: 936
            }
        );
    }
}
