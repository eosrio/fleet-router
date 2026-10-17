//! Request classification on the raw request-target, in the order nodeos
//! handles a request: the target check first, then OPTIONS, then an exact
//! lookup of the whole target, query string included. A target with a query
//! string therefore matches nothing and gets nodeos's 404, as it does on a
//! node.

use http::{Method, Uri};

use crate::fleet::MetricLabel;
use crate::models::ApiClass;

/// A block endpoint, routed by the block number in its body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockEndpoint {
    /// `get_block`: decoded action data, so live nodes only.
    Block,
    /// `get_block_header`.
    BlockHeader,
    /// `get_block_header_state`: fork state, so live nodes only.
    BlockHeaderState,
    /// `get_block_info`, keyed by `block_num`.
    BlockInfo,
    /// `get_raw_block`.
    RawBlock,
}

impl BlockEndpoint {
    pub const ALL: [BlockEndpoint; 5] = [
        BlockEndpoint::Block,
        BlockEndpoint::BlockHeader,
        BlockEndpoint::BlockHeaderState,
        BlockEndpoint::BlockInfo,
        BlockEndpoint::RawBlock,
    ];

    pub fn path(self) -> &'static str {
        match self {
            BlockEndpoint::Block => "/v1/chain/get_block",
            BlockEndpoint::BlockHeader => "/v1/chain/get_block_header",
            BlockEndpoint::BlockHeaderState => "/v1/chain/get_block_header_state",
            BlockEndpoint::BlockInfo => "/v1/chain/get_block_info",
            BlockEndpoint::RawBlock => "/v1/chain/get_raw_block",
        }
    }

    /// The endpoint whose path is exactly `target`.
    pub fn from_target(target: &str) -> Option<BlockEndpoint> {
        BlockEndpoint::ALL
            .into_iter()
            .find(|ep| ep.path() == target)
    }
}

/// What the router does with a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Not an origin-form target: answered with "Illegal request-target".
    IllegalTarget,
    /// Any OPTIONS request with a legal target: answered `200 {}`.
    Options,
    /// `/v1/node/get_supported_apis`, answered by the router.
    SupportedApis,
    /// Routed by block number.
    Block(BlockEndpoint),
    /// `get_info`, from one live node.
    Info,
    /// Any other read-only `/v1/chain/*` call, from live nodes.
    State,
    /// `compute_transaction`, `send_read_only_transaction`.
    Exec,
    /// Transaction pushes.
    Push,
    /// nodeos's "Unknown Endpoint" 404: an unknown path, a query string, a
    /// class missing from `allow`, `push_block`, or any non-chain API.
    NotFound,
}

/// `class` label of `fleet_router_api_requests_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequestClass {
    Block,
    Info,
    State,
    Exec,
    Push,
    /// Answered by the router: OPTIONS and `get_supported_apis`.
    Node,
    /// Answered with the 404.
    Denied,
    /// Illegal request-target.
    Invalid,
}

impl MetricLabel for RequestClass {
    const ALL: &'static [RequestClass] = &[
        RequestClass::Block,
        RequestClass::Info,
        RequestClass::State,
        RequestClass::Exec,
        RequestClass::Push,
        RequestClass::Node,
        RequestClass::Denied,
        RequestClass::Invalid,
    ];

    fn as_str(self) -> &'static str {
        match self {
            RequestClass::Block => "block",
            RequestClass::Info => "info",
            RequestClass::State => "state",
            RequestClass::Exec => "exec",
            RequestClass::Push => "push",
            RequestClass::Node => "node",
            RequestClass::Denied => "denied",
            RequestClass::Invalid => "invalid",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl Route {
    /// The `allow` class that serves this route, for proxied routes.
    pub fn class(self) -> Option<ApiClass> {
        match self {
            Route::Block(_) => Some(ApiClass::Block),
            Route::Info => Some(ApiClass::Info),
            Route::State => Some(ApiClass::State),
            Route::Exec => Some(ApiClass::Exec),
            Route::Push => Some(ApiClass::Push),
            Route::IllegalTarget | Route::Options | Route::SupportedApis | Route::NotFound => None,
        }
    }

    pub fn label(self) -> RequestClass {
        match self {
            Route::Block(_) => RequestClass::Block,
            Route::Info => RequestClass::Info,
            Route::State => RequestClass::State,
            Route::Exec => RequestClass::Exec,
            Route::Push => RequestClass::Push,
            Route::Options | Route::SupportedApis => RequestClass::Node,
            Route::NotFound => RequestClass::Denied,
            Route::IllegalTarget => RequestClass::Invalid,
        }
    }
}

const SUPPORTED_APIS: &str = "/v1/node/get_supported_apis";
const CHAIN_PREFIX: &str = "/v1/chain/";

/// The request-target as nodeos sees it, or `None` when nodeos answers
/// "Illegal request-target": the target must be origin-form (no scheme, no
/// authority, so `*` and absolute-form fail), start with `/` and contain no
/// `..` anywhere.
pub fn origin_target(uri: &Uri) -> Option<&str> {
    if uri.scheme().is_some() || uri.authority().is_some() {
        return None;
    }
    let target = uri.path_and_query()?.as_str();
    (target.starts_with('/') && !target.contains("..")).then_some(target)
}

/// The proxied route a legal target names, ignoring `allow`. `None` for the
/// router-answered and denied targets.
fn proxied(target: &str) -> Option<Route> {
    if let Some(ep) = BlockEndpoint::from_target(target) {
        return Some(Route::Block(ep));
    }
    let name = target.strip_prefix(CHAIN_PREFIX)?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return None;
    }
    Some(match name {
        "get_info" => Route::Info,
        "compute_transaction" | "send_read_only_transaction" => Route::Exec,
        "push_transaction" | "push_transactions" | "send_transaction" | "send_transaction2" => {
            Route::Push
        }
        // Never forwarded: a block from a client is not this router's to relay.
        "push_block" => return None,
        _ => Route::State,
    })
}

/// Classify a request. Any method reaches its handler; only OPTIONS is
/// answered before dispatch.
pub fn classify(method: &Method, uri: &Uri, allow: &[ApiClass]) -> Route {
    let Some(target) = origin_target(uri) else {
        return Route::IllegalTarget;
    };
    if method == Method::OPTIONS {
        return Route::Options;
    }
    if target == SUPPORTED_APIS {
        return Route::SupportedApis;
    }
    match proxied(target) {
        Some(route) if route.class().is_some_and(|class| allow.contains(&class)) => route,
        _ => Route::NotFound,
    }
}

/// Whether the listener serves `path` for clients: what
/// `get_supported_apis` may list. Never itself, never a denied path.
pub fn serves_path(path: &str, allow: &[ApiClass]) -> bool {
    proxied(path)
        .and_then(Route::class)
        .is_some_and(|class| allow.contains(&class))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CLASSES: [ApiClass; 5] = [
        ApiClass::Block,
        ApiClass::Info,
        ApiClass::State,
        ApiClass::Exec,
        ApiClass::Push,
    ];

    fn uri(target: &str) -> Uri {
        target.parse().unwrap()
    }

    fn post(target: &str, allow: &[ApiClass]) -> Route {
        classify(&Method::POST, &uri(target), allow)
    }

    // CU1
    #[test]
    fn every_endpoint_row_is_classified() {
        let rows = [
            ("/v1/chain/get_block", Route::Block(BlockEndpoint::Block)),
            (
                "/v1/chain/get_raw_block",
                Route::Block(BlockEndpoint::RawBlock),
            ),
            (
                "/v1/chain/get_block_header",
                Route::Block(BlockEndpoint::BlockHeader),
            ),
            (
                "/v1/chain/get_block_header_state",
                Route::Block(BlockEndpoint::BlockHeaderState),
            ),
            (
                "/v1/chain/get_block_info",
                Route::Block(BlockEndpoint::BlockInfo),
            ),
            ("/v1/chain/get_info", Route::Info),
            ("/v1/chain/compute_transaction", Route::Exec),
            ("/v1/chain/send_read_only_transaction", Route::Exec),
            ("/v1/chain/push_transaction", Route::Push),
            ("/v1/chain/push_transactions", Route::Push),
            ("/v1/chain/send_transaction", Route::Push),
            ("/v1/chain/send_transaction2", Route::Push),
            ("/v1/chain/push_block", Route::NotFound),
            ("/v1/chain/get_account", Route::State),
            ("/v1/chain/get_table_rows", Route::State),
            ("/v1/chain/get_abi", Route::State),
            ("/v1/chain/get_raw_abi", Route::State),
            ("/v1/chain/get_transaction_id", Route::State),
            ("/v1/chain/abi_json_to_bin", Route::State),
            ("/v1/chain/get_producer_schedule2", Route::State),
            ("/v1/node/get_supported_apis", Route::SupportedApis),
            ("/v1/trace_api/get_block", Route::NotFound),
            ("/v1/history/get_actions", Route::NotFound),
            ("/v1/producer/pause", Route::NotFound),
            ("/v1/net/connections", Route::NotFound),
            ("/v1/db_size/get", Route::NotFound),
            ("/v1/archive/describe", Route::NotFound),
            ("/", Route::NotFound),
            ("/v1/chain/", Route::NotFound),
            ("/v1/chain", Route::NotFound),
            ("/v1/chain/Get_Info", Route::NotFound),
            ("/v1/chain/get-info", Route::NotFound),
            ("/v1/chain/get_info/", Route::NotFound),
            ("/v1/chain/get_info/x", Route::NotFound),
            ("/v1//chain/get_info", Route::NotFound),
            ("/V1/chain/get_info", Route::NotFound),
        ];
        for (target, want) in rows {
            assert_eq!(post(target, &ALL_CLASSES), want, "{target}");
            // Any method reaches the handler.
            for method in [Method::GET, Method::PUT, Method::HEAD, Method::DELETE] {
                assert_eq!(
                    classify(&method, &uri(target), &ALL_CLASSES),
                    want,
                    "{method} {target}"
                );
            }
        }
    }

    // CU1
    #[test]
    fn query_strings_never_match_a_handler() {
        for target in [
            "/v1/chain/get_info?",
            "/v1/chain/get_raw_block?block_num_or_id=1",
            "/v1/chain/get_account?x=1",
            "/v1/node/get_supported_apis?x",
        ] {
            assert_eq!(post(target, &ALL_CLASSES), Route::NotFound, "{target}");
        }
        assert!(!serves_path("/v1/chain/get_info?x", &ALL_CLASSES));
    }

    // CU1
    #[test]
    fn illegal_targets_are_found_first() {
        let asterisk = Uri::from_static("*");
        assert_eq!(origin_target(&asterisk), None);
        assert_eq!(
            classify(&Method::OPTIONS, &asterisk, &ALL_CLASSES),
            Route::IllegalTarget
        );
        for target in [
            "http://10.0.0.1/v1/chain/get_info",
            "http://10.0.0.1:8888/v1/chain/get_info?x=1",
            "10.0.0.1:8888",
            "/v1/chain/../chain/get_info",
            "/v1/chain/get_info?x=..",
            "/..",
        ] {
            let parsed = uri(target);
            assert_eq!(origin_target(&parsed), None, "{target}");
            for method in [Method::POST, Method::OPTIONS, Method::GET] {
                assert_eq!(
                    classify(&method, &parsed, &ALL_CLASSES),
                    Route::IllegalTarget,
                    "{method} {target}"
                );
            }
        }
        // A single dot is fine.
        assert_eq!(
            origin_target(&uri("/v1/chain/./get_info")),
            Some("/v1/chain/./get_info")
        );
        assert_eq!(post("/v1/chain/./get_info", &ALL_CLASSES), Route::NotFound);
        assert_eq!(
            origin_target(&uri("/v1/chain/get_info?a=b")),
            Some("/v1/chain/get_info?a=b")
        );
    }

    // CU1
    #[test]
    fn options_is_answered_for_any_legal_target() {
        for target in [
            "/",
            "/v1/chain/get_info",
            "/v1/chain/push_block",
            "/v1/trace_api/get_block",
            "/v1/chain/get_info?x=1",
            "/v1/node/get_supported_apis",
        ] {
            assert_eq!(
                classify(&Method::OPTIONS, &uri(target), &[ApiClass::Block]),
                Route::Options,
                "{target}"
            );
        }
    }

    // CU1
    #[test]
    fn classes_missing_from_allow_get_the_404() {
        let defaults = [ApiClass::Block, ApiClass::Info, ApiClass::State];
        assert_eq!(
            post("/v1/chain/get_raw_block", &defaults),
            Route::Block(BlockEndpoint::RawBlock)
        );
        assert_eq!(
            post("/v1/chain/push_transaction", &defaults),
            Route::NotFound
        );
        assert_eq!(
            post("/v1/chain/compute_transaction", &defaults),
            Route::NotFound
        );

        let blocks_only = [ApiClass::Block];
        assert_eq!(post("/v1/chain/get_info", &blocks_only), Route::NotFound);
        assert_eq!(post("/v1/chain/get_account", &blocks_only), Route::NotFound);
        assert_eq!(
            post("/v1/chain/get_block_info", &blocks_only),
            Route::Block(BlockEndpoint::BlockInfo)
        );

        let no_blocks = [ApiClass::Info, ApiClass::State, ApiClass::Push];
        for ep in BlockEndpoint::ALL {
            assert_eq!(post(ep.path(), &no_blocks), Route::NotFound, "{ep:?}");
        }
        assert_eq!(post("/v1/chain/send_transaction", &no_blocks), Route::Push);
        // `push_block` stays denied whatever is allowed, and
        // `get_supported_apis` is served whatever is allowed.
        assert_eq!(post("/v1/chain/push_block", &ALL_CLASSES), Route::NotFound);
        assert_eq!(
            post("/v1/node/get_supported_apis", &[ApiClass::Exec]),
            Route::SupportedApis
        );
    }

    #[test]
    fn served_paths_follow_allow_and_never_include_router_answers() {
        let defaults = [ApiClass::Block, ApiClass::Info, ApiClass::State];
        assert!(serves_path("/v1/chain/get_block", &defaults));
        assert!(serves_path("/v1/chain/get_info", &defaults));
        assert!(serves_path("/v1/chain/get_account", &defaults));
        assert!(!serves_path("/v1/chain/push_transaction", &defaults));
        assert!(serves_path("/v1/chain/push_transaction", &ALL_CLASSES));
        assert!(!serves_path("/v1/chain/push_block", &ALL_CLASSES));
        assert!(!serves_path("/v1/node/get_supported_apis", &ALL_CLASSES));
        assert!(!serves_path("/v1/archive/describe", &ALL_CLASSES));
        assert!(!serves_path("/v1/trace_api/get_block", &ALL_CLASSES));
    }

    #[test]
    fn routes_map_to_classes_and_labels() {
        assert_eq!(
            Route::Block(BlockEndpoint::Block).class(),
            Some(ApiClass::Block)
        );
        assert_eq!(Route::Push.class(), Some(ApiClass::Push));
        assert_eq!(Route::Options.class(), None);
        assert_eq!(Route::SupportedApis.label().as_str(), "node");
        assert_eq!(Route::Options.label().as_str(), "node");
        assert_eq!(Route::NotFound.label().as_str(), "denied");
        assert_eq!(Route::IllegalTarget.label().as_str(), "invalid");
        assert_eq!(Route::Exec.label().as_str(), "exec");
        for (i, label) in RequestClass::ALL.iter().enumerate() {
            assert_eq!(label.index(), i);
        }
        for ep in BlockEndpoint::ALL {
            assert_eq!(BlockEndpoint::from_target(ep.path()), Some(ep));
        }
        assert_eq!(BlockEndpoint::from_target("/v1/chain/get_blocks"), None);
    }
}
