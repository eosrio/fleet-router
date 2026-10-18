//! Bodies the router writes itself, in nodeos's error shape
//! (`{"code":H,"message":M,"error":{...}}`), so a client parses them like any
//! node answer. The router writes only these five kinds; every parameter
//! error and every Unknown Block comes from an upstream.

/// `error.code` of nodeos's `unknown_block_exception`.
pub const UNKNOWN_BLOCK_CODE: i64 = 3_100_002;

/// The answer to every OPTIONS request, with status 200.
pub const OPTIONS_BODY: &[u8] = b"{}";

/// nodeos's answer to a request-target that is not origin-form.
const ILLEGAL_TARGET: &str = r#"{"code":400,"message":"Illegal request-target","error":{"code":0,"name":"","what":"","details":[]}}"#;

/// nodeos's answer to an unknown or disabled endpoint.
const NOT_FOUND: &str = r#"{"code":404,"message":"Not Found","error":{"code":0,"name":"exception","what":"unspecified","details":[{"message":"Unknown Endpoint","file":"beast_http_session.hpp","line_number":186,"method":"handle_request"}]}}"#;

/// Why the router answers 503. A client retries a 503; it never mistakes it
/// for an answer about the block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Busy {
    /// The listener is at `max_in_flight` (nodeos's wording).
    InFlight(usize),
    /// No upstream could answer: none was a candidate, or every attempt
    /// failed in transport or was busy.
    NoUpstream,
    /// `get_info` with no live API up.
    NoLiveUpstream,
    /// A push lost its connection after the request was written; it is
    /// never resent.
    LostAfterSend,
}

impl Busy {
    /// The `error.what` text.
    pub fn what(self) -> String {
        match self {
            Busy::InFlight(n) => format!("Too many requests in flight: {n}"),
            Busy::NoUpstream => "No chain API upstream available".to_string(),
            Busy::NoLiveUpstream => "No live chain API upstream available".to_string(),
            Busy::LostAfterSend => {
                "Upstream connection lost after the request was sent".to_string()
            }
        }
    }
}

/// A response the router generates instead of proxying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouterError {
    /// The request-target is not origin-form.
    IllegalTarget,
    /// Unknown or disabled endpoint.
    NotFound,
    /// The request body exceeds the limit (bytes); the connection is closed
    /// after the answer.
    TooLarge(usize),
    /// The upstream's answer is longer than `max_response_bytes` (the limit).
    /// Not a 503: another attempt would get the same answer.
    AnswerTooLarge(u64),
    Busy(Busy),
}

impl RouterError {
    pub fn status(self) -> u16 {
        match self {
            RouterError::IllegalTarget => 400,
            RouterError::NotFound => 404,
            RouterError::TooLarge(_) => 413,
            RouterError::AnswerTooLarge(_) => 502,
            RouterError::Busy(_) => 503,
        }
    }

    /// The JSON body, sent with `Content-Type: application/json`.
    pub fn body(self) -> Vec<u8> {
        match self {
            RouterError::IllegalTarget => ILLEGAL_TARGET.as_bytes().to_vec(),
            RouterError::NotFound => NOT_FOUND.as_bytes().to_vec(),
            RouterError::TooLarge(limit) => short_error(
                413,
                "Payload Too Large",
                &format!("Request body exceeds {limit} bytes"),
            ),
            RouterError::AnswerTooLarge(limit) => short_error(
                502,
                "Bad Gateway",
                &format!("Upstream answer exceeds {limit} bytes"),
            ),
            RouterError::Busy(why) => short_error(503, "Busy", &why.what()),
        }
    }
}

/// `{"code":C,"message":N,"error":{"code":C,"name":N,"what":W,"details":[]}}`.
fn short_error(code: u16, name: &str, what: &str) -> Vec<u8> {
    let name = json_string(name);
    format!(
        r#"{{"code":{code},"message":{name},"error":{{"code":{code},"name":{name},"what":{},"details":[]}}}}"#,
        json_string(what)
    )
    .into_bytes()
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("a string always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(err: RouterError) -> String {
        String::from_utf8(err.body()).unwrap()
    }

    #[test]
    fn router_bodies_are_exact() {
        assert_eq!(RouterError::IllegalTarget.status(), 400);
        assert_eq!(
            text(RouterError::IllegalTarget),
            r#"{"code":400,"message":"Illegal request-target","error":{"code":0,"name":"","what":"","details":[]}}"#
        );
        assert_eq!(RouterError::NotFound.status(), 404);
        assert_eq!(
            text(RouterError::NotFound),
            r#"{"code":404,"message":"Not Found","error":{"code":0,"name":"exception","what":"unspecified","details":[{"message":"Unknown Endpoint","file":"beast_http_session.hpp","line_number":186,"method":"handle_request"}]}}"#
        );
        assert_eq!(RouterError::TooLarge(2_097_152).status(), 413);
        assert_eq!(
            text(RouterError::TooLarge(2_097_152)),
            r#"{"code":413,"message":"Payload Too Large","error":{"code":413,"name":"Payload Too Large","what":"Request body exceeds 2097152 bytes","details":[]}}"#
        );
        assert_eq!(RouterError::AnswerTooLarge(1_048_576).status(), 502);
        assert_eq!(
            text(RouterError::AnswerTooLarge(1_048_576)),
            r#"{"code":502,"message":"Bad Gateway","error":{"code":502,"name":"Bad Gateway","what":"Upstream answer exceeds 1048576 bytes","details":[]}}"#
        );
        assert_eq!(OPTIONS_BODY, b"{}");
    }

    #[test]
    fn busy_bodies_carry_the_reason() {
        let cases = [
            (
                Busy::InFlight(5),
                r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"Too many requests in flight: 5","details":[]}}"#,
            ),
            (
                Busy::NoUpstream,
                r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"No chain API upstream available","details":[]}}"#,
            ),
            (
                Busy::NoLiveUpstream,
                r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"No live chain API upstream available","details":[]}}"#,
            ),
            (
                Busy::LostAfterSend,
                r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"Upstream connection lost after the request was sent","details":[]}}"#,
            ),
        ];
        for (why, want) in cases {
            assert_eq!(RouterError::Busy(why).status(), 503);
            assert_eq!(text(RouterError::Busy(why)), want);
            // Valid JSON, in nodeos's shape.
            let parsed: serde_json::Value = serde_json::from_str(want).unwrap();
            assert_eq!(parsed["error"]["what"], why.what());
        }
    }
}
