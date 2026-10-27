use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use cuprate_rpc_interface::RpcHandlerDummy;
use tower::ServiceExt;

use super::*;

mod real_handler;

#[tokio::test]
async fn peer_details_are_admin_only_on_the_production_router() {
    for restricted in [false, true] {
        let router = init_rpc_router(
            RpcHandlerDummy { restricted },
            1024 * 1024,
            Duration::from_secs(5),
        );
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/json_rpc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"get_connections"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                .unwrap();
        if restricted {
            assert_eq!(json["error"]["code"], -32601);
            assert!(json.get("result").is_none());
        } else {
            assert!(json["result"]["connections"].is_array());
        }
        for public_only in [true, false] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/get_peer_list")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"public_only":{public_only},"include_blocked":true}}"#
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if restricted {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::OK
                }
            );
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            if restricted {
                assert!(body.is_empty());
            } else {
                let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert!(json["white_list"].is_array() && json["gray_list"].is_array());
            }
        }
    }
}
