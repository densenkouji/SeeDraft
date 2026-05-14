use axum::{
    body::Bytes,
    http::header,
    response::{Html, IntoResponse, Response},
};

const APP_ICON_ICO: &[u8] = include_bytes!("../res/icon.ico");

pub(crate) async fn index_handler() -> Html<&'static str> {
    Html(include_str!("ui/index.html"))
}

pub(crate) async fn hold_overlay_handler() -> Html<&'static str> {
    Html(include_str!("ui/hold_overlay.html"))
}

pub(crate) async fn app_icon_handler() -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/x-icon"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        Bytes::from_static(APP_ICON_ICO),
    )
        .into_response()
}
