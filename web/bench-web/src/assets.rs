//! Serves `static/` (the demo page — `index.html`, `app.js`, `style.css`, `js/*`, `fonts/*`)
//! embedded into the binary via `rust-embed`, so the running server never reads from disk and
//! the binary is self-contained. Wired as `app_router`'s fallback in `lib.rs`: any request that
//! doesn't match `/ws` or an `/api/*` route falls through here.

use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Assets;

/// `GET /` and any path not otherwise routed. `/` (and any path with no embedded match, so a
/// client-side route or a stray trailing slash doesn't 404) serves `index.html`; everything else
/// is looked up by its embedded path with `/` stripped, so `/app.js` -> `app.js`, `/fonts/x.woff2`
/// -> `fonts/x.woff2`. The MIME type comes from `mime_guess` off the file's extension — `.woff2`
/// and `.css` are not in axum's own small built-in table, so getting this wrong would silently
/// break the font and the stylesheet in a browser that respects `Content-Type` strictly.
pub async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], Body::from(file.data)).into_response()
        }
        // Fall back to index.html for anything unrecognised rather than a bare 404 — the demo is
        // a single page, and this keeps a stray refresh on some future client-side route working.
        None => match Assets::get("index.html") {
            Some(file) => {
                ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], Body::from(file.data)).into_response()
            }
            None => (StatusCode::NOT_FOUND, "not found").into_response(),
        },
    }
}
