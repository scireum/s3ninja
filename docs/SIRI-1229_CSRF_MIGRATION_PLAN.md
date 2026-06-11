# SIRI-1229: CSRF Validation by Default Migration Plan

This document applies the generic CSRF default-validation migration guide to S3 Ninja.

## Scope

S3 Ninja currently uses `sirius-web` `102.0.0`. The framework change starts with `sirius-web` `104.0.0`, so the implementation must first bump the dependency and then adjust the code which becomes affected by default CSRF validation.

The routed web UI is implemented in `ninja.NinjaController`. The S3 protocol endpoints are implemented by `ninja.S3Dispatcher` as a custom `WebDispatcher`, not by `@Routed` controller methods. They must keep their AWS signature, session token, and presigned policy authentication model and must not be converted to browser-session CSRF validation.

## Route Audit

| Route | Status | Notes |
|-------|--------|-------|
| `/.api/generate-presigned-post` | Added | Internal UI endpoint called by `fetch(..., { method: 'POST' })` in `templates/presigned-post.html.pasta`; add `CSRFToken` to the form-encoded body and restrict the route to `POST`. |
| `/ui/:1?upload` | Added | Object upload is already called through `t:fileUpload`, which should supply the token after the framework upgrade; make the upload branch explicitly require `POST`. |
| `/.api/generate-session-token` | Forced POST | This currently generates credentials through a `GET` fetch from `templates/index.html.pasta` and `templates/presigned-post.html.pasta`; change both calls to CSRF-protected `POST` and restrict the route to `POST`. |
| `/ui/:1?create` | Forced POST | Bucket creation is currently triggered by JavaScript navigation to a `GET` URL in `templates/index.html.pasta`; convert to a POST form/request with `CSRFToken`. |
| `/ui/:1?delete` | Forced POST | Bucket deletion is destructive. `t:dropdownDeleteItem` can provide a POST with CSRF, but the controller must reject GET for this branch. |
| `/ui/:1?make-private` | Forced POST | ACL mutation is currently reachable through GET links in `templates/index.html.pasta` and `templates/bucket.html.pasta`; convert all triggers to POST with CSRF and reject GET. |
| `/ui/:1?make-public` | Forced POST | ACL mutation is currently reachable through GET links in `templates/index.html.pasta` and `templates/bucket.html.pasta`; convert all triggers to POST with CSRF and reject GET. |
| `/ui/:1/**?delete` | Forced POST | Object deletion is destructive. `t:dropdownDeleteItem` can provide a POST with CSRF, but the controller must reject GET for this branch. |
| `/ui` |  | Pure UI page route. Query variants `license`, `api`, and `log` render read-only pages. |
| `/ui/:1` |  | Bucket listing and object search/pagination remain GET. Mutation query variants are listed separately above. |
| `/ui/:1/**` |  | Object download remains GET. The delete query variant is listed separately above. |
| `/ui/presigned-post` |  | Pure page route for the presigned POST workbench. |

## Mechanical API Changes

Current source does not use the removed CSRF APIs:

- No `WebContext#isSafePOST()`, `ensureSafePOST()`, or `isUnsafePOST()`.
- No `WebContext#hidePost()`.
- No `BasicController#enforceMethodPost(...)`.
- No `SaveHelper#disableSafePOST()`.
- No existing `skipCsrfValidation` or `isSkipCsrfValidation()` overrides.

The implementation should still import `io.netty.handler.codec.http.HttpMethod` in `NinjaController` when route methods are declared or request methods are checked.

## Controller Implementation Plan

Update `NinjaController` so that state-changing controller behavior is only reachable via POST:

- Restrict `generateSessionToken` to `POST`.
- Restrict `generatePresignedPost` to `POST`.
- For `bucket(WebContext, String)`, keep GET bucket listing behavior but require POST for the `create`, `make-public`, `make-private`, `delete`, and `upload` branches.
- For `object(WebContext, String, List<String>)`, keep GET download behavior but require POST for the `delete` branch.

The cleanest implementation is likely to keep the existing mixed routes and add a small local helper that rejects mutation branches when the request method is not POST. If the framework offers a convenient 405 response helper, use it; otherwise respond with `HttpResponseStatus.METHOD_NOT_ALLOWED`. Do not add `skipCsrfValidation` for these UI routes.

## Template and JavaScript Plan

Update `templates/index.html.pasta`:

- Replace the create-bucket JavaScript `location.href = '/ui/' + encodeURIComponent(name) + '?create'` with a POST submission that includes `CSRFToken`.
- Change `generateSessionToken()` from GET `fetch('/.api/generate-session-token')` to POST and include `CSRFToken`.
- Convert the bucket ACL dropdown actions from plain `t:dropdownItem` GET links to CSRF-protected POST actions. Prefer Tycho's `sendPostRequest="true"` if it emits the token for the installed framework version.
- Keep `t:dropdownDeleteItem` for bucket deletion, but verify that it emits a POST form and not a GET link after the framework upgrade.

Update `templates/bucket.html.pasta`:

- Convert the large `Make public` / `Make private` buttons from plain GET links to CSRF-protected POST actions.
- Keep `t:fileUpload`; verify that its generated upload request includes `CSRFToken` after the framework upgrade.
- Keep `t:dropdownDeleteItem` for object deletion, but verify that it emits a POST form and not a GET link after the framework upgrade.

Update `templates/presigned-post.html.pasta`:

- Change `generateAndFillToken()` from GET `fetch('/.api/generate-session-token')` to POST and include `CSRFToken`.
- Add `CSRFToken` to the `URLSearchParams` sent to `/.api/generate-presigned-post`.
- Do not add S3 Ninja CSRF tokens to the generated presigned upload request itself. That request targets the S3 protocol endpoint and is authenticated by the generated AWS policy/signature fields.

No hand-written `<form method="post">` without token was found. The create-bucket form currently has no `method` and is intercepted by JavaScript.

## S3 Dispatcher Notes

`S3Dispatcher` handles non-UI paths such as:

- `GET /`
- `GET|HEAD|PUT|DELETE|POST /bucket`
- `GET|HEAD|PUT|DELETE|POST /bucket/key`
- multipart upload routes like `POST /bucket/key?uploads`, `PUT /bucket/key?uploadId=X&partNumber=Y`, `POST /bucket/key?uploadId=X`, and `DELETE /bucket/key?uploadId=X`
- policy-based presigned POST uploads

These are API protocol endpoints called by AWS SDKs, CLI tools, curl, and the presigned POST workbench. They are not `@Routed` controller methods and already use AWS signatures, generated session tokens, or policy signatures. The migration should not require browser CSRF tokens for them.

## Dependency and Compile Plan

1. Bump `sirius.web` from `102.0.0` to at least `104.0.0`.
2. Build once to reveal any framework API changes not visible in the source audit.
3. Apply the controller and template changes above.
4. Rebuild and adjust imports or route annotations as needed.

## Verification Plan

Automated coverage is currently absent under `src/test`. Add focused tests if the framework test helpers are available after the dependency bump:

- Missing `CSRFToken` on `POST /.api/generate-session-token` returns 403.
- Valid CSRF POST to `/.api/generate-session-token` returns a token.
- Missing `CSRFToken` on `POST /.api/generate-presigned-post` returns 403.
- Valid CSRF POST to `/.api/generate-presigned-post` returns fields.
- GET mutation attempts such as `/ui/<bucket>?delete`, `/ui/<bucket>?make-public`, and `/ui/<bucket>/<object>?delete` are rejected.
- Valid CSRF POST mutation requests still work.
- S3 protocol requests through `S3Dispatcher` still work without `CSRFToken` when authenticated by their existing AWS/session/policy mechanisms.

Manual verification should cover:

- Creating a bucket from the UI.
- Making a bucket public/private from the index and bucket detail pages.
- Deleting a bucket.
- Uploading an object through the UI file upload.
- Deleting an object.
- Generating a session token.
- Generating and testing a presigned POST upload.
