# SIRI-1229: CSRF Validation by Default Migration Plan

This document applies the generic CSRF default-validation migration guide to S3 Ninja and records the implemented migration.

## Scope

S3 Ninja used `sirius-web` `102.0.0`. The framework change starts with `sirius-web` `104.0.0`; the migration updates the project to `sirius-web` `104.1.0`.

The routed web UI is implemented in `ninja.NinjaController`. The S3 protocol endpoints are implemented by `ninja.S3Dispatcher` as a custom `WebDispatcher`, not by `@Routed` controller methods. They must keep their AWS signature, session token, and presigned policy authentication model and must not be converted to browser-session CSRF validation.

The full route audit confirms that `NinjaController` is the only `@Routed` controller (six routes) and `S3Dispatcher` is the only `WebDispatcher`. There are no other controllers to migrate.

## What actually breaks vs. what we harden

Default CSRF validation only affects routes that are *already reached* via `POST`/`PUT`/`PATCH`/`DELETE`. `GET` is never validated. It is therefore useful to separate the two kinds of change:

- **Genuine post-upgrade breakages** (these return 403 after the bump unless fixed):
  - `/.api/generate-presigned-post` — already called via `fetch(..., { method: 'POST' })` in `templates/presigned-post.html.pasta`. Must receive a `CSRFToken`.
  - `/ui/:1?upload` — driven by `t:fileUpload`, whose generated request uses a CSRF-validated method. Depends on the upload tag emitting the token after the upgrade (see the verification note in the template plan).
- **Proactive hardening** (currently `GET`, so they would *not* break, but are destructive/state-changing and should not be reachable by `GET`): `create`, `make-public`, `make-private`, bucket `delete`, object `delete`, and `/.api/generate-session-token`. These are converted to `POST` (Category E) while we are touching the code.

`/.api/generate-session-token` is a purely internal UI helper (its only callers are the two `.pasta` pages), so requiring a browser CSRF token on it is safe and does not break any external/programmatic consumer.

## Route Audit

| Route | Status | Notes |
|-------|--------|-------|
| `/.api/generate-presigned-post` | Added | Internal UI endpoint called by `fetch(..., { method: 'POST' })` in `templates/presigned-post.html.pasta`; now receives `CSRFToken` in the form-encoded body and is restricted to `POST`. |
| `/ui/:1?upload` | Added | Object upload is called through `t:fileUpload`; `sirius-web` `104.1.0` appends `CSRFToken` automatically. The controller branch now requires `POST`. |
| `/.api/generate-session-token` | Forced POST | Purely internal UI helper (`@InternalService`), only called from `templates/index.html.pasta` and `templates/presigned-post.html.pasta`; now called as CSRF-protected `POST` and restricted to `POST`. |
| `/ui/:1?create` | Forced POST | Bucket creation now uses a dynamically submitted `POST` form with `CSRFToken`; the controller rejects non-POST requests. |
| `/ui/:1?delete` | Forced POST | Bucket deletion remains a `t:dropdownDeleteItem`, which renders a CSRF-protected POST in `sirius-web` `104.1.0`; the controller rejects non-POST requests. |
| `/ui/:1?make-private` | Forced POST | ACL mutation now uses `sendPostRequest="true"` dropdown actions or explicit POST forms with `CSRFToken`; the controller rejects non-POST requests. |
| `/ui/:1?make-public` | Forced POST | ACL mutation now uses `sendPostRequest="true"` dropdown actions or explicit POST forms with `CSRFToken`; the controller rejects non-POST requests. |
| `/ui/:1/**?delete` | Forced POST | Object deletion remains a `t:dropdownDeleteItem`, which renders a CSRF-protected POST in `sirius-web` `104.1.0`; the controller rejects non-POST requests. |
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

Route annotations now use `sirius.web.controller.HttpMethod`; request method checks still compare against Netty's request method enum.

## Controller Implementation

`NinjaController` now ensures that state-changing controller behavior is only reachable via POST:

- `generateSessionToken` and `generatePresignedPost` are single-purpose routes, so restrict them at the annotation level: `@Routed(value = "...", methods = HttpMethod.POST)`. A `GET` then yields a framework 405 and a `POST` is CSRF-validated by the dispatcher automatically.
- `bucket(WebContext, String)` and `object(WebContext, String, List<String>)` are mixed GET/POST routes (they serve both listing/download via GET and mutations via query parameter), so they **cannot** use a method-restricted annotation. They keep accepting all methods and use a small local helper that rejects mutation branches when the request method is not POST:
  - `bucket`: keep GET listing, require POST for the `create`, `make-public`, `make-private`, `delete`, and `upload` branches.
  - `object`: keep GET download, require POST for the `delete` branch.

The dispatcher validates the CSRF token for every non-exempt POST before the controller method runs, so the local helper only enforces the HTTP method. It does not manually check CSRF. For non-POST mutation branches it throws a `HandledException` with `Controller.HTTP_STATUS = 405` and `Controller.HTTP_HEADER_ALLOW = POST`, so Sirius formats the error through the normal controller error pipeline. No `skipCsrfValidation` was added for these UI routes.

## Template and JavaScript Implementation

`templates/index.html.pasta`:

- The create-bucket JavaScript now builds and submits a hidden `<form method="post" action="/ui/<name>?create">` with a hidden `CSRFToken` field.
- `generateSessionToken()` now uses `POST` and sends `CSRFToken`.
- Bucket ACL dropdown actions now use `t:dropdownItem sendPostRequest="true"`, which emits a POST form with `CSRFToken` in `sirius-web` `104.1.0`.
- Bucket deletion remains `t:dropdownDeleteItem`; in `sirius-web` `104.1.0` this emits a CSRF-protected POST when no confirmation dialog is used.

`templates/bucket.html.pasta`:

- The large `Make public` / `Make private` buttons are now explicit `POST` forms with hidden `CSRFToken` fields.
- `t:fileUpload` remains in use; `sirius-web` `104.1.0` appends `filename` and `CSRFToken` to its generated upload URL.
- Object deletion remains `t:dropdownDeleteItem`, which emits a CSRF-protected POST when no confirmation dialog is used.

`templates/presigned-post.html.pasta`:

- `generateAndFillToken()` now uses `POST` and sends `CSRFToken`.
- `generatePresignedPost()` now adds `CSRFToken` to the `URLSearchParams` sent to `/.api/generate-presigned-post`.
- S3 Ninja CSRF tokens were intentionally not added to the generated presigned upload request itself. That request targets the S3 protocol endpoint and is authenticated by the generated AWS policy/signature fields.

All hand-written `<form method="post">` elements introduced by this migration include a hidden `CSRFToken` field.

## S3 Dispatcher Notes

`S3Dispatcher` handles non-UI paths such as:

- `GET /`
- `GET|HEAD|PUT|DELETE|POST /bucket`
- `GET|HEAD|PUT|DELETE|POST /bucket/key`
- multipart upload routes like `POST /bucket/key?uploads`, `PUT /bucket/key?uploadId=X&partNumber=Y`, `POST /bucket/key?uploadId=X`, and `DELETE /bucket/key?uploadId=X`
- policy-based presigned POST uploads

These are API protocol endpoints called by AWS SDKs, CLI tools, curl, and the presigned POST workbench. They are not `@Routed` controller methods and already use AWS signatures, generated session tokens, or policy signatures. The migration should not require browser CSRF tokens for them.

## Dependency and Compile Result

1. `sirius.web` was bumped from `102.0.0` to `104.1.0`.
2. `mvn -q -DskipTests compile` succeeds with the existing `sirius.kernel` `51.0.0` and `sirius-parent` `14.3.3`.
3. The only framework API adjustment required in application code was using `sirius.web.controller.HttpMethod` for `@Routed(methods = ...)`.
4. `PresignedPostRequest#from` was adjusted to treat `sessionToken` as optional, matching the UI and allowing the no-session-token presigned POST flow.

## Verification Plan

A Kotlin/JUnit5 test suite already exists under `src/test/kotlin` (run via `SiriusExtension`, with an in-process HTTP server on port 9999), so the test harness needed for CSRF tests is available today — no need to wait for the dependency bump. Relevant existing tests:

- `PresignedPostMultipartTest` performs a real `POST http://localhost:9999/<bucket>` (multipart) against `S3Dispatcher` and asserts `204`. This is exactly the dispatcher path that must stay CSRF-free, so it is valuable existing regression coverage — re-run it after the bump to confirm `S3Dispatcher` is unaffected by default validation.
- `SessionTokenTest` calls `s3Dispatcher.generateSessionToken()` directly (not the HTTP route), so it is unaffected by the controller changes and should not be modified.

Focused tests were added in `CsrfProtectionTest`:

- Missing `CSRFToken` on `POST /.api/generate-session-token` returns 403.
- Valid CSRF POST to `/.api/generate-session-token` returns a token.
- Missing `CSRFToken` on `POST /.api/generate-presigned-post` returns 403.
- Valid CSRF POST to `/.api/generate-presigned-post` returns fields.
- GET mutation attempts for `/.api/generate-session-token` and `/ui/<bucket>?create` are rejected (405).
- Valid CSRF POST bucket create/delete requests still work.
- S3 protocol requests through `S3Dispatcher` still work without `CSRFToken` when authenticated by their existing AWS/session/policy mechanisms (covered by `PresignedPostMultipartTest`).

Executed verification:

- `mvn -q -DskipTests compile`
- `mvn -q -Dtest=CsrfProtectionTest test`

Manual verification should cover:

- Creating a bucket from the UI.
- Making a bucket public/private from the index and bucket detail pages.
- Deleting a bucket.
- Uploading an object through the UI file upload.
- Deleting an object.
- Generating a session token.
- Generating and testing a presigned POST upload.
