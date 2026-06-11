# SIRI-1229: CSRF Validation by Default Migration Plan

This document applies the generic CSRF default-validation migration guide to S3 Ninja.

## Scope

S3 Ninja currently uses `sirius-web` `102.0.0`. The framework change starts with `sirius-web` `104.0.0`, so the implementation must first bump the dependency and then adjust the code which becomes affected by default CSRF validation.

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
| `/.api/generate-presigned-post` | Added | Internal UI endpoint called by `fetch(..., { method: 'POST' })` in `templates/presigned-post.html.pasta`; add `CSRFToken` to the form-encoded body and restrict the route to `POST`. |
| `/ui/:1?upload` | Added | Object upload is already called through `t:fileUpload`, which should supply the token after the framework upgrade; make the upload branch explicitly require `POST`. |
| `/.api/generate-session-token` | Forced POST | Purely internal UI helper (`@InternalService`), only called from `templates/index.html.pasta` and `templates/presigned-post.html.pasta` via `GET` fetch. Adding CSRF is safe because there are no external/programmatic callers. Change both calls to CSRF-protected `POST` and restrict the route to `POST`. |
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

- `generateSessionToken` and `generatePresignedPost` are single-purpose routes, so restrict them at the annotation level: `@Routed(value = "...", methods = HttpMethod.POST)`. A `GET` then yields a framework 405 and a `POST` is CSRF-validated by the dispatcher automatically.
- `bucket(WebContext, String)` and `object(WebContext, String, List<String>)` are mixed GET/POST routes (they serve both listing/download via GET and mutations via query parameter), so they **cannot** use a method-restricted annotation. Keep them accepting all methods and add a small local helper that rejects mutation branches when the request method is not POST:
  - `bucket`: keep GET listing, require POST for the `create`, `make-public`, `make-private`, `delete`, and `upload` branches.
  - `object`: keep GET download, require POST for the `delete` branch.

The dispatcher validates the CSRF token for every non-exempt POST before the controller method runs, so the local helper only has to enforce the HTTP method (return `HttpResponseStatus.METHOD_NOT_ALLOWED` / 405 for non-POST mutation branches); it must not perform the CSRF check itself. If the framework offers a convenient 405 response helper, use it. Do not add `skipCsrfValidation` for these UI routes.

## Template and JavaScript Plan

Update `templates/index.html.pasta`:

- Replace the create-bucket JavaScript `location.href = '/ui/' + encodeURIComponent(name) + '?create'` with a POST submission that includes `CSRFToken`. Prefer building and submitting a hidden `<form method="post" action="/ui/<name>?create">` with a hidden `CSRFToken` field (`@part(CSRFHelper.class).getCSRFToken()`, valid here because `index.html.pasta` is Tagliatelle-rendered) and calling `form.submit()`. A real form submit preserves the existing POST → 302 redirect → GET flow; a `fetch` POST would require handling the redirect manually.
- Change `generateSessionToken()` from GET `fetch('/.api/generate-session-token')` to POST and include `CSRFToken`.
- Convert the bucket ACL dropdown actions from plain `t:dropdownItem` GET links to CSRF-protected POST actions. Prefer Tycho's `sendPostRequest="true"` if it emits the token for the installed framework version.
- Keep `t:dropdownDeleteItem` for bucket deletion, but verify that it emits a POST form and not a GET link after the framework upgrade.

Update `templates/bucket.html.pasta`:

- Convert the large `Make public` / `Make private` buttons from plain GET links to CSRF-protected POST actions.
- Keep `t:fileUpload`, but treat its behavior on `sirius-web >= 104.0.0` as a hard prerequisite to verify, since this is one of the two genuine breakages: confirm (a) which HTTP method the generated upload request uses (so the forced-POST branch matches it) and (b) that it appends `CSRFToken` automatically. If the tag does not emit the token, append it manually to `uploadUrl` (e.g. `&CSRFToken=@escapeJS(part(CSRFHelper.class).getCSRFToken())`) per the generic guide §4.5.
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

1. Bump `sirius.web` from `102.0.0` to at least `104.0.0`. Bumping `sirius.web` alone is likely insufficient: it almost certainly requires a compatible `sirius.kernel` (currently `51.0.0`) and possibly a newer `sirius-parent` (currently `14.3.3`). Find a consistent set of versions, not just the web bump in isolation.
2. Build once to reveal any framework API changes not visible in the source audit.
3. Apply the controller and template changes above.
4. Rebuild and adjust imports or route annotations as needed.

## Verification Plan

A Kotlin/JUnit5 test suite already exists under `src/test/kotlin` (run via `SiriusExtension`, with an in-process HTTP server on port 9999), so the test harness needed for CSRF tests is available today — no need to wait for the dependency bump. Relevant existing tests:

- `PresignedPostMultipartTest` performs a real `POST http://localhost:9999/<bucket>` (multipart) against `S3Dispatcher` and asserts `204`. This is exactly the dispatcher path that must stay CSRF-free, so it is valuable existing regression coverage — re-run it after the bump to confirm `S3Dispatcher` is unaffected by default validation.
- `SessionTokenTest` calls `s3Dispatcher.generateSessionToken()` directly (not the HTTP route), so it is unaffected by the controller changes and should not be modified.

Add focused tests for the migrated behavior:

- Missing `CSRFToken` on `POST /.api/generate-session-token` returns 403.
- Valid CSRF POST to `/.api/generate-session-token` returns a token.
- Missing `CSRFToken` on `POST /.api/generate-presigned-post` returns 403.
- Valid CSRF POST to `/.api/generate-presigned-post` returns fields.
- GET mutation attempts such as `/ui/<bucket>?delete`, `/ui/<bucket>?make-public`, and `/ui/<bucket>/<object>?delete` are rejected (405).
- Valid CSRF POST mutation requests still work.
- S3 protocol requests through `S3Dispatcher` still work without `CSRFToken` when authenticated by their existing AWS/session/policy mechanisms (covered by `PresignedPostMultipartTest`).

Manual verification should cover:

- Creating a bucket from the UI.
- Making a bucket public/private from the index and bucket detail pages.
- Deleting a bucket.
- Uploading an object through the UI file upload.
- Deleting an object.
- Generating a session token.
- Generating and testing a presigned POST upload.
