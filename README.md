# codex-bridge

`codex-bridge` is an asynchronous Rust client for owning and operating one `codex app-server` child process.

This README is written primarily for LLMs and code-generation agents that need to use the crate correctly. Prefer explicit rules in this document over assumptions inferred from API names. For exact type signatures and field-level details, inspect the public rustdoc and source before generating code.

## Purpose

Use this crate when an application needs to:

- start and initialize one `codex app-server` child process;
- create app-server threads;
- run one active turn at a time;
- receive asynchronous app-server events;
- respond to app-server `ServerRequest`s;
- interrupt the active turn;
- explicitly shut down and reap the owned process.

A `Client` is a cloneable handle. All clones share the same app-server process, runtime state, request correlation state, and lifecycle. Cloning a `Client` does not create another app-server process.

The crate does not provide multi-process orchestration or concurrent active turns on one `Client`.

## Installation

This crate is currently consumed directly from its private GitHub repository. Pin the dependency to an explicit commit with `rev`:

```toml
[dependencies]
codex-bridge = { git = "ssh://git@github.com/niumlaque/codex-bridge.git", rev = "<commit-sha>" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The basic usage example below uses `#[tokio::main]`, so applications using that example need Tokio as a direct dependency. The listed features provide the macro and its default multi-threaded runtime.

The build environment must be able to authenticate to GitHub over SSH and read the private repository.

Cargo normally fetches Git dependencies with its built-in Git implementation. If SSH authentication does not work in the environment, Cargo can instead use the system `git` CLI. Add the following to `.cargo/config.toml` when needed:

```toml
[net]
git-fetch-with-cli = true
```

This setting is not universally required. Use it when the environment relies on Git CLI authentication or Cargo's built-in Git transport cannot authenticate successfully.

## Usage rules

Follow these rules when generating code that uses this crate.

1. Create configuration with `ClientConfig::new`.
2. Start the app-server with `Client::spawn`. A successful return means the initialize handshake has completed.
3. If asynchronous events matter, call `Client::subscribe` before starting work that may emit those events. Event subscription does not replay earlier events.
4. Create a thread with `Client::start_thread` before starting a turn.
5. Start a turn with `Client::run_turn`. Only one active turn may exist across all clones of the same `Client`.
6. Treat `Event::ServerRequest` as a request that requires caller-side handling. The crate does not choose approval decisions or user-input answers for the caller.
7. Use the response type that matches the received `ServerRequestKind`. Do not fabricate raw responses for typed approval or user-input requests.
8. Use `TurnOutcome::final_agent_message()` for the authoritative final agent text. Do not reconstruct the final answer from `AgentMessageDelta` events.
9. Call `Client::shutdown().await` when graceful, deterministic cleanup is required.
10. Do not rely on dropping the last `Client` handle for graceful shutdown. The last-handle Drop path stops the writer and requests immediate process-group termination.
11. A runtime failure does not, by itself, actively stop the writer or kill the app-server process merely because the client entered `Failed`.
12. Do not infer retry, reconnect, recovery, or continued usability after `Failed`. `Failed` is not `Ready`.

## Lifecycle

Keep the following cases distinct. They are not interchangeable.

| Situation | Meaning for the caller | Process behavior |
| --- | --- | --- |
| `Client::spawn` succeeds | Initialization is complete and the returned client is ready for normal requests. | The owned app-server process is running. |
| `Client::spawn` fails | No usable `Client` is returned. | Spawn-time cleanup attempts to reclaim the partially initialized process and background tasks. |
| Runtime failure | The primary failure is latched and the client enters `ClientState::Failed`. | Recording the failure does not itself send writer-stop or `Kill`. The process may nevertheless already have exited, or the transport may already be unusable, depending on the underlying failure. |
| `Client::shutdown().await` | The caller explicitly requests bounded cleanup. | Active work is cancelled, protocol cleanup is attempted, stdin is closed, the child is reaped, and forced termination may be used when required by shutdown cleanup. |
| Last `Client` handle is dropped | No public handle remains and async shutdown cannot be awaited. | The writer is stopped and the process supervisor is asked to kill the process group. |

Do not translate `ClientState::Failed` into "the process was killed". These are different facts.

Do not translate "the process was not killed by the failure path" into "the client recovered" or "the transport is still usable". This crate does not make that guarantee.

## Canonical call order

For the common request/response path, use this order:

1. `ClientConfig::new(...)`
2. optional configuration overrides
3. `Client::spawn(config).await`
4. `client.subscribe()` if events must be observed
5. `client.start_thread(...)`
6. `client.run_turn(...)`
7. handle the returned terminal `TurnOutcome`
8. `client.shutdown().await`

If the selected app-server policy or tools can produce `ServerRequest`s while a turn is running, the application must be able to handle those requests while `run_turn` is still pending. Subscribing to events before starting the turn is the normal way to receive them; `pending_server_requests()` provides a current snapshot when the event stream has lagged.

## Basic usage

The following example shows the minimal thread/turn lifecycle. It intentionally does not define an approval policy or a generic `ServerRequest` policy; those decisions belong to the application.

```rust
use codex_bridge::{Client, ClientConfig, ThreadStartParams, TurnStartParams, UserInput};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("/path/to/CODEX_HOME", "/path/to/workspace");
    let client = Client::spawn(config).await?;

    let thread = client.start_thread(ThreadStartParams::default()).await?;

    let outcome = client
        .run_turn(TurnStartParams::new(
            thread.thread_id,
            vec![UserInput::Text {
                text: "Reply with hello.".to_owned(),
                text_elements: Vec::new(),
            }],
        ))
        .await?;

    if let Some(message) = outcome.final_agent_message() {
        println!("{message}");
    }

    client.shutdown().await?;
    Ok(())
}
```

If a turn can produce `ServerRequest`s, arrange for the caller to receive and handle them while `run_turn` is pending. Event subscription is the normal delivery path.

## Configuration

`ClientConfig::new(codex_home, working_directory)` creates the default client configuration.

The default configuration:

- runs the `codex` executable;
- passes the supplied `codex_home` as `CODEX_HOME`;
- uses the supplied working directory;
- inherits the parent process environment;
- applies built-in timeout, queue-capacity, and message-size defaults.

`Client::spawn` validates configuration before starting the child process. In particular, `codex_home` and `working_directory` must already exist, timeout and capacity values must be non-zero, retained-memory limits must remain valid, and the generic environment map must not override `CODEX_HOME`.

## Threads and turns

### Threads

Create a thread with:

```rust
let thread = client.start_thread(ThreadStartParams::default()).await?;
```

`ThreadStartParams` contains optional overrides. A `None` field is omitted from the request so the app-server configuration/default remains in effect.

`start_thread` returns `ThreadStartOutcome`, including the new `thread_id` and the effective initial settings returned by the app-server.

### Turns

Create a turn with `TurnStartParams::new(thread_id, input)`.

Only the thread ID and input are required by that constructor. Other fields are optional per-turn overrides.

`Client::run_turn` waits for the terminal `turn/completed` notification and returns a `TurnOutcome`. The runtime handles the case where the terminal notification arrives before the `turn/start` response.

Only one active turn is allowed per `Client`, including all clones that share the same runtime.

For final response text, use:

```rust
let text = outcome.final_agent_message();
```

`AgentMessageDelta` is useful for incremental display, but it is not the authoritative final-answer source. A subscriber can lag or miss deltas. `TurnOutcome` contains the terminal turn snapshot and `final_agent_message()` extracts the final agent message from that snapshot.

### Interrupting a turn

Use `Client::interrupt_turn(thread_id, turn_id)` only for the currently active turn. The client validates the supplied IDs against the local active turn before sending the interrupt request.

An interrupt request does not replace the `run_turn` waiter. The terminal outcome still returns through the corresponding `run_turn` call.

## Events

Create an independent event receiver with:

```rust
let mut events = client.subscribe();
```

Important event semantics:

- subscription is not retrospective; events emitted before `subscribe()` are not replayed;
- each call to `subscribe()` creates a new broadcast receiver;
- a slow receiver can get `tokio::sync::broadcast::error::RecvError::Lagged`;
- `Event` is non-exhaustive, so generated `match` statements must include a fallback arm for future variants;
- `Event::AgentMessageDelta` is for incremental output, not authoritative final output;
- `Event::ServerRequest` requires caller-side handling;
- `Event::Failure` reports the primary client failure;
- `Event::ProcessExited` reports that the child exited outside the normal shutdown sequence.

When an event receiver reports `Lagged`, reconcile application state using current snapshots such as `Client::state()` and `Client::pending_server_requests()` rather than assuming the missed events can be replayed.

## Server requests

A `ServerRequest` is an app-server JSON-RPC request waiting for a caller response. Do not treat it as a notification.

The caller must decide what to do with the request. This crate validates response shape and request/response correlation, but it does not decide whether an operation should be approved, declined, cancelled, or answered with user input.

Use `ServerRequest.kind` to select the response path.

| `ServerRequestKind` | Response |
| --- | --- |
| `CommandExecutionApproval` | `ServerRequestResult::command_execution_approval(...)` |
| `FileChangeApproval` | `ServerRequestResult::file_change_approval(...)` |
| `PermissionApproval` | `ServerRequestResult::permission_approval(...)` |
| `UserInput` | `ServerRequestResult::user_input(...)` |
| `Known` | raw result only when the application knows the required result shape |
| `Unsupported` | raw result only when the application explicitly chooses to handle the unsupported request |

For typed request kinds, call `Client::respond_server_request` with the matching typed `ServerRequestResult`. The client rejects a result whose typed kind does not match the pending request.

Use `Client::reject_server_request` when the application intentionally returns a JSON-RPC error response.

For a raw/untyped request that the application explicitly knows how to answer, `Client::claim_server_request` obtains a one-time local claim before the response is sent. The returned `ClaimedServerRequest` is consumed by `respond(...)`. Typed approval and user-input requests cannot be claimed through this raw path.

### Recovering from event lag

If a `ServerRequest` event may have been missed because the broadcast receiver lagged, call:

```rust
let pending = client.pending_server_requests();
```

This returns a snapshot of requests currently waiting for caller handling. Reconcile the application's request UI or decision state from this snapshot.

## Failure, shutdown, and Drop

These paths have deliberately different semantics.

### Runtime failure

A primary runtime failure transitions the client to `ClientState::Failed` and releases affected waiters with an error.

Entering `Failed` does **not** itself mean "kill the app-server". The failure path does not actively send writer-stop or a process `Kill` merely because the failure was recorded.

This is not a recovery mechanism. Do not assume that:

- the underlying transport still works;
- the app-server is still alive;
- new normal requests are accepted;
- retrying the same request is safe;
- reconnect or recovery happens automatically.

The underlying failure determines what is still physically available. The crate only guarantees that recording the client failure is not itself an instruction to terminate the process.

If deterministic resource cleanup is still required, use the explicit shutdown API while the application still owns a `Client` handle.

### Explicit shutdown

Use:

```rust
let report = client.shutdown().await?;
```

`shutdown` owns the asynchronous cleanup sequence. It cancels active waiters, performs best-effort protocol cleanup, closes stdin, asks the process supervisor to reclaim the child process, and joins background tasks within the configured shutdown deadline. Forced process termination may be used as part of that cleanup when necessary.

Multiple clones may call `shutdown`; the shutdown sequence is serialized and its completed result is reused.

### Last-handle Drop

Dropping the last public `Client` handle is not equivalent to `shutdown().await`.

Because Rust `Drop` cannot await the asynchronous shutdown sequence, the last-handle Drop path stops the writer and requests immediate process-group termination so the owned process is not orphaned.

Therefore:

- use `shutdown().await` for intentional graceful cleanup;
- treat last-handle Drop as a fail-safe ownership cleanup path;
- do not generate code that intentionally uses `drop(client)` as the normal shutdown API.

## Cloning `Client`

`Client::clone()` creates another handle to the same runtime. It does not create a new process, a new lifecycle, or a separate active-turn slot.

Consequences:

- all clones share one app-server process;
- all clones share the one-active-turn restriction;
- dropping one clone does not terminate the process while other public handles still exist;
- the last public handle being dropped triggers the Drop cleanup described above.

If independent app-server processes are required, spawn independent `Client` instances rather than cloning one `Client`.

## Do not assume

When generating integration code, do **not** assume any of the following unless a future crate version explicitly documents it:

- multiple active turns are supported on one `Client`;
- a `Client` clone owns an independent process;
- event subscription replays past events;
- `AgentMessageDelta` contains a complete authoritative final response;
- the crate chooses approval or user-input decisions for `ServerRequest`s;
- raw response payloads may be used for typed approval/user-input requests;
- entering `ClientState::Failed` kills the app-server process;
- entering `ClientState::Failed` keeps the client operational;
- retry, reconnect, or recovery occurs automatically;
- dropping the last `Client` performs graceful asynchronous shutdown.

When uncertain, inspect the public rustdoc/source rather than inventing behavior.

## API documentation

Use rustdoc for exact public signatures, fields, enums, and error types:

```text
cargo doc --open
```

Useful entry points are:

- `Client`
- `ClientConfig`
- `ClientState`
- `Event`
- `ServerRequest`
- `ServerRequestKind`
- `ServerRequestResult`
- `ThreadStartParams`
- `ThreadStartOutcome`
- `TurnStartParams`
- `TurnOutcome`

The README describes how these pieces are intended to be combined. The public API documentation and checked-in source define their exact current shape.
