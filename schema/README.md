# Codex App Server JSON Schemas

This directory contains JSON Schema definitions for the OpenAI Codex App Server protocol.

## Source

- Project: [OpenAI Codex](https://github.com/openai/codex)
- Codex CLI version: `0.164.0`
- Protocol: Codex App Server (JSON-RPC)

The schemas were generated from the Codex App Server protocol and are maintained as a versioned snapshot.

The upstream project provides the following command to generate JSON Schema files:

`codex app-server generate-json-schema --out DIR`

The generated schemas correspond to the Codex CLI version used to run the command.

## Usage

`src/protocol/schema.rs` reads known method names from `ServerRequest.json` and `ServerNotification.json`.
It uses `PermissionsRequestApprovalParams.json` to validate the `permissions` profile that can be reused in approval responses, including the schema definitions it references.

For incoming messages, the library validates the JSON-RPC envelope and the fields it uses or exposes through typed APIs. It also checks request ID and turn state consistency.
The library preserves raw JSON for generic and unknown notifications, server requests, and uninterpreted thread items.

## License

The schemas are derived from OpenAI Codex and are distributed under the Apache License, Version 2.0.

Copyright 2025 OpenAI.

See [LICENSE](LICENSE) for the Apache-2.0 license text.

The MIT license of `codex-bridge` does not replace the Apache-2.0 license applicable to these schemas.
