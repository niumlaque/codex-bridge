# Codex App Server JSON Schemas

This directory contains JSON Schema definitions for the OpenAI Codex App Server protocol.

## Source

- Project: [OpenAI Codex](https://github.com/openai/codex)
- Codex CLI version: `0.157.0`
- Protocol: Codex App Server (JSON-RPC)

The schemas were generated from the Codex App Server protocol and are maintained as a versioned snapshot.

The upstream project provides the following command to generate JSON Schema files:

`codex app-server generate-json-schema --out DIR`

The generated schemas correspond to the Codex CLI version used to run the command.

## Usage

`codex-bridge` includes these schemas to validate messages received from the Codex App Server.

In particular, `src/protocol/schema.rs` embeds the following files at compile time:

- `ServerRequest.json`
- `ServerNotification.json`

## License

The schemas are derived from OpenAI Codex and are distributed under the Apache License, Version 2.0.

Copyright 2025 OpenAI.

See [LICENSE](LICENSE) for the Apache-2.0 license text.

The MIT license of `codex-bridge` does not replace the Apache-2.0 license applicable to these schemas.
