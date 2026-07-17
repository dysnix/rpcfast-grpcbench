# RPCFast gRPCBench

`rpcfast-grpcbench` compares Solana transaction stream delivery speed across:

- Yellowstone gRPC
- Jito ShredStream `SubscribeEntries`
- Aperture txstream

It records local receive timestamps for transaction signatures, compares only signatures seen by at least two endpoints, and writes a self-contained HTML report. It does not call RPC or use block time.

## Usage

```bash
cp grpcbench.example.toml grpcbench.toml
export YELLOWSTONE_X_TOKEN=...
export JITO_SHREDSTREAM_X_TOKEN=...
export APERTURE_X_TOKEN=...
cargo run --release -- --config grpcbench.toml --duration 60s --output report.html
```

Open `report.html` in a browser.

## TOML Config

```toml
duration = "60s"
warmup = "5s"
no_tx_timeout = "30s"
buffer_size = 4194304

[yellowstone.rpcfast]
url = "https://solana-yellowstone-grpc.rpcfast.com:443"
x_token_env = "YELLOWSTONE_X_TOKEN"

[jito_shredstream.jito]
url = "https://jito-shredstream.example.com:443"
x_token_env = "JITO_SHREDSTREAM_X_TOKEN"

[aperture_txstream.rpcfast]
url = "https://aperture-grpc.rpcfast.com:443"
x_token_env = "APERTURE_X_TOKEN"
signatures_only = true
include_simulation = false
```

Endpoint table names become report aliases. Tokens can be supplied inline with `x_token`, but `x_token_env` is recommended.
For Aperture endpoints, `include_simulation` requests transaction simulation results and defaults to `false`.

## Report Semantics

- `Observed signatures`: unique signatures seen by at least one endpoint during the measured window.
- `Race eligible`: signatures seen by at least two endpoints.
- `Wins`: times an endpoint was first among endpoints that saw the same signature.
- `Lag`: local receive-time delay behind the fastest endpoint for the same signature.
- `Pairwise winners`: one row per endpoint pair. `Wins` counts how often the displayed faster endpoint arrived first within shared signatures.
- `Pairwise lead`: signed receive-time delta for the displayed faster endpoint. Positive values mean it led; negative values mean it was behind for that sample.
- `Pairwise lag`: positive receive-time lag for samples where the displayed faster endpoint was behind. `n/a` means it never lost that pair.

All endpoints are measured by timestamp of receive event.

## License

This project and its complete source code are licensed under the [Apache License 2.0](LICENSE).
