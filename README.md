# circular-tickets

Find "circular" winning-ticket redemptions on the Livepeer Arbitrum subgraph:
events where the **sender** (gateway/broadcaster) address equals the
**recipient** (orchestrator/transcoder) address — i.e. an operator paying
fees to themselves.

The script aggregates totals across all gateways and breaks them down per
sender address.

## Prerequisites

- Python 3.9+
- A free API key from [The Graph Studio](https://thegraph.com/studio/apikeys/)

## Install

```bash
pip install -r requirements.txt
export THEGRAPH_API_KEY=<your-key>
```

## Run

```bash
python circular_tickets.py                   # all-time
python circular_tickets.py --since 30d       # last 30 days
python circular_tickets.py --since 2025-01-01
```

Optional flags:

| Flag             | Default                   | Purpose                                                           |
| ---------------- | ------------------------- | ----------------------------------------------------------------- |
| `--since`        | _all-time_                | `Nd` for a relative window, `YYYY-MM-DD` for an absolute UTC date |
| `--tickets-csv`  | `circular_tickets.csv`    | Per-event output path                                             |
| `--gateways-csv` | `circular_by_gateway.csv` | Per-gateway aggregate output path                                 |

## Output

Console: grand total (count, ETH, USD) plus a per-gateway breakdown.

Two CSVs:

- `circular_tickets.csv` — one row per matched event (`timestamp_utc`, `tx_hash`, `sender_recipient`, `face_value_eth`, `face_value_usd`)
- `circular_by_gateway.csv` — per-sender aggregates sorted USD-descending

## How it works

1. Fetch all `Transcoder` entities with non-zero fee volume (the only addresses that could possibly receive a self-payment).
2. For each, query `winningTicketRedeemedEvents` where `sender == recipient == <addr>` — the subgraph indexes both fields, so this filter runs server-side.
3. Parallelize the per-orchestrator queries across 16 worker threads with `tqdm` progress.
4. Aggregate and write CSVs.

The subgraph schema doesn't support comparing two fields in a `where` clause
(`sender == recipient` directly), which is why we iterate per-orchestrator
instead of paginating every ticket and filtering client-side. The per-orch
approach transfers far less data over the wire.

## Limitations

This only catches **literal** self-payments (`sender == recipient` on-chain).
Operators running a gateway and an orchestrator under **different** addresses
will not show up — detecting that requires an external operator-to-address
mapping or clustering heuristics.
# livepeer-circular-demand-check
