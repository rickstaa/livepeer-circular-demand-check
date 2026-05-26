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

Console:

- **Total protocol fees** in the selected window (ETH + USD)
- **Circular fees** (self-payments) in the same window — count, ETH, USD
- **Circular share** — circular fees as a percentage of total protocol fees
- **Per-gateway breakdown** sorted USD-descending

Two CSVs:

- `circular_tickets.csv` — one row per matched event (`timestamp_utc`, `tx_hash`, `sender_recipient`, `face_value_eth`, `face_value_usd`)
- `circular_by_gateway.csv` — per-sender aggregates sorted USD-descending

### How USD values are computed

USD figures are the **spot value at ticket-redemption time**, as recorded by
the subgraph when each `WinningTicketRedeemed` event was indexed. Nothing is
marked-to-current — a ticket redeemed at $3000/ETH still counts as $3000/ETH
in this report, even if ETH is $4000 today. This applies to both the total
protocol fees and the circular total, so the percentage comparison is
apples-to-apples within the window.

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

---

## Mapped operators: `mapped_tickets.py`

To catch the broader case where an operator runs gateways and orchestrators
under **different** addresses, supply an external mapping and run
`mapped_tickets.py`. It queries `WinningTicketRedeemed` events where the
sender is one of the operator's gateways and the recipient is one of the
same operator's orchestrators, and reports the per-operator and combined
share of total protocol fees.

### Mapping format

Copy `operators.example.json` to `operators.json` and edit:

```json
[
  {
    "name": "operator-a",
    "gateways":      ["0x...", "0x..."],
    "orchestrators": ["0x..."]
  }
]
```

You can list as many operators as you like. The address sets can be derived
however you want — explorer.livepeer.org broadcasting/orchestrator pages,
livepeer.tools, ServiceRegistry / AIServiceRegistry ServiceURI hostnames,
or off-chain disclosure.

`operators.json` is gitignored; `operators.example.json` is checked in as a
template.

### Helper: discovering orchs by ServiceURI

`fetch_registry_orchs.py` scans the on-chain `ServiceRegistry` and
`AIServiceRegistry` for every address that has ever set a `ServiceURI`
matching a caller-supplied substring. Useful when you suspect an operator
runs many orchestrators under one hostname.

```bash
python fetch_registry_orchs.py --match example.com
# writes registry_orchs.json — paste matched addresses into operators.json
```

An archive RPC (Alchemy/Infura/Ankr) is strongly recommended over the
public Arbitrum RPC for full-history scans.

### Run

```bash
python mapped_tickets.py --operators operators.json
python mapped_tickets.py --operators operators.json --since 30d
python mapped_tickets.py --operators operators.json --since 2025-01-01
```

Output:

- Console: per-operator fee totals, per-(sender, recipient) breakdown,
  and each operator's share of total protocol fees in the window.
- `mapped_tickets.csv` — one row per matched event with an `operator`
  column.
