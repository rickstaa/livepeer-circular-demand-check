#!/usr/bin/env python3
"""
Identify "circular" winning-ticket redemptions on the Livepeer Arbitrum subgraph:
WinningTicketRedeemed events where the sender (gateway/broadcaster) address
equals the recipient (orchestrator/transcoder) address — i.e. an operator
paying fees to themselves.

Reports:
  - total protocol fees in the selected window
  - circular (self-payment) fees in the same window
  - circular share of total (%)
  - per-gateway breakdown

Writes two CSVs:
  circular_tickets.csv      every matching event
  circular_by_gateway.csv   per-sender aggregates

USD values use the spot price recorded by the subgraph at ticket redemption
time. They are NOT marked-to-current — a ticket redeemed at $3000/ETH stays
$3000/ETH in this report.

Env:
  THEGRAPH_API_KEY   required, from https://thegraph.com/studio/apikeys/

Install:
  pip install -r requirements.txt

Usage:
  python circular_tickets.py
  python circular_tickets.py --since 30d
  python circular_tickets.py --since 2025-01-01
"""

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from tqdm import tqdm

SUBGRAPH_ID = "FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC"
PAGE = 1000
WORKERS = 16


def build_client():
    key = os.environ.get("THEGRAPH_API_KEY")
    if not key:
        sys.exit(
            "error: THEGRAPH_API_KEY env var is required.\n"
            "  export THEGRAPH_API_KEY=<your-key>   # from https://thegraph.com/studio/apikeys/"
        )
    transport = RequestsHTTPTransport(
        url=f"https://gateway.thegraph.com/api/{key}/subgraphs/id/{SUBGRAPH_ID}",
        timeout=60,
        retries=3,
    )
    return Client(transport=transport, fetch_schema_from_transport=False)


def parse_since(s):
    """Returns a unix timestamp; 0 means 'no filter' (matches all events)."""
    if not s:
        return 0
    if s.endswith("d") and s[:-1].isdigit():
        days = int(s[:-1])
        return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    return int(
        datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )


TRANSCODERS_QUERY = gql(
    """
    query Transcoders($last: String!, $first: Int!) {
      transcoders(
        first: $first,
        where: { id_gt: $last, totalVolumeETH_gt: 0 },
        orderBy: id,
        orderDirection: asc
      ) { id }
    }
    """
)

PROTOCOL_TOTALS_QUERY = gql(
    """
    query ProtocolTotals {
      protocol(id: "0") {
        totalVolumeETH
        totalVolumeUSD
      }
    }
    """
)

DAYS_QUERY = gql(
    """
    query Days($lastDate: Int!, $first: Int!, $sinceTs: Int!) {
      days(
        first: $first,
        where: { date_gt: $lastDate, date_gte: $sinceTs },
        orderBy: date,
        orderDirection: asc
      ) {
        date
        volumeETH
        volumeUSD
      }
    }
    """
)

SELF_REDEMPTIONS_QUERY = gql(
    """
    query SelfRedemptions(
      $addr: String!,
      $last: String!,
      $first: Int!,
      $sinceTs: Int
    ) {
      winningTicketRedeemedEvents(
        first: $first,
        where: {
          sender: $addr,
          recipient: $addr,
          id_gt: $last,
          timestamp_gte: $sinceTs
        },
        orderBy: id,
        orderDirection: asc
      ) {
        id
        timestamp
        transaction { id }
        faceValue
        faceValueUSD
      }
    }
    """
)


def fetch_total_fees(client, since_ts):
    """Total protocol fees in the selected window.

    All-time: single Protocol singleton lookup.
    Windowed: paginate Day entities and sum volumeETH / volumeUSD.
    """
    if since_ts == 0:
        data = client.execute(PROTOCOL_TOTALS_QUERY)
        p = data["protocol"]
        return float(p["totalVolumeETH"]), float(p["totalVolumeUSD"])

    total_eth = 0.0
    total_usd = 0.0
    last_date = since_ts - 1  # cursor; first batch picks up days with date >= since_ts
    while True:
        data = client.execute(
            DAYS_QUERY,
            variable_values={"lastDate": last_date, "first": PAGE, "sinceTs": since_ts},
        )
        batch = data["days"]
        if not batch:
            break
        for d in batch:
            total_eth += float(d["volumeETH"])
            total_usd += float(d["volumeUSD"])
        if len(batch) < PAGE:
            break
        last_date = int(batch[-1]["date"])
    return total_eth, total_usd


def fetch_all_transcoders(client):
    ids = []
    last = ""
    while True:
        data = client.execute(TRANSCODERS_QUERY, variable_values={"last": last, "first": PAGE})
        batch = data["transcoders"]
        if not batch:
            break
        ids.extend(t["id"] for t in batch)
        if len(batch) < PAGE:
            break
        last = batch[-1]["id"]
    return ids


def fetch_self_redemptions(addr, since_ts):
    """Each worker thread builds its own client — requests sessions aren't
    guaranteed thread-safe across concurrent execute() calls."""
    client = build_client()
    results = []
    last_id = ""
    while True:
        data = client.execute(
            SELF_REDEMPTIONS_QUERY,
            variable_values={
                "addr": addr,
                "last": last_id,
                "first": PAGE,
                "sinceTs": since_ts,
            },
        )
        batch = data["winningTicketRedeemedEvents"]
        if not batch:
            break
        for e in batch:
            e["addr"] = addr  # remember the gateway for aggregation
        results.extend(batch)
        if len(batch) < PAGE:
            break
        last_id = batch[-1]["id"]
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--since",
        help="Time filter: '30d' (relative) or 'YYYY-MM-DD' (absolute UTC). Default: all-time.",
    )
    ap.add_argument("--tickets-csv", default="circular_tickets.csv")
    ap.add_argument("--gateways-csv", default="circular_by_gateway.csv")
    args = ap.parse_args()

    since_ts = parse_since(args.since)
    if since_ts:
        print(
            f"Filtering to events on/after "
            f"{datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()}",
            file=sys.stderr,
        )

    main_client = build_client()

    print("Fetching protocol totals for window...", file=sys.stderr)
    total_protocol_eth, total_protocol_usd = fetch_total_fees(main_client, since_ts)
    print(
        f"  {total_protocol_eth:.6f} ETH (${total_protocol_usd:,.2f}) "
        f"— at spot price when each ticket was redeemed",
        file=sys.stderr,
    )

    print("Fetching transcoder list...", file=sys.stderr)
    transcoders = fetch_all_transcoders(main_client)
    print(f"  {len(transcoders)} transcoders with non-zero fee volume", file=sys.stderr)

    all_events = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_self_redemptions, addr, since_ts): addr for addr in transcoders}
        bar = tqdm(as_completed(futures), total=len(futures), unit="orch", file=sys.stderr)
        for fut in bar:
            addr = futures[fut]
            events = fut.result()
            if events:
                eth = sum(float(e["faceValue"]) for e in events)
                bar.write(f"  {addr}: {len(events)} self-payment(s), {eth:.6f} ETH")
                all_events.extend(events)

    per_gw = {}
    for e in all_events:
        s = e["addr"]
        agg = per_gw.setdefault(s, {"count": 0, "eth": 0.0, "usd": 0.0})
        agg["count"] += 1
        agg["eth"] += float(e["faceValue"])
        agg["usd"] += float(e["faceValueUSD"])

    total_eth = sum(a["eth"] for a in per_gw.values())
    total_usd = sum(a["usd"] for a in per_gw.values())
    total_n = sum(a["count"] for a in per_gw.values())

    with open(args.tickets_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["timestamp_utc", "tx_hash", "sender_recipient", "face_value_eth", "face_value_usd"]
        )
        for e in sorted(all_events, key=lambda x: int(x["timestamp"])):
            w.writerow(
                [
                    datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).isoformat(),
                    e["transaction"]["id"],
                    e["addr"],
                    e["faceValue"],
                    e["faceValueUSD"],
                ]
            )

    with open(args.gateways_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["gateway", "self_payment_count", "total_face_value_eth", "total_face_value_usd"]
        )
        for s, a in sorted(per_gw.items(), key=lambda kv: -kv[1]["usd"]):
            w.writerow([s, a["count"], f"{a['eth']:.6f}", f"{a['usd']:.2f}"])

    pct_eth = (total_eth / total_protocol_eth * 100) if total_protocol_eth > 0 else 0.0
    pct_usd = (total_usd / total_protocol_usd * 100) if total_protocol_usd > 0 else 0.0

    window_label = (
        f"since {datetime.fromtimestamp(since_ts, tz=timezone.utc).date().isoformat()}"
        if since_ts else "all-time"
    )

    print()
    print(f"Window: {window_label}")
    print(f"  USD values are spot at ticket redemption (not current price).")
    print()
    print(f"Total protocol fees: {total_protocol_eth:.6f} ETH (${total_protocol_usd:,.2f})")
    print(
        f"Circular fees:       {total_eth:.6f} ETH (${total_usd:,.2f}) "
        f"across {total_n} tickets"
    )
    print(f"Circular share:      {pct_eth:.2f}% of ETH  |  {pct_usd:.2f}% of USD")
    print()
    print(f"Per-gateway breakdown ({len(per_gw)} addresses):")
    for s, a in sorted(per_gw.items(), key=lambda kv: -kv[1]["usd"]):
        print(
            f"  {s}: {a['count']:>5} tickets, "
            f"{a['eth']:.6f} ETH (${a['usd']:,.2f})"
        )
    print()
    print(f"Wrote {args.tickets_csv} and {args.gateways_csv}")


if __name__ == "__main__":
    main()
