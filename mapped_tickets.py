#!/usr/bin/env python3
"""
Match WinningTicketRedeemed events between known sets of gateways and
orchestrators that are believed to be operated by the same entity on the
Livepeer Arbitrum subgraph.

Whereas circular_tickets.py only catches literal self-payments
(sender == recipient on-chain), this script catches the broader case
where an operator runs a gateway and an orchestrator under different
addresses. It needs an external mapping to do so.

The mapping is supplied as a JSON file (--operators) listing one or more
operators, each with their set of gateway addresses and orchestrator
addresses. For each operator, the script queries redemptions where the
sender is in that operator's gateway set and the recipient is in the
same operator's orchestrator set, then reports the per-operator and
combined share of total protocol fees.

How mappings are typically derived (out of scope for this script):
  - explorer.livepeer.org broadcasting / orchestrator pages
  - livepeer.tools
  - ServiceRegistry / AIServiceRegistry ServiceURI hostnames
  - off-chain disclosure by the operator

Env:
  THEGRAPH_API_KEY   required, from https://thegraph.com/studio/apikeys/

Usage:
  python mapped_tickets.py --operators operators.json
  python mapped_tickets.py --operators operators.json --since 30d
  python mapped_tickets.py --operators operators.json --since 2025-01-01
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

SUBGRAPH_ID = "FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC"
PAGE = 1000


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
    if not s:
        return 0
    if s.endswith("d") and s[:-1].isdigit():
        days = int(s[:-1])
        return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    return int(
        datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )


def load_operators(path):
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        sys.exit(f"error: {path} must be a non-empty JSON array of operator entries.")
    out = []
    seen_names = set()
    for i, entry in enumerate(data):
        name = entry.get("name") or f"operator-{i}"
        if name in seen_names:
            sys.exit(f"error: duplicate operator name {name!r} in {path}.")
        seen_names.add(name)
        gateways = {g.lower() for g in entry.get("gateways", []) if g}
        orchs = {o.lower() for o in entry.get("orchestrators", []) if o}
        if not gateways or not orchs:
            sys.exit(
                f"error: operator {name!r} must have at least one gateway and one orchestrator."
            )
        out.append({"name": name, "gateways": gateways, "orchestrators": orchs})
    return out


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

CROSS_REDEMPTIONS_QUERY = gql(
    """
    query CrossRedemptions(
      $senders: [String!]!,
      $recipients: [String!]!,
      $last: String!,
      $first: Int!,
      $sinceTs: Int
    ) {
      winningTicketRedeemedEvents(
        first: $first,
        where: {
          sender_in: $senders,
          recipient_in: $recipients,
          id_gt: $last,
          timestamp_gte: $sinceTs
        },
        orderBy: id,
        orderDirection: asc
      ) {
        id
        timestamp
        transaction { id }
        sender { id }
        recipient { id }
        faceValue
        faceValueUSD
      }
    }
    """
)


def fetch_total_fees(client, since_ts):
    if since_ts == 0:
        data = client.execute(PROTOCOL_TOTALS_QUERY)
        p = data["protocol"]
        return float(p["totalVolumeETH"]), float(p["totalVolumeUSD"])
    total_eth = 0.0
    total_usd = 0.0
    last_date = since_ts - 1
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


def fetch_cross_redemptions(client, senders, recipients, since_ts):
    results = []
    last_id = ""
    while True:
        data = client.execute(
            CROSS_REDEMPTIONS_QUERY,
            variable_values={
                "senders": sorted(senders),
                "recipients": sorted(recipients),
                "last": last_id,
                "first": PAGE,
                "sinceTs": since_ts,
            },
        )
        batch = data["winningTicketRedeemedEvents"]
        if not batch:
            break
        results.extend(batch)
        if len(batch) < PAGE:
            break
        last_id = batch[-1]["id"]
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--operators",
        required=True,
        help="Path to JSON file listing operators (see operators.example.json).",
    )
    ap.add_argument(
        "--since",
        help="Time filter: '30d' (relative) or 'YYYY-MM-DD' (absolute UTC). Default: all-time.",
    )
    ap.add_argument("--out-csv", default="mapped_tickets.csv")
    args = ap.parse_args()

    operators = load_operators(args.operators)
    since_ts = parse_since(args.since)
    if since_ts:
        print(
            f"Filtering to events on/after "
            f"{datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat()}",
            file=sys.stderr,
        )

    client = build_client()

    print("Fetching protocol totals for window...", file=sys.stderr)
    total_protocol_eth, total_protocol_usd = fetch_total_fees(client, since_ts)
    print(
        f"  {total_protocol_eth:.6f} ETH (${total_protocol_usd:,.2f}) "
        f"— at spot price when each ticket was redeemed",
        file=sys.stderr,
    )

    per_operator = {}
    all_events = []
    for op in operators:
        print(
            f"Fetching redemptions for {op['name']}: "
            f"{len(op['gateways'])} gateway(s) -> {len(op['orchestrators'])} orchestrator(s)...",
            file=sys.stderr,
        )
        events = fetch_cross_redemptions(
            client, op["gateways"], op["orchestrators"], since_ts
        )
        print(f"  {len(events)} matching event(s)", file=sys.stderr)
        agg = {"count": 0, "eth": 0.0, "usd": 0.0, "pairs": {}}
        for e in events:
            s = e["sender"]["id"].lower()
            r = e["recipient"]["id"].lower()
            agg["count"] += 1
            agg["eth"] += float(e["faceValue"])
            agg["usd"] += float(e["faceValueUSD"])
            pair = agg["pairs"].setdefault((s, r), {"count": 0, "eth": 0.0, "usd": 0.0})
            pair["count"] += 1
            pair["eth"] += float(e["faceValue"])
            pair["usd"] += float(e["faceValueUSD"])
            e["operator"] = op["name"]
            all_events.append(e)
        per_operator[op["name"]] = agg

    total_eth = sum(a["eth"] for a in per_operator.values())
    total_usd = sum(a["usd"] for a in per_operator.values())
    total_n = sum(a["count"] for a in per_operator.values())

    with open(args.out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "operator",
                "timestamp_utc",
                "tx_hash",
                "sender",
                "recipient",
                "face_value_eth",
                "face_value_usd",
            ]
        )
        for e in sorted(all_events, key=lambda x: (x["operator"], int(x["timestamp"]))):
            w.writerow(
                [
                    e["operator"],
                    datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).isoformat(),
                    e["transaction"]["id"],
                    e["sender"]["id"],
                    e["recipient"]["id"],
                    e["faceValue"],
                    e["faceValueUSD"],
                ]
            )

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
    print(f"Operators ({len(operators)}):")
    for op in operators:
        print(f"  {op['name']}:")
        print(f"    gateways ({len(op['gateways'])}):")
        for g in sorted(op["gateways"]):
            print(f"      {g}")
        print(f"    orchestrators ({len(op['orchestrators'])}):")
        for o in sorted(op["orchestrators"]):
            print(f"      {o}")
    print()
    print(f"Total protocol fees: {total_protocol_eth:.6f} ETH (${total_protocol_usd:,.2f})")
    print(
        f"Mapped intra-operator fees: {total_eth:.6f} ETH (${total_usd:,.2f}) "
        f"across {total_n} tickets"
    )
    print(f"Mapped share: {pct_eth:.2f}% of ETH  |  {pct_usd:.2f}% of USD")
    print()
    print("Per-operator breakdown:")
    for name, a in sorted(per_operator.items(), key=lambda kv: -kv[1]["usd"]):
        op_pct_eth = (a["eth"] / total_protocol_eth * 100) if total_protocol_eth > 0 else 0.0
        op_pct_usd = (a["usd"] / total_protocol_usd * 100) if total_protocol_usd > 0 else 0.0
        print(
            f"  {name}: {a['count']:>5} tickets, "
            f"{a['eth']:.6f} ETH (${a['usd']:,.2f}) "
            f"— {op_pct_eth:.2f}% of ETH | {op_pct_usd:.2f}% of USD"
        )
        for (s, r), p in sorted(a["pairs"].items(), key=lambda kv: -kv[1]["usd"]):
            print(
                f"    {s} -> {r}: {p['count']:>5} tickets, "
                f"{p['eth']:.6f} ETH (${p['usd']:,.2f})"
            )
    print()
    print(f"Wrote {args.out_csv}")


if __name__ == "__main__":
    main()
