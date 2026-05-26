#!/usr/bin/env python3
"""
Scan the Livepeer ServiceRegistry and AIServiceRegistry on Arbitrum One
for ServiceURIUpdate events, then list every address that has EVER set a
ServiceURI matching a caller-supplied substring.

The two registries:
  - ServiceRegistry      0xc92d3a360b8f9e083ba64de15d95cf8180897431
  - AIServiceRegistry    0x04C0b249740175999E5BF5c9ac1dA92431EF34C5

Both emit:
  event ServiceURIUpdate(address indexed addr, string serviceURI)

We scan logs in chunked block ranges, decode the URI, group by address,
and filter by substring match. Output is JSON suitable for feeding into
mapped_tickets.py (drop the matched addresses into an operator entry's
`orchestrators` field).

Env:
  ARBITRUM_RPC_URL   optional, defaults to https://arb1.arbitrum.io/rpc.
                     For full-history scans an archive RPC is strongly
                     recommended (Alchemy/Infura/Ankr).

Usage:
  python fetch_registry_orchs.py --match example.com
  python fetch_registry_orchs.py --match example.com --from-block 6700000 --chunk 10000
"""

import argparse
import json
import os
import sys
import time

from web3 import Web3
from web3.exceptions import Web3RPCError

SERVICE_REGISTRY = "0xc92d3A360b8f9e083BA64DE15d95CF8180897431"
AI_SERVICE_REGISTRY = "0x04C0b249740175999E5BF5c9ac1dA92431EF34C5"

ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "addr", "type": "address"},
            {"indexed": False, "name": "serviceURI", "type": "string"},
        ],
        "name": "ServiceURIUpdate",
        "type": "event",
    }
]

DEFAULT_RPC = "https://arb1.arbitrum.io/rpc"
# Livepeer protocol was deployed to Arbitrum One in Feb 2022 around this
# block. Setting it lower wastes time scanning empty blocks; setting it
# higher risks missing early ServiceURIUpdate events.
DEFAULT_FROM_BLOCK = 6_700_000
DEFAULT_CHUNK = 10_000
MIN_CHUNK = 500


def scan_registry(w3, addr, from_block, to_block, chunk, label):
    contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ABI)
    event = contract.events.ServiceURIUpdate
    out = []
    start = from_block
    current_chunk = chunk
    last_progress = time.time()
    while start <= to_block:
        end = min(start + current_chunk - 1, to_block)
        try:
            logs = event.get_logs(from_block=start, to_block=end)
        except Web3RPCError as e:
            # RPC complained about range/response size — back off.
            if current_chunk > MIN_CHUNK:
                current_chunk = max(MIN_CHUNK, current_chunk // 2)
                print(
                    f"  [{label}] {start}-{end} rejected ({e}); shrinking chunk to {current_chunk}",
                    file=sys.stderr,
                )
                continue
            raise
        for l in logs:
            out.append(
                {
                    "block": l["blockNumber"],
                    "tx": l["transactionHash"].hex(),
                    "log_index": l["logIndex"],
                    "addr": l["args"]["addr"].lower(),
                    "uri": l["args"]["serviceURI"],
                    "source": label,
                }
            )
        now = time.time()
        if logs or (now - last_progress) > 5:
            pct = (end - from_block) / max(1, to_block - from_block) * 100
            print(
                f"  [{label}] {start}-{end} ({pct:5.1f}%) +{len(logs)} events "
                f"(running total {len(out)})",
                file=sys.stderr,
            )
            last_progress = now
        start = end + 1
        # Gently grow the chunk back if we shrank it earlier.
        if current_chunk < chunk:
            current_chunk = min(chunk, current_chunk * 2)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--rpc",
        default=os.environ.get("ARBITRUM_RPC_URL", DEFAULT_RPC),
        help="Arbitrum One JSON-RPC endpoint. Default: ARBITRUM_RPC_URL env or public RPC.",
    )
    ap.add_argument("--from-block", type=int, default=DEFAULT_FROM_BLOCK)
    ap.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="Default: latest block.",
    )
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK)
    ap.add_argument(
        "--match",
        required=True,
        help="Case-insensitive substring matched against each ServiceURI.",
    )
    ap.add_argument("--out", default="registry_orchs.json")
    args = ap.parse_args()

    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        sys.exit(f"error: cannot connect to RPC {args.rpc}")
    chain_id = w3.eth.chain_id
    if chain_id != 42161:
        print(
            f"warning: connected chain id is {chain_id}, expected 42161 (Arbitrum One)",
            file=sys.stderr,
        )

    to_block = args.to_block if args.to_block is not None else w3.eth.block_number
    print(
        f"Scanning Arbitrum blocks {args.from_block:,} -> {to_block:,} "
        f"(chunk {args.chunk:,})",
        file=sys.stderr,
    )

    events = []
    print("Scanning ServiceRegistry...", file=sys.stderr)
    events.extend(
        scan_registry(w3, SERVICE_REGISTRY, args.from_block, to_block, args.chunk, "service")
    )
    print("Scanning AIServiceRegistry...", file=sys.stderr)
    events.extend(
        scan_registry(
            w3, AI_SERVICE_REGISTRY, args.from_block, to_block, args.chunk, "ai_service"
        )
    )

    by_addr = {}
    for e in sorted(events, key=lambda x: (x["block"], x["log_index"])):
        a = e["addr"]
        rec = by_addr.setdefault(a, {"events": [], "uris": []})
        rec["events"].append(e)
        if e["uri"] not in rec["uris"]:
            rec["uris"].append(e["uri"])

    needle = args.match.lower()
    matches = {}
    for a, rec in by_addr.items():
        matching_uris = [u for u in rec["uris"] if needle in u.lower()]
        if not matching_uris:
            continue
        matches[a] = {
            "first_seen_block": rec["events"][0]["block"],
            "last_seen_block": rec["events"][-1]["block"],
            "current_uri": rec["events"][-1]["uri"],
            "current_uri_matches": needle in rec["events"][-1]["uri"].lower(),
            "all_uris": rec["uris"],
            "matching_uris": matching_uris,
            "sources": sorted({e["source"] for e in rec["events"]}),
            "events": rec["events"],
        }

    out = {
        "match": args.match,
        "scan_from_block": args.from_block,
        "scan_to_block": to_block,
        "chain_id": chain_id,
        "registries": {
            "service": SERVICE_REGISTRY,
            "ai_service": AI_SERVICE_REGISTRY,
        },
        "total_unique_addresses": len(by_addr),
        "total_events": len(events),
        "matched_addresses": sorted(matches.keys()),
        "details": matches,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2)

    print()
    print(f"Total ServiceURIUpdate events:           {len(events):,}")
    print(f"Unique addresses with any URI:           {len(by_addr):,}")
    print(f"Addresses matching '{args.match}':       {len(matches):,}")
    print()
    print(f"{'address':42} {'sources':22} current_uri")
    for a in sorted(matches.keys(), key=lambda k: matches[k]["last_seen_block"]):
        d = matches[a]
        cur = d["current_uri"]
        if not d["current_uri_matches"]:
            cur = f"{cur}  [LAST URI NO LONGER MATCHES]"
        print(f"  {a}  {','.join(d['sources']):20} {cur}")
    print()
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
