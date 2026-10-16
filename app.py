# ============================================================
# SOLANA EARLY SIGNAL - RAILWAY EDITION
# ============================================================

import asyncio
import json
import aiohttp
import websockets
import base64
import hashlib
import struct
import time
import os
from collections import deque

# قراءة المتغيرات من بيئة Railway
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

print("Telegram Bot Token:", "OK" if TELEGRAM_BOT_TOKEN else "MISSING")
print("Telegram Chat ID:", "OK" if TELEGRAM_CHAT_ID else "MISSING")

# قائمة العقد المجانية مع إمكانية إضافة Helius إذا كان متوفراً
PUBLIC_WSS_NODES = [
    "wss://solana-rpc.publicnode.com",
    "wss://api.mainnet-beta.solana.com",
    "wss://rpc.ankr.com/solana/ws"
]

if HELIUS_API_KEY:
    HELIUS_API_KEY = HELIUS_API_KEY.strip()
    PUBLIC_WSS_NODES.insert(0, f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}")

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# ============================================================
# FILTERS CONFIGURATION
# ============================================================
MIN_TRANSACTIONS = 80
MIN_UNIQUE_WALLETS = 77
MIN_TRANSACTION_SOL = 0.008
MAX_WALLET_TRANSACTIONS = 3
MIN_MCAP_SOL = 60.0

MAX_LIQUIDITY_MCAP_PCT = 25.0
MAX_VOLUME_LIQUIDITY_PCT = 92.0

MIN_BUY_PRESSURE_PCT = 59.0
MIN_LIQUIDITY_SOL = 50.0

CLUSTER_WINDOW_SECONDS = 30
TOTAL_SUPPLY = 1_000_000_000_000_000
LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_CLEANUP_SECONDS = 7200
MAX_PROCESSED_SIGNATURES = 100_000
BUNDLER_MAX_SECONDS = 5.0
MAX_TOP_30_BUY_PCT = 16.0

TRADE_EVENT_DISCRIMINATOR = hashlib.sha256(b"event:TradeEvent").digest()[:8]
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def scrub(text):
    text = str(text)
    if HELIUS_API_KEY:
        text = text.replace(HELIUS_API_KEY, "***")
    return text

def base58_encode(data):
    n = int.from_bytes(data, "big")
    result = ""
    while n > 0:
        n, r = divmod(n, 58)
        result = ALPHABET[r] + result
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return "1" * pad + result

def decode_trade_event(raw):
    try:
        if len(raw) < 8 or raw[:8] != TRADE_EVENT_DISCRIMINATOR:
            return None
        data = raw[8:]
        if len(data) < 121:
            return None

        offset = 0
        mint_bytes = data[offset:offset + 32]; offset += 32
        sol_amount = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        token_amount = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        is_buy = bool(data[offset]); offset += 1
        user_bytes = data[offset:offset + 32]; offset += 32
        timestamp = struct.unpack_from("<q", data, offset)[0]; offset += 8
        virtual_sol_reserves = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        virtual_token_reserves = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        real_sol_reserves = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        real_token_reserves = struct.unpack_from("<Q", data, offset)[0]

        return {
            "mint": base58_encode(mint_bytes),
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "user": base58_encode(user_bytes),
            "timestamp": timestamp,
            "virtual_sol_reserves": virtual_sol_reserves,
            "virtual_token_reserves": virtual_token_reserves,
            "real_sol_reserves": real_sol_reserves,
            "real_token_reserves": real_token_reserves,
        }
    except Exception:
        return None

async def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=15) as response:
                res_text = await response.text()
                if response.status == 200:
                    return True
                else:
                    print(f"❌ Telegram API Error ({response.status}): {res_text}")
                    return False
    except Exception as e:
        print(f"❌ Telegram Exception: {e}")
        return False

tokens = {}
processed_signatures = set()

def create_token(event, signature):
    return {
        "mint": event["mint"],
        "first_timestamp": event["timestamp"],
        "trades": 0,
        "wallets": set(),
        "first_30_wallets": [],
        "top30_buy_tokens": 0,
        "top30_completion_time": None,
        "buy_sol": 0.0,
        "sell_sol": 0.0,
        "volume_sol": 0.0,
        "mcap_sol": 0.0,
        "peak_mcap_sol": 0.0,
        "liquidity_sol": 0.0,
        "wallet_stats": {},
        "excluded_wallets": set(),
        "wallet_buy_sol": {},
        "wallet_sell_sol": {},
        "cluster_events": deque(),
        "cluster_wallets": set(),
        "last_activity": time.time(),
        "alert_sent": False
    }

def calculate_mcap(event):
    try:
        vs = event["virtual_sol_reserves"]
        vt = event["virtual_token_reserves"]
        if vt <= 0:
            return 0.0
        sol = vs / LAMPORTS_PER_SOL
        return sol / (vt / TOTAL_SUPPLY)
    except Exception:
        return 0.0

async def process_trade(event, signature):
    mint = event["mint"]
    sol_amount = event["sol_amount"] / LAMPORTS_PER_SOL
    token_amount = event["token_amount"]
    wallet = event["user"]

    if sol_amount < MIN_TRANSACTION_SOL:
        return

    if mint not in tokens:
        tokens[mint] = create_token(event, signature)

    token = tokens[mint]
    token["last_activity"] = time.time()

    if token["alert_sent"]:
        return

    if wallet in token["excluded_wallets"]:
        return

    current_wallet_transactions = token["wallet_stats"].get(wallet, 0)
    new_wallet_transactions = current_wallet_transactions + 1

    if new_wallet_transactions > MAX_WALLET_TRANSACTIONS:
        previous_count = current_wallet_transactions
        token["wallets"].discard(wallet)
        token["excluded_wallets"].add(wallet)
        token["wallet_stats"].pop(wallet, None)
        token["trades"] = max(0, token["trades"] - previous_count)

        previous_buy = token["wallet_buy_sol"].pop(wallet, 0.0)
        previous_sell = token["wallet_sell_sol"].pop(wallet, 0.0)
        token["buy_sol"] = max(0.0, token["buy_sol"] - previous_buy)
        token["sell_sol"] = max(0.0, token["sell_sol"] - previous_sell)
        token["volume_sol"] = token["buy_sol"] + token["sell_sol"]
        return

    token["wallet_stats"][wallet] = new_wallet_transactions
    token["wallets"].add(wallet)
    token["trades"] += 1

    if event["is_buy"]:
        if wallet not in token["first_30_wallets"] and len(token["first_30_wallets"]) < 30:
            token["first_30_wallets"].append(wallet)
            if len(token["first_30_wallets"]) == 30:
                token["top30_completion_time"] = event["timestamp"]

        if wallet in token["first_30_wallets"]:
            token["top30_buy_tokens"] += token_amount

    if event["is_buy"]:
        token["buy_sol"] += sol_amount
        token["wallet_buy_sol"][wallet] = token["wallet_buy_sol"].get(wallet, 0.0) + sol_amount
    else:
        token["sell_sol"] += sol_amount
        token["wallet_sell_sol"][wallet] = token["wallet_sell_sol"].get(wallet, 0.0) + sol_amount

    token["volume_sol"] = token["buy_sol"] + token["sell_sol"]

    current_mcap = calculate_mcap(event)
    token["mcap_sol"] = current_mcap
    if current_mcap > token["peak_mcap_sol"]:
        token["peak_mcap_sol"] = current_mcap

    token["liquidity_sol"] = event["real_sol_reserves"] / LAMPORTS_PER_SOL

    now_ts = event["timestamp"]
    token["cluster_events"].append((now_ts, wallet))
    while token["cluster_events"]:
        oldest_ts, _ = token["cluster_events"][0]
        if now_ts - oldest_ts <= CLUSTER_WINDOW_SECONDS:
            break
        token["cluster_events"].popleft()
    token["cluster_wallets"] = {w for _, w in token["cluster_events"]}

    await qualify_token(token)

async def qualify_token(token):
    if token["alert_sent"]:
        return

    if token["trades"] < MIN_TRANSACTIONS:
        return

    unique_wallets = len(token["wallets"])
    if unique_wallets < MIN_UNIQUE_WALLETS:
        return

    current_mcap = token["mcap_sol"]
    if current_mcap < MIN_MCAP_SOL:
        return

    liquidity = token["liquidity_sol"]
    if liquidity < MIN_LIQUIDITY_SOL:
        return

    liquidity_mcap_pct = (liquidity / current_mcap) * 100
    if liquidity_mcap_pct >= MAX_LIQUIDITY_MCAP_PCT:
        return

    volume = token["volume_sol"]
    if volume <= 0:
        return

    volume_liquidity_pct = (volume / liquidity) * 100
    if volume_liquidity_pct >= MAX_VOLUME_LIQUIDITY_PCT:
        return

    top30_pct = (token["top30_buy_tokens"] / TOTAL_SUPPLY) * 100.0
    if top30_pct > MAX_TOP_30_BUY_PCT:
        return

    buy_pressure = (token["buy_sol"] / volume * 100) if volume > 0 else 0
    if buy_pressure < MIN_BUY_PRESSURE_PCT:
        return

    token["alert_sent"] = True

    is_likely_bundler = False
    top30_window = 0.0
    if token["top30_completion_time"] is not None:
        top30_window = token["top30_completion_time"] - token["first_timestamp"]
        if top30_window <= BUNDLER_MAX_SECONDS:
            is_likely_bundler = True

    bundler_text = f"⚠️ LIKELY BUNDLER ({top30_window:.1f}s)" if is_likely_bundler else "✅ ORGANIC FLOW"
    net_flow = token["buy_sol"] - token["sell_sol"]
    age = max(0, int(time.time() - token["first_timestamp"]))
    mint = token["mint"]

    message = f"""
🚨 SOLANA EARLY SIGNAL 🚨

QUALIFIED (Top 30 under {MAX_TOP_30_BUY_PCT}%)

Mint
<code>{mint}</code>

Age: {age}s

ACTIVITY
Transactions: {token["trades"]}
Unique Wallets: {unique_wallets}
Top 30 Wallets Buy: {top30_pct:.2f}%
Pattern: {bundler_text}

MCAP
Current: {current_mcap:.2f} SOL
Peak: {token["peak_mcap_sol"]:.2f} SOL

FLOW
Buy: {token["buy_sol"]:.4f} SOL
Sell: {token["sell_sol"]:.4f} SOL
Net: {net_flow:+.4f} SOL
Buy Pressure: {buy_pressure:.2f}% (>= {MIN_BUY_PRESSURE_PCT}%)

LIQUIDITY
Liquidity: {liquidity:.4f} SOL (>= {MIN_LIQUIDITY_SOL} SOL)
Liq / MCAP: {liquidity_mcap_pct:.2f}%

VOLUME
Volume: {volume:.4f} SOL
Vol / Liq: {volume_liquidity_pct:.2f}%

🔗 <a href="https://pump.fun/coin/{mint}">Pump.fun</a>
• <a href="https://solscan.io/token/{mint}">Solscan</a>
"""

    sent = await send_telegram(message)
    if sent:
        print(f"\n✅ Qualified & Alert Sent! Mint: {mint} | Buy Pressure: {buy_pressure:.1f}% | Liq: {liquidity:.1f} SOL")
    else:
        print(f"\n⚠️ Alert generated for {mint}, but Telegram delivery failed.")

async def cleanup_loop():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        remove = [mint for mint, token in tokens.items() if now - token["last_activity"] > TOKEN_CLEANUP_SECONDS]
        for mint in remove:
            tokens.pop(mint, None)

async def status_loop():
    while True:
        await asyncio.sleep(30)
        total_trades = sum(t["trades"] for t in tokens.values())
        print(f"📡 Active tokens: {len(tokens)} | Accepted transactions: {total_trades}")

async def listen_solana():
    node_index = 0
    reconnect_delay = 5

    while True:
        current_wss = PUBLIC_WSS_NODES[node_index]
        try:
            print(f"\n⏳ Connecting to WSS: {scrub(current_wss)}")
            async with websockets.connect(current_wss, ping_interval=15, ping_timeout=60) as ws:
                subscription = {
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [PUMP_FUN_PROGRAM]}, {"commitment": "confirmed"}]
                }
                await ws.send(json.dumps(subscription))
                await ws.recv()
                print(f"✅ Connected to {scrub(current_wss)}")
                reconnect_delay = 5

                async for raw_message in ws:
                    try:
                        message = json.loads(raw_message)
                        if message.get("method") != "logsNotification":
                            continue
                        value = message.get("params", {}).get("result", {}).get("value", {})
                        signature = value.get("signature")
                        if not signature or signature in processed_signatures:
                            continue

                        processed_signatures.add(signature)
                        if len(processed_signatures) > MAX_PROCESSED_SIGNATURES:
                            processed_signatures.pop()

                        for log in value.get("logs", []):
                            if log.startswith("Program data:"):
                                try:
                                    encoded = log.split("Program data:", 1)[1].strip()
                                    raw = base64.b64decode(encoded)
                                    event = decode_trade_event(raw)
                                    if event:
                                        await process_trade(event, signature)
                                except Exception:
                                    continue
                    except Exception:
                        continue
        except Exception as e:
            print(f"❌ WebSocket error on {scrub(current_wss)}: {e}")
            node_index = (node_index + 1) % len(PUBLIC_WSS_NODES)
            print(f"🔄 Switching to next public node in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)

async def main():
    print("=" * 60)
    print("SOLANA EARLY SIGNAL - RAILWAY EDITION")
    print("=" * 60)
    await asyncio.gather(listen_solana(), cleanup_loop(), status_loop())

if __name__ == "__main__":
    asyncio.run(main())
