import os
import json
import asyncio
import requests
import numpy as np
import websockets
import threading
import time

# ================= CONFIG =================
APP_ID = os.getenv("DERIV_APP_ID")
TOKEN = os.getenv("DERIV_API_TOKEN_DEMO")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

bot_running = True
candles = {}
account = {"balance": 0, "currency": ""}

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        pass

# ================= INDICATORS =================
def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50

    delta = np.diff(closes)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)

    avg_gain = np.mean(gain[-period:])
    avg_loss = np.mean(loss[-period:]) + 1e-9

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def ema(data, period):
    alpha = 2 / (period + 1)
    out = [data[0]]
    for i in data[1:]:
        out.append(alpha * i + (1 - alpha) * out[-1])
    return np.array(out)


def macd(closes):
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    m = e12[-len(e26):] - e26
    s = ema(m, 9)
    return m[-1], s[-1]

# ================= STRATEGY =================
def check(c):
    if len(c) < 20:
        return None

    closes = np.array([x["close"] for x in c[-20:]])

    r = rsi(closes)
    m, s = macd(closes)

    direction = "GREEN" if c[-1]["close"] > c[-1]["open"] else "RED"

    # BUY condition
    if r <= 30 and m > s and direction == "GREEN":
        return "BUY"

    # SELL condition
    if r >= 70 and m < s and direction == "RED":
        return "SELL"

    return None


# ================= SIGNAL MEMORY =================
last_signal = {}

def allow_signal(sym, signal):
    key = f"{sym}_{signal}"
    now = time.time()

    if key in last_signal and now - last_signal[key] < 600:
        return False

    last_signal[key] = now
    return True


# ================= TELEGRAM CONTROL =================
def telegram_listener():
    global bot_running

    last = 0

    while True:
        try:
            res = requests.get(
                f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            ).json()

            for u in res.get("result", []):

                if u["update_id"] <= last:
                    continue

                last = u["update_id"]
                text = u.get("message", {}).get("text", "")

                if text == "/start":
                    bot_running = True
                    send("✅ BOT STARTED")

                elif text == "/stop":
                    bot_running = False
                    send("🛑 BOT STOPPED")

                elif text == "/status":
                    send(f"RUNNING: {bot_running}")

                elif text == "/balance":
                    send(f"BALANCE: {account['balance']} {account['currency']}")

        except:
            pass

        time.sleep(3)


# ================= GET ALL SYNTHETICS =================
async def get_synthetics(ws):
    await ws.send(json.dumps({
        "active_symbols": "full",
        "product_type": "basic"
    }))

    msg = json.loads(await ws.recv())

    symbols = []
    for s in msg.get("active_symbols", []):
        name = s["symbol"]

        # auto filter synthetic indices ONLY
        if any(x in name for x in ["R_", "BOOM", "CRASH"]):
            symbols.append(name)

    return symbols


# ================= CORE ENGINE =================
async def run():
    global candles

    async with websockets.connect(WS_URL) as ws:

        # AUTH
        await ws.send(json.dumps({"authorize": TOKEN}))
        await ws.recv()

        # BALANCE STREAM
        await ws.send(json.dumps({
            "balance": 1,
            "subscribe": 1
        }))

        send("📊 BOT CONNECTED")

        # GET ALL SYNTHETICS AUTOMATICALLY
        symbols = await get_synthetics(ws)

        send(f"🔍 Loaded {len(symbols)} synthetic assets")

        batch_size = 10

        while bot_running:

            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i+batch_size]

                for sym in batch:

                    # REQUEST M1 CANDLES
                    await ws.send(json.dumps({
                        "ticks_history": sym,
                        "count": 60,
                        "end": "latest",
                        "granularity": 60,
                        "style": "candles"
                    }))

                    msg = json.loads(await ws.recv())

                    # BALANCE UPDATE
                    if "balance" in msg:
                        b = msg["balance"]
                        account["balance"] = b.get("balance", 0)
                        account["currency"] = b.get("currency", "")

                    # CANDLE PROCESSING
                    if "candles" in msg:
                        candles[sym] = msg["candles"]

                        signal = check(candles[sym])

                        if signal and allow_signal(sym, signal):
                            send(f"""
🚨 SIGNAL

Asset: {sym}
Direction: {signal}
Timeframe: M1
""")

                # wait before next batch (prevents timeout)
                await asyncio.sleep(60)


# ================= START =================
def start():
    t = threading.Thread(target=telegram_listener)
    t.daemon = True
    t.start()

    asyncio.run(run())


start()
