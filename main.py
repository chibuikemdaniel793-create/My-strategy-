import os
import json
import asyncio
import requests
import numpy as np
import websockets
import threading
import time

# =========================
# ENV (DEMO ONLY)
# =========================
APP_ID = os.getenv("DERIV_APP_ID")
TOKEN = os.getenv("DERIV_API_TOKEN_DEMO")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

# =========================
# STATE
# =========================
bot_running = True
candles = {}
last_signal = {}

account = {
    "balance": 0,
    "currency": "",
    "profit": 0
}

# =========================
# TELEGRAM
# =========================
def send(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})


# =========================
# RSI
# =========================
def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:]) + 1e-9

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# =========================
# MACD
# =========================
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


# =========================
# CANDLE STREAK (>=3)
# =========================
def streak(c):
    last = c[-1]
    direction = "GREEN" if last["close"] > last["open"] else "RED"
    count = 1

    for i in range(len(c) - 2, -1, -1):
        x = c[i]
        if direction == "GREEN" and x["close"] > x["open"]:
            count += 1
        elif direction == "RED" and x["close"] < x["open"]:
            count += 1
        else:
            break

    return direction, count


# =========================
# STRATEGY
# =========================
def check(symbol, c):

    if len(c) < 20:
        return None

    closes = np.array([x["close"] for x in c[-20:]])

    r = rsi(closes)
    m, s = macd(closes)

    direction, st = streak(c)

    if st < 3:
        return None

    if r <= 30 and m > s and direction == "GREEN":
        return "BUY"

    if r >= 70 and m < s and direction == "RED":
        return "SELL"

    return None


# =========================
# ACCOUNT UPDATE
# =========================
def update_account(msg):
    global account

    if "balance" in msg:
        b = msg["balance"]
        account["balance"] = b.get("balance", 0)
        account["currency"] = b.get("currency", "")
        account["profit"] = b.get("profit", 0)


# =========================
# TELEGRAM CONTROL PANEL
# =========================
def telegram_listener():

    global bot_running

    last = 0

    while True:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            res = requests.get(url).json()

            for u in res.get("result", []):

                if u["update_id"] <= last:
                    continue

                last = u["update_id"]

                text = u.get("message", {}).get("text", "")

                if text == "/start":
                    send("✅ DEMO BOT ACTIVE\nScanning M1 markets...")

                elif text == "/stop":
                    bot_running = False
                    send("🛑 BOT STOPPED")

                elif text == "/status":
                    send(f"📊 RUNNING: {bot_running}\nMODE: DEMO ONLY")

                elif text == "/balance":
                    send(f"""
💰 DEMO ACCOUNT

Balance: {account['balance']} {account['currency']}
Profit: {account['profit']}
Mode: DEMO ONLY
""")

        except:
            pass

        time.sleep(3)


# =========================
# DERIV ENGINE (M1 ONLY)
# =========================
async def run():

    global candles

    async with websockets.connect(WS_URL) as ws:

        # AUTH
        await ws.send(json.dumps({"authorize": TOKEN}))
        await ws.recv()

        # DEMO BALANCE STREAM
        await ws.send(json.dumps({
            "balance": 1,
            "subscribe": 1
        }))

        # SYMBOLS
        await ws.send(json.dumps({
            "active_symbols": "full",
            "product_type": "basic"
        }))

        symbols = json.loads(await ws.recv())["active_symbols"]
        symbols = [s["symbol"] for s in symbols]

        send(f"📊 Loaded {len(symbols)} symbols (DEMO MODE)")

        # M1 CANDLES ONLY
        for sym in symbols:
            await ws.send(json.dumps({
                "ticks_history": sym,
                "adjust_start_time": 1,
                "count": 60,
                "end": "latest",
                "granularity": 60,   # 1 MINUTE
                "style": "candles",
                "subscribe": 1
            }))

        while bot_running:

            msg = json.loads(await ws.recv())

            # ACCOUNT
            if "balance" in msg:
                update_account(msg)

            # CANDLES
            if "candles" in msg:

                sym = msg["echo_req"]["ticks_history"]
                candles[sym] = msg["candles"]

                signal = check(sym, candles[sym])

                if signal:

                    key = f"{sym}_{signal}"

                    if key not in last_signal or time.time() - last_signal[key] > 600:

                        last_signal[key] = time.time()

                        send(f"""
🚨 DEMO SIGNAL

Asset: {sym}
Direction: {signal}

Timeframe: M1
RSI + MACD + 3+ candles
""")

# =========================
# START
# =========================
def start():

    t = threading.Thread(target=telegram_listener)
    t.daemon = True
    t.start()

    asyncio.run(run())


start()
