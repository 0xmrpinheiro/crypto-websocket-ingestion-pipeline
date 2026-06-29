#!/usr/bin/env python3
import asyncio
import json
import time
import random
import websockets
from concurrent.futures import ThreadPoolExecutor

BINANCE_WS = "wss://stream.binance.com:9443"
SYMBOLS = ["btcusdt", "ethusdt"]

async def send_messages(symbol: str, count: int):
    stream = f"{symbol}@trade"
    url = f"{BINANCE_WS}/stream?streams={stream}"
    
    start_time = time.time()
    messages_sent = 0
    
    try:
        async with websockets.connect(url) as ws:
            while messages_sent < count:
                msg = await ws.recv()
                messages_sent += 1
                
                if messages_sent % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_sent / elapsed
                    print(f"{symbol}: {rate:.2f} msg/sec")
    except Exception as e:
        print(f"{symbol} error: {e}")
    
    elapsed = time.time() - start_time
    print(f"{symbol} final: {messages_sent} messages in {elapsed:.2f}s = {messages_sent/elapsed:.2f} msg/sec")

async def run_load_test(duration_seconds: int = 60):
    print(f"Starting load test for {duration_seconds} seconds...")
    start = time.time()
    
    tasks = [send_messages(sym, 10000) for sym in SYMBOLS]
    await asyncio.gather(*tasks)
    
    elapsed = time.time() - start
    print(f"Total time: {elapsed:.2f}s")

if __name__ == "__main__":
    asyncio.run(run_load_test())