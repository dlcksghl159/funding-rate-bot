#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Funding Rate Monitoring Telegram Bot
Monitor funding rates from Bybit, Binance, Bitget, and OKX exchanges
(선물 + 현물 둘 다 있는 코인만 필터링)
"""

import asyncio
import logging
import os
import json
import time
import aiohttp
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from telegram.error import BadRequest

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================
# spot symbol cache (현물)
SPOT_SYMBOL_CACHE = { 'binance': set(), 'bybit': set(), 'bitget': set(), 'okx': set() }
SPOT_SYMBOL_LAST_FETCH = { 'binance': 0.0, 'bybit': 0.0, 'bitget': 0.0, 'okx': 0.0 }
SPOT_SYMBOL_TTL = 60 * 60  # 1시간 캐시

# Binance 현물 심볼
async def get_binance_spot_symbols():
    now = time.time()
    if SPOT_SYMBOL_CACHE['binance'] and now - SPOT_SYMBOL_LAST_FETCH['binance'] < SPOT_SYMBOL_TTL:
        logger.info(f"Binance 현물 심볼 캐시 사용: {len(SPOT_SYMBOL_CACHE['binance'])}개")
        return SPOT_SYMBOL_CACHE['binance']

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/exchangeInfo") as resp:
                data = await resp.json()
                symbols = set()
                for s in data['symbols']:
                    if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                        symbols.add(f"{s['baseAsset']}/USDT:USDT")
                SPOT_SYMBOL_CACHE['binance'] = symbols
                SPOT_SYMBOL_LAST_FETCH['binance'] = now
                logger.info(f"Binance 현물 심볼 새로 로드: {len(symbols)}개")
                return symbols
    except Exception as e:
        logger.error(f"Binance spot symbol fetch error: {e}")
        return SPOT_SYMBOL_CACHE.get('binance', set())

# Bybit 현물 심볼
async def get_bybit_spot_symbols():
    now = time.time()
    if SPOT_SYMBOL_CACHE['bybit'] and now - SPOT_SYMBOL_LAST_FETCH['bybit'] < SPOT_SYMBOL_TTL:
        logger.info(f"Bybit 현물 심볼 캐시 사용: {len(SPOT_SYMBOL_CACHE['bybit'])}개")
        return SPOT_SYMBOL_CACHE['bybit']
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"}) as resp:
                data = await resp.json()
                symbols = set()
                if data.get("retCode") == 0:
                    for s in data['result']['list']:
                        if s['quoteCoin'] == 'USDT':
                            symbols.add(f"{s['baseCoin']}/USDT:USDT")
                SPOT_SYMBOL_CACHE['bybit'] = symbols
                SPOT_SYMBOL_LAST_FETCH['bybit'] = now
                logger.info(f"Bybit 현물 심볼 새로 로드: {len(symbols)}개")
                return symbols
    except Exception as e:
        logger.error(f"Bybit spot symbol fetch error: {e}")
        return SPOT_SYMBOL_CACHE.get('bybit', set())

# Bitget 현물 심볼
async def get_bitget_spot_symbols():
    now = time.time()
    if SPOT_SYMBOL_CACHE['bitget'] and now - SPOT_SYMBOL_LAST_FETCH['bitget'] < SPOT_SYMBOL_TTL:
        logger.info(f"Bitget 현물 심볼 캐시 사용: {len(SPOT_SYMBOL_CACHE['bitget'])}개")
        return SPOT_SYMBOL_CACHE['bitget']
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.bitget.com/api/v2/spot/public/symbols") as resp:
                data = await resp.json()
                symbols = set()
                if data.get("code") == "00000":
                    for s in data['data']:
                        if s['quoteCoin'] == 'USDT' and s['status'] == 'online':
                            symbols.add(f"{s['baseCoin']}/USDT:USDT")
                SPOT_SYMBOL_CACHE['bitget'] = symbols
                SPOT_SYMBOL_LAST_FETCH['bitget'] = now
                logger.info(f"Bitget 현물 심볼 새로 로드: {len(symbols)}개")
                return symbols
    except Exception as e:
        logger.error(f"Bitget spot symbol fetch error: {e}")
        return SPOT_SYMBOL_CACHE.get('bitget', set())

# OKX 현물 심볼
async def get_okx_spot_symbols():
    now = time.time()
    if SPOT_SYMBOL_CACHE['okx'] and now - SPOT_SYMBOL_LAST_FETCH['okx'] < SPOT_SYMBOL_TTL:
        logger.info(f"OKX 현물 심볼 캐시 사용: {len(SPOT_SYMBOL_CACHE['okx'])}개")
        return SPOT_SYMBOL_CACHE['okx']
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"}) as resp:
                data = await resp.json()
                symbols = set()
                if data.get("code") == "0":
                    for s in data['data']:
                        if s['quoteCcy'] == 'USDT' and s['state'] == 'live':
                            symbols.add(f"{s['baseCcy']}/USDT:USDT")
                SPOT_SYMBOL_CACHE['okx'] = symbols
                SPOT_SYMBOL_LAST_FETCH['okx'] = now
                logger.info(f"OKX 현물 심볼 새로 로드: {len(symbols)}개")
                return symbols
    except Exception as e:
        logger.error(f"OKX spot symbol fetch error: {e}")
        return SPOT_SYMBOL_CACHE.get('okx', set())
# ==========================

# 데이터베이스 초기화
def init_db():
    db_path = os.path.join(os.getenv('DATA_DIR', '.'), 'funding_bot.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # 테이블 생성
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings
                 (user_id INTEGER PRIMARY KEY,
                  threshold REAL DEFAULT 0.1,
                  volume_filter REAL DEFAULT 0,
                  exchanges TEXT DEFAULT '["bybit","binance","bitget","okx"]',
                  active INTEGER DEFAULT 1,
                  spot_filter INTEGER DEFAULT 1)''')
    
    # 기존 테이블에 spot_filter 컬럼이 없으면 추가
    try:
        c.execute("ALTER TABLE user_settings ADD COLUMN spot_filter INTEGER DEFAULT 1")
        conn.commit()
        logger.info("spot_filter 컬럼이 추가되었습니다.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            logger.info("spot_filter 컬럼이 이미 존재합니다.")
        else:
            logger.error(f"데이터베이스 오류: {e}")
    
    conn.commit()
    conn.close()

# 사용자 설정 가져오기
def get_user_settings(user_id):
    db_path = os.path.join(os.getenv('DATA_DIR', '.'), 'funding_bot.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT * FROM user_settings WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    
    if result:
        # 기존 데이터의 경우 spot_filter 컬럼이 없을 수 있음
        if len(result) >= 6:  # spot_filter 컬럼이 있는 경우
            # spot_filter 값을 안전하게 정수로 변환
            spot_filter_val = result[5]
            if spot_filter_val in [1, '1', True, 'true', 'True']:
                spot_filter = 1
            else:
                spot_filter = 0
            
            return {
                'threshold': result[1],
                'volume_filter': result[2],
                'exchanges': json.loads(result[3]),
                'active': result[4],
                'spot_filter': spot_filter
            }
        else:  # 기존 데이터의 경우 기본값 사용
            return {
                'threshold': result[1],
                'volume_filter': result[2],
                'exchanges': json.loads(result[3]),
                'active': result[4],
                'spot_filter': 1  # 기본값
            }
    return {
        'threshold': 0.1,
        'volume_filter': 0,
        'exchanges': ['bybit', 'binance', 'bitget', 'okx'],
        'active': 1,
        'spot_filter': 1
    }

# 사용자 설정 저장
def save_user_settings(user_id, settings):
    db_path = os.path.join(os.getenv('DATA_DIR', '.'), 'funding_bot.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO user_settings 
                 (user_id, threshold, volume_filter, exchanges, active, spot_filter) 
                 VALUES (?, ?, ?, ?, ?, ?)""",
              (user_id, settings['threshold'], settings['volume_filter'],
               json.dumps(settings['exchanges']), settings['active'], settings['spot_filter']))
    conn.commit()
    conn.close()

# ===== 거래소별 펀딩비 가져오기 (REST API) =====
async def get_binance_funding_rates(use_spot_filter=True):
    """Binance 펀딩비 가져오기 (REST API) + 현물+선물 교집합 필터"""
    try:
        if use_spot_filter:
            spot_symbols = await get_binance_spot_symbols()
            logger.info(f"Binance 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("Binance 현물 필터 비활성 - 모든 선물 코인 포함")
            
        async with aiohttp.ClientSession() as session:
            # 펀딩비 데이터
            async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex") as resp:
                if resp.status == 200:
                    prem_data = await resp.json()
                else:
                    return {}
            
            # 24시간 거래량 데이터
            async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as resp:
                if resp.status == 200:
                    ticker_data = await resp.json()
                else:
                    ticker_data = []
            
            ticker_dict = {t['symbol']: t for t in ticker_data}
            
            result = {}
            total_futures = 0
            filtered_out = 0
            
            for item in prem_data:
                symbol = item['symbol']
                if not symbol.endswith('USDT'):
                    continue
                    
                total_futures += 1
                base = symbol[:-4]
                ccxt_symbol = f"{base}/USDT:USDT"
                
                if use_spot_filter and ccxt_symbol not in spot_symbols:
                    filtered_out += 1
                    continue  # 현물에 없는 심볼은 제외
                
                ticker = ticker_dict.get(symbol, {})
                result[ccxt_symbol] = {
                    'symbol': ccxt_symbol,
                    'rate': float(item['lastFundingRate']) * 100,
                    'volume': float(ticker.get('quoteVolume', 0)),
                    'price': float(item['markPrice']),
                    'next_funding': int(item['nextFundingTime'])
                }
            
            logger.info(f"Binance 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(result)}")
            return result
            
    except Exception as e:
        logger.error(f"Binance API 오류: {e}")
        return {}

async def get_bybit_funding_rates(use_spot_filter=True):
    """Bybit 펀딩비 가져오기 + 현물+선물 교집합 필터"""
    try:
        if use_spot_filter:
            spot_symbols = await get_bybit_spot_symbols()
            logger.info(f"Bybit 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("Bybit 현물 필터 비활성 - 모든 선물 코인 포함")
            
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear"}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {}
                    total_futures = 0
                    filtered_out = 0
                    
                    if data.get("retCode") == 0:
                        for item in data["result"]["list"]:
                            symbol = item["symbol"]
                            if not symbol.endswith("USDT"):
                                continue
                                
                            total_futures += 1
                            base = symbol[:-4]
                            ccxt_symbol = f"{base}/USDT:USDT"
                            
                            if use_spot_filter and ccxt_symbol not in spot_symbols:
                                filtered_out += 1
                                continue
                            
                            result[ccxt_symbol] = {
                                'symbol': ccxt_symbol,
                                'rate': float(item["fundingRate"]) * 100,
                                'volume': float(item["turnover24h"]),
                                'price': float(item["lastPrice"]),
                                'next_funding': int(item["nextFundingTime"])
                            }
                    
                    logger.info(f"Bybit 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(result)}")
                    return result
        return {}
    except Exception as e:
        logger.error(f"Bybit API 오류: {e}")
        return {}

async def get_bitget_funding_rates(use_spot_filter=True):
    """Bitget 펀딩비 가져오기 + 현물+선물 교집합 필터"""
    try:
        if use_spot_filter:
            spot_symbols = await get_bitget_spot_symbols()
            logger.info(f"Bitget 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("Bitget 현물 필터 비활성 - 모든 선물 코인 포함")
            
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.bitget.com/api/mix/v1/market/tickers",
                params={"productType": "umcbl"}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {}
                    total_futures = 0
                    filtered_out = 0
                    
                    if data.get("code") == "00000":
                        for item in data["data"]:
                            raw_symbol = item["symbol"]
                            if not raw_symbol.endswith("_UMCBL"):
                                continue
                                
                            pair = raw_symbol.split("_")[0]
                            if not pair.endswith("USDT"):
                                continue
                                
                            total_futures += 1
                            base = pair[:-4]
                            ccxt_symbol = f"{base}/USDT:USDT"
                            
                            if use_spot_filter and ccxt_symbol not in spot_symbols:
                                filtered_out += 1
                                continue
                            
                            result[ccxt_symbol] = {
                                'symbol': ccxt_symbol,
                                'rate': float(item.get("fundingRate", 0)) * 100,
                                'volume': float(item.get("quoteVolume", 0)),
                                'price': float(item["last"]),
                                'next_funding': 0  # Bitget은 별도 API 호출 필요
                            }
                    
                    logger.info(f"Bitget 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(result)}")
                    return result
        return {}
    except Exception as e:
        logger.error(f"Bitget API 오류: {e}")
        return {}

async def get_okx_funding_rates(use_spot_filter=True):
    """OKX 펀딩비 가져오기 + 현물+선물 교집합 필터"""
    try:
        if use_spot_filter:
            spot_symbols = await get_okx_spot_symbols()
            logger.info(f"OKX 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("OKX 현물 필터 비활성 - 모든 선물 코인 포함")
            
        async with aiohttp.ClientSession() as session:
            # 선물 심볼 목록 가져오기
            async with session.get(
                "https://www.okx.com/api/v5/public/instruments",
                params={"instType": "SWAP"}
            ) as resp:
                if resp.status == 200:
                    inst_data = await resp.json()
                else:
                    return {}
            
            result = {}
            total_futures = 0
            filtered_out = 0
            
            if inst_data.get("code") == "0":
                # USDT 무기한 선물만 필터링
                swap_symbols = []
                for inst in inst_data["data"]:
                    if inst["settleCcy"] == "USDT" and inst["instId"].endswith("-USDT-SWAP"):
                        swap_symbols.append(inst["instId"])
                
                # 펀딩비 정보를 배치로 가져오기
                for i in range(0, len(swap_symbols), 20):  # 20개씩 배치 처리
                    batch = swap_symbols[i:i+20]
                    
                    # 각 심볼에 대한 펀딩비 가져오기
                    funding_tasks = []
                    ticker_tasks = []
                    
                    for inst_id in batch:
                        funding_tasks.append(session.get(
                            "https://www.okx.com/api/v5/public/funding-rate",
                            params={"instId": inst_id}
                        ))
                        ticker_tasks.append(session.get(
                            "https://www.okx.com/api/v5/market/ticker",
                            params={"instId": inst_id}
                        ))
                    
                    funding_responses = await asyncio.gather(*funding_tasks, return_exceptions=True)
                    ticker_responses = await asyncio.gather(*ticker_tasks, return_exceptions=True)
                    
                    for idx, inst_id in enumerate(batch):
                        try:
                            if isinstance(funding_responses[idx], Exception) or isinstance(ticker_responses[idx], Exception):
                                continue
                                
                            funding_resp = funding_responses[idx]
                            ticker_resp = ticker_responses[idx]
                            
                            if funding_resp.status == 200 and ticker_resp.status == 200:
                                funding_data = await funding_resp.json()
                                ticker_data = await ticker_resp.json()
                                
                                if funding_data.get("code") == "0" and ticker_data.get("code") == "0":
                                    if funding_data["data"] and ticker_data["data"]:
                                        base = inst_id.split("-")[0]
                                        ccxt_symbol = f"{base}/USDT:USDT"
                                        
                                        total_futures += 1
                                        
                                        if use_spot_filter and ccxt_symbol not in spot_symbols:
                                            filtered_out += 1
                                            continue
                                        
                                        funding_info = funding_data["data"][0]
                                        ticker_info = ticker_data["data"][0]
                                        
                                        result[ccxt_symbol] = {
                                            'symbol': ccxt_symbol,
                                            'rate': float(funding_info["fundingRate"]) * 100,
                                            'volume': float(ticker_info.get("volCcy24h", 0)),
                                            'price': float(ticker_info["last"]),
                                            'next_funding': int(funding_info["nextFundingTime"])
                                        }
                        except Exception as e:
                            logger.error(f"OKX symbol {inst_id} 처리 오류: {e}")
                            continue
                    
                    # API 레이트 리밋을 위한 대기
                    await asyncio.sleep(0.1)
            
            logger.info(f"OKX 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(result)}")
            return result
            
    except Exception as e:
        logger.error(f"OKX API 오류: {e}")
        return {}

# ===== 바이낸스 웹소켓 모니터 클래스 =====
class BinanceWebSocketMonitor:
    """바이낸스 펀딩비 및 24시간 거래량을 웹소켓으로 수신하여 메모리에 보관"""

    MARK_STREAM = "!markPrice@arr@1s"
    TICKER_STREAM = "!ticker@arr"

    def __init__(self):
        self._mark_data: Dict[str, dict] = {}
        self._ticker_volume: Dict[str, float] = {}
        self._task: Optional[asyncio.Task] = None  # 웹소켓 수신 태스크

    async def _connect(self):
        url = (
            "wss://fstream.binance.com/stream?streams="
            f"{self.MARK_STREAM}/{self.TICKER_STREAM}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                            stream = payload.get("stream", "")
                            data = payload.get("data")
                            if stream.startswith("!markPrice") and isinstance(data, list):
                                for item in data:
                                    sym = item.get("s")  # 예: BTCUSDT
                                    if not sym or not sym.endswith("USDT"):
                                        continue
                                    ccxt_symbol = f"{sym[:-4]}/USDT:USDT"
                                    self._mark_data[ccxt_symbol] = {
                                        "rate": float(item.get("r", 0)) * 100,
                                        "price": float(item.get("p", 0)),
                                        "next_funding": int(item.get("T", 0)),
                                    }
                            elif stream.startswith("!ticker") and isinstance(data, list):
                                for item in data:
                                    sym = item.get("s")
                                    if not sym or not sym.endswith("USDT"):
                                        continue
                                    ccxt_symbol = f"{sym[:-4]}/USDT:USDT"
                                    # quote asset volume(q) 사용
                                    self._ticker_volume[ccxt_symbol] = float(item.get("q", 0))
                        except Exception as e:
                            logger.error(f"바이낸스 웹소켓 파싱 오류: {e}")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("바이낸스 웹소켓 오류 발생, 재연결 시도")
                        break  # while 루프 바깥에서 재연결

    async def _run(self):
        while True:
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"바이낸스 웹소켓 연결 오류: {e}")
            # 재연결 딜레이
            await asyncio.sleep(5)

    async def start(self):
        """이전에 시작되지 않았다면 웹소켓 모니터링 태스크를 시작"""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def get_snapshot(self, use_spot_filter=True) -> Dict[str, Dict]:
        """현재까지 수집된 데이터를 반환 (start 자동 호출)"""
        await self.start()
        
        if use_spot_filter:
            spot_symbols = await get_binance_spot_symbols()
            logger.info(f"Binance WS 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("Binance WS 현물 필터 비활성 - 모든 선물 코인 포함")
            
        snapshot = {}
        total_futures = len(self._mark_data)
        filtered_out = 0
        
        for sym, mark in self._mark_data.items():
            if use_spot_filter and sym not in spot_symbols:
                filtered_out += 1
                continue  # 현물에 없는 심볼 제외
            snapshot[sym] = {
                "symbol": sym,
                "rate": mark["rate"],
                "volume": self._ticker_volume.get(sym, 0),
                "price": mark["price"],
                "next_funding": mark["next_funding"],
            }
        
        logger.info(f"Binance WS 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(snapshot)}")
        return snapshot

# ===== OKX 웹소켓 모니터 클래스 =====
class OKXWebSocketMonitor:
    """OKX 펀딩비를 웹소켓으로 수신하여 메모리에 보관"""

    def __init__(self):
        self._funding_data: Dict[str, dict] = {}
        self._ticker_data: Dict[str, dict] = {}
        self._task: Optional[asyncio.Task] = None
        self._subscribed_symbols: set = set()

    async def _subscribe_to_channels(self, ws):
        """선물 심볼 목록을 가져와서 채널 구독"""
        try:
            # 선물 심볼 목록 먼저 가져오기
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://www.okx.com/api/v5/public/instruments",
                    params={"instType": "SWAP"}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("code") == "0":
                            # USDT 무기한 선물만 필터링
                            for inst in data["data"]:
                                if inst["settleCcy"] == "USDT" and inst["instId"].endswith("-USDT-SWAP"):
                                    self._subscribed_symbols.add(inst["instId"])

            # 펀딩비 채널 구독 (배치로 처리)
            args = []
            for inst_id in self._subscribed_symbols:
                args.append({
                    "channel": "funding-rate",
                    "instId": inst_id
                })
                args.append({
                    "channel": "tickers",
                    "instId": inst_id
                })
            
            # 50개씩 배치로 구독 (OKX 제한)
            for i in range(0, len(args), 50):
                batch = args[i:i+50]
                subscribe_msg = {
                    "op": "subscribe",
                    "args": batch
                }
                await ws.send_str(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)  # 레이트 리밋 대응
                
            logger.info(f"OKX 웹소켓 {len(self._subscribed_symbols)}개 심볼 구독 완료")
            
        except Exception as e:
            logger.error(f"OKX 웹소켓 구독 오류: {e}")

    async def _connect(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=30) as ws:
                # 채널 구독
                await self._subscribe_to_channels(ws)
                
                # 메시지 수신
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            
                            # 펀딩비 데이터
                            if data.get("arg", {}).get("channel") == "funding-rate":
                                for item in data.get("data", []):
                                    inst_id = item["instId"]
                                    base = inst_id.split("-")[0]
                                    ccxt_symbol = f"{base}/USDT:USDT"
                                    
                                    self._funding_data[ccxt_symbol] = {
                                        "rate": float(item["fundingRate"]) * 100,
                                        "next_funding": int(item["nextFundingTime"])
                                    }
                            
                            # 티커 데이터
                            elif data.get("arg", {}).get("channel") == "tickers":
                                for item in data.get("data", []):
                                    inst_id = item["instId"]
                                    base = inst_id.split("-")[0]
                                    ccxt_symbol = f"{base}/USDT:USDT"
                                    
                                    self._ticker_data[ccxt_symbol] = {
                                        "price": float(item["last"]),
                                        "volume": float(item.get("volCcy24h", 0))
                                    }
                            
                            # ping-pong 처리
                            elif data.get("op") == "ping":
                                pong_msg = {"op": "pong"}
                                await ws.send_str(json.dumps(pong_msg))
                                
                        except Exception as e:
                            logger.error(f"OKX 웹소켓 파싱 오류: {e}")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("OKX 웹소켓 오류 발생, 재연결 시도")
                        break

    async def _run(self):
        while True:
            try:
                await self._connect()
            except Exception as e:
                logger.error(f"OKX 웹소켓 연결 오류: {e}")
            await asyncio.sleep(5)

    async def start(self):
        """웹소켓 모니터링 태스크 시작"""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def get_snapshot(self, use_spot_filter=True) -> Dict[str, Dict]:
        """현재까지 수집된 데이터를 반환"""
        await self.start()
        
        # 데이터가 충분히 모일 때까지 대기 (최초 실행시)
        if not self._funding_data:
            logger.info("OKX 웹소켓 데이터 수집 대기중...")
            await asyncio.sleep(3)
        
        if use_spot_filter:
            spot_symbols = await get_okx_spot_symbols()
            logger.info(f"OKX WS 현물 필터 활성 - 현물 심볼: {len(spot_symbols)}개")
        else:
            spot_symbols = set()
            logger.info("OKX WS 현물 필터 비활성 - 모든 선물 코인 포함")
        
        snapshot = {}
        total_futures = len(self._funding_data)
        filtered_out = 0
        
        for symbol, funding in self._funding_data.items():
            if use_spot_filter and symbol not in spot_symbols:
                filtered_out += 1
                continue
                
            ticker = self._ticker_data.get(symbol, {})
            snapshot[symbol] = {
                "symbol": symbol,
                "rate": funding["rate"],
                "volume": ticker.get("volume", 0),
                "price": ticker.get("price", 0),
                "next_funding": funding["next_funding"]
            }
        
        logger.info(f"OKX WS 결과 - 전체 선물: {total_futures}, 필터링됨: {filtered_out}, 최종: {len(snapshot)}")
        return snapshot

# ===== 전역 모니터 인스턴스 =====
binance_monitor = BinanceWebSocketMonitor()
okx_monitor = OKXWebSocketMonitor()

# ===== 펀딩비 가져오기 통합 함수 =====
async def get_all_funding_rates(exchanges: List[str], use_spot_filter: bool = True) -> Dict[str, Dict]:
    """모든 거래소 펀딩비 가져오기 (바이낸스, OKX는 웹소켓)"""
    results = {}
    tasks = []
    
    # 디버깅: 현물 필터 상태 로깅
    logger.info(f"펀딩비 조회 시작 - 현물 필터 사용: {use_spot_filter}")
    
    if use_spot_filter:
        # 현물 심볼 캐시 상태 확인
        for exchange in ['binance', 'bybit', 'bitget', 'okx']:
            spot_count = len(SPOT_SYMBOL_CACHE.get(exchange, set()))
            logger.info(f"{exchange} 현물 심볼 캐시: {spot_count}개")
    
    if 'bybit' in exchanges:
        tasks.append(('bybit', get_bybit_funding_rates(use_spot_filter)))
    if 'binance' in exchanges:
        tasks.append(('binance', binance_monitor.get_snapshot(use_spot_filter)))
    if 'bitget' in exchanges:
        tasks.append(('bitget', get_bitget_funding_rates(use_spot_filter)))
    if 'okx' in exchanges:
        tasks.append(('okx', okx_monitor.get_snapshot(use_spot_filter)))

    if tasks:
        responses = await asyncio.gather(*[t[1] for t in tasks])
        for idx, (exch, _) in enumerate(tasks):
            results[exch] = responses[idx]
            logger.info(f"{exch} 결과: {len(results[exch])}개 코인")
    
    return results

# Top 5 펀딩비 가져오기
def get_top_funding_rates(funding_rates: Dict[str, dict], positive=True) -> List[Tuple[str, float]]:
    filtered = [(symbol, data['rate']) for symbol, data in funding_rates.items() 
                if (data['rate'] > 0) == positive]
    sorted_rates = sorted(filtered, key=lambda x: abs(x[1]), reverse=True)
    return sorted_rates[:5]

# 거래소 URL 생성
def get_exchange_url(exchange: str, symbol: str) -> str:
    base_symbol = symbol.split('/')[0]
    
    urls = {
        'bybit': f"https://www.bybit.com/trade/usdt/{base_symbol}USDT",
        'binance': f"https://www.binance.com/futures/trade/{base_symbol}USDT",
        'bitget': f"https://www.bitget.com/futures/{base_symbol}USDT",
        'okx': f"https://www.okx.com/trade-swap/{base_symbol.lower()}-usdt-swap"
    }
    return urls.get(exchange, '#')

# 정기 알림 (30분마다)
async def send_periodic_update(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data['chat_id']
    user_settings = get_user_settings(chat_id)
    
    if not user_settings['active']:
        return
    
    message = "📊 *펀딩비 TOP 5 리포트*\n\n"
    
    all_rates = await get_all_funding_rates(user_settings['exchanges'], bool(user_settings['spot_filter']))
    
    for exchange_name, funding_rates in all_rates.items():
        if not funding_rates:
            continue
            
        # 양수 TOP 5
        positive_top = get_top_funding_rates(funding_rates, positive=True)
        message += f"*{exchange_name.upper()} 양수 펀비 TOP5*\n"
        for symbol, rate in positive_top:
            coin_name = symbol.split('/')[0]
            message += f"• {coin_name}: +{rate:.3f}%\n"
        
        # 음수 TOP 5
        negative_top = get_top_funding_rates(funding_rates, positive=False)
        message += f"\n*{exchange_name.upper()} 음수 펀비 TOP5*\n"
        for symbol, rate in negative_top:
            coin_name = symbol.split('/')[0]
            message += f"• {coin_name}: {rate:.3f}%\n"
        
        message += "\n"
    
    await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')

# 임계값 알림 (5분마다)
async def check_threshold_alerts(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data['chat_id']
    user_settings = get_user_settings(chat_id)
    
    if not user_settings['active']:
        return
    
    alerts = []
    all_rates = await get_all_funding_rates(user_settings['exchanges'], bool(user_settings['spot_filter']))
    
    for exchange_name, funding_rates in all_rates.items():
        for symbol, data in funding_rates.items():
            if (abs(data['rate']) >= user_settings['threshold'] and 
                data['volume'] >= user_settings['volume_filter']):
                
                # 다음 펀딩 시간 계산
                if data.get('next_funding'):
                    remaining = max(0, data['next_funding'] / 1000 - time.time())
                    hours = int(remaining // 3600)
                    minutes = int((remaining % 3600) // 60)
                    countdown = f"{hours}시간 {minutes}분"
                else:
                    countdown = "정보 없음"
                
                alert_data = {
                    'exchange': exchange_name,
                    'symbol': symbol,
                    'coin_name': symbol.split('/')[0],  # 코인 이름만 추출
                    'rate': data['rate'],
                    'price': data['price'],
                    'volume': data['volume'],
                    'countdown': countdown
                }
                alerts.append(alert_data)
    
    if alerts:
        message = "🚨 *펀딩비 알림*\n\n"
        
        for alert in alerts:
            url = get_exchange_url(alert['exchange'], alert['symbol'])
            
            message += f"*[{alert['exchange'].upper()}] {alert['coin_name']}*\n"
            message += f"• 펀딩비: {alert['rate']:+.3f}%\n"
            message += f"• 현재가: ${alert['price']:,.2f}\n"
            message += f"• 거래량: ${alert['volume']:,.0f}\n"
            message += f"• 다음 펀딩: {alert['countdown']}\n"
            message += f"• [차트 보기]({url})\n\n"
        
        await context.bot.send_message(
            chat_id=chat_id, 
            text=message, 
            parse_mode='Markdown',
            disable_web_page_preview=True
        )

# 명령어 핸들러
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    init_db()
    save_user_settings(user_id, get_user_settings(user_id))
    
    # 스케줄러에 작업 추가
    job_queue = context.job_queue
    
    # 기존 작업 제거
    current_jobs = job_queue.get_jobs_by_name(f'periodic_{user_id}')
    for job in current_jobs:
        job.schedule_removal()
    
    current_jobs = job_queue.get_jobs_by_name(f'threshold_{user_id}')
    for job in current_jobs:
        job.schedule_removal()
    
    # 30분마다 정기 업데이트
    job_queue.run_repeating(
        send_periodic_update,
        interval=1800,  # 30분
        first=10,
        data={'chat_id': user_id},
        name=f'periodic_{user_id}'
    )
    
    # 5분마다 임계값 체크
    job_queue.run_repeating(
        check_threshold_alerts,
        interval=300,  # 5분
        first=5,
        data={'chat_id': user_id},
        name=f'threshold_{user_id}'
    )
    
    await update.message.reply_text(
        "안녕하세요! 펀딩비 알림 봇입니다.\n\n"
        "사용 가능한 명령어:\n"
        "/settings - 알림 설정 변경\n"
        "/status - 현재 설정 확인\n"
        "/stop - 알림 중지\n"
        "/resume - 알림 재개\n"
        "/now - 현재 펀딩비 확인"
    )

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_settings = get_user_settings(user_id)
    
    # context.user_data에 최신 값이 있으면 우선 사용하되, 안전하게 처리
    effective_spot_filter = user_settings['spot_filter']  # 기본값으로 DB에서 가져온 값 사용
    if hasattr(context, 'user_data') and 'spot_filter' in context.user_data:
        effective_spot_filter = context.user_data['spot_filter']
    elif hasattr(context, 'chat_data') and 'spot_filter' in context.chat_data:
        effective_spot_filter = context.chat_data['spot_filter']
    
    # 안전한 불린 변환
    is_active = effective_spot_filter in [1, '1', True]
    filter_status = "✅ 활성" if is_active else "❌ 비활성"
    
    keyboard = [
        [InlineKeyboardButton("펀딩비 임계값 설정", callback_data='set_threshold')],
        [InlineKeyboardButton("거래량 필터 설정", callback_data='set_volume')],
        [InlineKeyboardButton("거래소 선택", callback_data='set_exchanges')],
        [InlineKeyboardButton(f"현물+선물 필터 ({filter_status})", callback_data='toggle_spot_filter')],
        [InlineKeyboardButton("뒤로", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('설정을 선택하세요:', reply_markup=reply_markup)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = get_user_settings(user_id)
    
    # 상태 메시지 구성
    effective_spot_filter = settings['spot_filter']
    if 'spot_filter' in context.user_data:
        effective_spot_filter = context.user_data['spot_filter']
    elif 'spot_filter' in context.chat_data:
        effective_spot_filter = context.chat_data['spot_filter']

    # exchanges 리스트 정리 (과거 'spot_filter' 오염 제거)
    cleaned_exchanges = [ex for ex in settings['exchanges'] if ex in ['bybit', 'binance', 'bitget', 'okx']]
    if len(cleaned_exchanges) != len(settings['exchanges']):
        # DB 업데이트
        settings['exchanges'] = cleaned_exchanges
        save_user_settings(user_id, settings)

    status_text = (
        "*현재 설정 상태*\n\n"
        f"• 펀딩비 임계값: {settings['threshold']}%\n"
        f"• 거래량 필터: ${settings['volume_filter']:,.0f}\n"
        f"• 활성 거래소: {', '.join(cleaned_exchanges)}\n"
        f"• 현물+선물 필터: {'활성' if effective_spot_filter else '비활성'}\n"
        f"• 알림 상태: {'활성' if settings['active'] else '비활성'}"
    )
    
    # Markdown 파싱 오류 대비
    try:
        await update.message.reply_text(status_text, parse_mode='Markdown')
    except BadRequest as e:
        logger.warning(f"/status 메시지 Markdown 파싱 오류: {e}. Markdown 없이 전송합니다.")
        await update.message.reply_text(status_text)

async def now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """현재 펀딩비 즉시 확인 (최적화 버전)"""
    user_id = update.effective_user.id
    user_settings = get_user_settings(user_id)
    
    # context에서 최신 값 가져오기 (안전하게 처리)
    effective_spot_filter = user_settings['spot_filter']  # 기본값
    if hasattr(context, 'user_data') and 'spot_filter' in context.user_data:
        effective_spot_filter = context.user_data['spot_filter']
    elif hasattr(context, 'chat_data') and 'spot_filter' in context.chat_data:
        effective_spot_filter = context.chat_data['spot_filter']
    
    # 로딩 메시지
    loading_msg = await update.message.reply_text("⏳ 펀딩비 데이터를 가져오는 중...")
    
    try:
        # 안전한 불린 변환 및 디버깅 정보
        is_filter_active = effective_spot_filter in [1, '1', True]
        filter_status = f"현물+선물 필터: {'활성' if is_filter_active else '비활성'} (값: {effective_spot_filter})"
        
        message = f"📊 *현재 펀딩비 TOP 5*\n({filter_status})\n\n"
        
        # 모든 거래소 데이터 병렬로 가져오기 - 필터링 여부를 명확히 전달
        all_rates = await get_all_funding_rates(user_settings['exchanges'], is_filter_active)
        
        total_coins_found = 0
        for exchange_name, funding_rates in all_rates.items():
            total_coins_found += len(funding_rates)
            
            if not funding_rates:
                message += f"*{exchange_name.upper()}*\n⚠️ 데이터를 가져올 수 없습니다.\n\n"
                continue
            
            message += f"━━━ *{exchange_name.upper()}* ({len(funding_rates)}개 코인) ━━━\n\n"
            
            # 양수 TOP 5
            positive_rates = [(symbol, data) for symbol, data in funding_rates.items() 
                            if data['rate'] > 0]
            positive_sorted = sorted(positive_rates, key=lambda x: x[1]['rate'], reverse=True)[:5]
            
            if positive_sorted:
                message += "📈 *양수 펀딩비 TOP 5*\n"
                for symbol, data in positive_sorted:
                    coin_name = symbol.split('/')[0]  # IOST/USDT:USDT → IOST
                    message += f"• {coin_name}: +{data['rate']:.4f}%\n"
                message += "\n"
            
            # 음수 TOP 5
            negative_rates = [(symbol, data) for symbol, data in funding_rates.items() 
                            if data['rate'] < 0]
            negative_sorted = sorted(negative_rates, key=lambda x: x[1]['rate'])[:5]
            
            if negative_sorted:
                message += "📉 *음수 펀딩비 TOP 5*\n"
                for symbol, data in negative_sorted:
                    coin_name = symbol.split('/')[0]  # IOST/USDT:USDT → IOST
                    message += f"• {coin_name}: {data['rate']:.4f}%\n"
                message += "\n"
            
            # 양수/음수가 모두 없는 경우
            if not positive_sorted and not negative_sorted:
                message += "• 데이터가 없습니다.\n\n"
        
        # 총 코인 개수 정보 추가
        message += f"\n💡 총 {total_coins_found}개 코인 발견"
        
        await loading_msg.edit_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"/now 명령어 오류: {e}")
        await loading_msg.edit_text("❌ 데이터를 가져오는 중 오류가 발생했습니다.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = get_user_settings(user_id)
    settings['active'] = 0
    save_user_settings(user_id, settings)
    
    await update.message.reply_text("알림이 중지되었습니다.")

async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = get_user_settings(user_id)
    settings['active'] = 1
    save_user_settings(user_id, settings)
    
    await update.message.reply_text("알림이 재개되었습니다.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if query.data == 'set_threshold':
        await query.edit_message_text(
            "펀딩비 임계값을 입력하세요 (예: 0.1):\n"
            "현재 설정값 이상의 펀딩비만 알림을 받습니다."
        )
        context.user_data['waiting_for'] = 'threshold'
    
    elif query.data == 'set_volume':
        await query.edit_message_text(
            "거래량 필터를 입력하세요 (예: 1000000):\n"
            "설정값 이상의 거래량을 가진 코인만 알림을 받습니다."
        )
        context.user_data['waiting_for'] = 'volume'
    
    elif query.data == 'set_exchanges':
        settings = get_user_settings(user_id)
        keyboard = [
            [InlineKeyboardButton(
                f"{'✅' if 'bybit' in settings['exchanges'] else '❌'} Bybit", 
                callback_data='toggle_bybit'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'binance' in settings['exchanges'] else '❌'} Binance", 
                callback_data='toggle_binance'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'bitget' in settings['exchanges'] else '❌'} Bitget", 
                callback_data='toggle_bitget'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'okx' in settings['exchanges'] else '❌'} OKX", 
                callback_data='toggle_okx'
            )],
            [InlineKeyboardButton("완료", callback_data='done_exchanges')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text('거래소를 선택하세요:', reply_markup=reply_markup)
    
    elif query.data == 'toggle_spot_filter':
        settings = get_user_settings(user_id)
        
        # context에서 최신 값 우선적으로 가져오기
        current_val = settings.get('spot_filter', 1)  # DB 기본값
        if hasattr(context, 'user_data') and 'spot_filter' in context.user_data:
            current_val = context.user_data['spot_filter']
        elif hasattr(context, 'chat_data') and 'spot_filter' in context.chat_data:
            current_val = context.chat_data['spot_filter']
        
        logger.info(f"토글 전 spot_filter 값: {current_val} (타입: {type(current_val)})")
        
        # 더 안전한 토글 방식
        if current_val in [1, '1', True]:
            new_val = 0
        else:
            new_val = 1
            
        settings['spot_filter'] = new_val

        # 과거 버그로 exchanges 리스트에 'spot_filter' 항목이 포함된 경우 제거
        if 'spot_filter' in settings.get('exchanges', []):
            settings['exchanges'] = [ex for ex in settings['exchanges'] if ex != 'spot_filter']
            logger.info("exchanges 리스트에서 잘못된 'spot_filter' 항목을 제거했습니다.")

        logger.info(f"토글 후 spot_filter 값: {new_val}")
        
        save_user_settings(user_id, settings)
        
        # 메모리에도 즉시 반영
        context.user_data['spot_filter'] = new_val
        context.chat_data['spot_filter'] = new_val
        
        # 설정창 닫고 상태 메시지 표시
        filter_status = "활성화" if new_val else "비활성화"
        await query.edit_message_text(f"현물+선물 필터가 {filter_status} 되었습니다.")
    
    elif query.data.startswith('toggle_'):
        exchange = query.data.replace('toggle_', '')
        settings = get_user_settings(user_id)
        
        if exchange in settings['exchanges']:
            settings['exchanges'].remove(exchange)
        else:
            settings['exchanges'].append(exchange)
        
        save_user_settings(user_id, settings)
        
        # 버튼 업데이트
        keyboard = [
            [InlineKeyboardButton(
                f"{'✅' if 'bybit' in settings['exchanges'] else '❌'} Bybit", 
                callback_data='toggle_bybit'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'binance' in settings['exchanges'] else '❌'} Binance", 
                callback_data='toggle_binance'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'bitget' in settings['exchanges'] else '❌'} Bitget", 
                callback_data='toggle_bitget'
            )],
            [InlineKeyboardButton(
                f"{'✅' if 'okx' in settings['exchanges'] else '❌'} OKX", 
                callback_data='toggle_okx'
            )],
            [InlineKeyboardButton("완료", callback_data='done_exchanges')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text('거래소를 선택하세요:', reply_markup=reply_markup)
    
    elif query.data == 'done_exchanges':
        await query.edit_message_text("거래소 설정이 완료되었습니다.")
    
    elif query.data == 'back':
        await query.edit_message_text("설정이 취소되었습니다.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'waiting_for' not in context.user_data:
        return
    
    user_id = update.effective_user.id
    settings = get_user_settings(user_id)
    
    if context.user_data['waiting_for'] == 'threshold':
        try:
            threshold = float(update.message.text)
            settings['threshold'] = threshold
            save_user_settings(user_id, settings)
            await update.message.reply_text(f"펀딩비 임계값이 {threshold}%로 설정되었습니다.")
        except ValueError:
            await update.message.reply_text("올바른 숫자를 입력해주세요.")
        del context.user_data['waiting_for']
    
    elif context.user_data['waiting_for'] == 'volume':
        try:
            volume = float(update.message.text)
            settings['volume_filter'] = volume
            save_user_settings(user_id, settings)
            await update.message.reply_text(f"거래량 필터가 ${volume:,.0f}로 설정되었습니다.")
        except ValueError:
            await update.message.reply_text("올바른 숫자를 입력해주세요.")
        del context.user_data['waiting_for']

# 메인 함수
def main():
    # 봇 토큰 설정
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN이 설정되지 않았습니다.")
    
    # post_init 콜백에서 명령어 목록 설정
    async def post_init(app: Application):
        commands = [
            BotCommand("start", "봇 시작 / 도움말"),
            BotCommand("settings", "알림 설정 변경"),
            BotCommand("status", "현재 설정 확인"),
            BotCommand("now", "현재 펀딩비 확인"),
            BotCommand("stop", "알림 중지"),
            BotCommand("resume", "알림 재개"),
        ]
        try:
            await app.bot.set_my_commands(commands)
            logger.info("명령어 목록이 설정되었습니다.")
        except Exception as e:
            logger.error(f"명령어 목록 설정 실패: {e}")

    # Application 생성
    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )
    
    # 핸들러 추가
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("now", now))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("resume", resume))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # 봇 실행
    application.run_polling()

if __name__ == '__main__':
    main()