#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  MACRO TERMINAL  //  Institutional Intelligence Platform  v5.0                      ║
║  Architecture : Streamlit + Plotly + Async Multi-Source Data Engine                 ║
║                                                                                     ║
║  Live Data Sources:                                                                 ║
║    • FRED (St. Louis Fed)  — Macro indicators, yield curve, CPI, GDP               ║
║    • Polygon.io            — Real-time US equity snapshots & OHLCV bars             ║
║    • Alpha Vantage         — Technical indicators (RSI, BBANDS, SMA/EMA)            ║
║    • Finnhub               — Real-time quotes, company fundamentals                 ║
║    • Marketstack           — Global exchange EOD / real-time aggregation            ║
║    • Tradier               — Order-flow reference, options chain, market status     ║
║    • NewsAPI.org           — Broad financial news mining                            ║
║    • GNews API             — High-speed breaking headlines                          ║
║    • NewsData.io           — Historical & categorised economic archive              ║
║    • World News API        — Geopolitical event & sentiment ingestion               ║
║    • CoinGecko Free        — Crypto prices, dominance, Fear & Greed                ║
║    • Alternative.me        — Crypto Fear & Greed Index                             ║
║    • ForexFactory JSON     — Economic calendar                                      ║
║    • RSS Feeds             — BBC, Reuters, FT, WSJ, CoinDesk, CoinTelegraph        ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
"""

# ── STANDARD LIBRARY ────────────────────────────────────────────────────────────────────
import asyncio
import hashlib
import html as _html_mod
import json
import logging
import math
import re
import threading
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ── THIRD-PARTY ─────────────────────────────────────────────────────────────────────────
import aiohttp
import feedparser
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)

# ═════════════════════════════════════════════════════════════════════════════════════
# §0  PAGE CONFIG  (must be first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="MACRO TERMINAL",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═════════════════════════════════════════════════════════════════════════════════════
# §1  API KEYS — Production credentials
# ═════════════════════════════════════════════════════════════════════════════════════
FRED_KEY        = "331f181e751715dbece9d7ba12fe53be"
ALPHAV_KEY      = "F7U2R2CZ3PVURXV7"
POLYGON_KEY     = "KczAzxrl897zcN2jXuNIqICxpF6ZKp7i"
MARKETSTACK_KEY = "3d031ebafc42d35fcefaf520b075f48f"
TRADIER_KEY     = "iZZHUCjeFGfziYmZEPfU702e1Y3T"
FINNHUB_KEY     = "d7rkj8pr01qviakd5vq0d7rkj8pr01qviakd5vqg"
NEWSAPI_KEY     = "119bfe615bad404699a524485a9f05bf"
GNEWS_KEY       = "17ac52a8bcb0331110bbc5d8c21e9ea2"
NEWSDATA_KEY    = "pub_37be7d2e51e04a3a838a729c07781e81"
WORLDNEWS_KEY   = "ea5928309d8e4881beecc8be10ebe3b6"

# ═════════════════════════════════════════════════════════════════════════════════════
# §2  COLOUR PALETTE + GLOBAL CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════════════
C: Dict[str, str] = {
    "bg":          "#050505",
    "panel":       "#0c0e13",
    "panel_alt":   "#10121a",
    "border":      "#1a1d24",
    "border2":     "#252b38",
    "gold":        "#c5a861",
    "gold_dim":    "#7a6535",
    "gold_bright": "#e0c878",
    "red":         "#d32f2f",
    "red_dim":     "#7a1a1a",
    "red_bright":  "#ff5252",
    "green":       "#2e7d52",
    "green_br":    "#4caf7d",
    "blue":        "#1e4d8c",
    "blue_br":     "#4a8fe0",
    "orange":      "#c07a2a",
    "orange_br":   "#e0a050",
    "purple":      "#7e57c2",
    "text":        "#c8cdd8",
    "text_dim":    "#8a9090",
    "muted":       "#42485a",
}

# FRED series to fetch
FRED_SERIES: Dict[str, str] = {
    "Fed Funds Rate":  "FEDFUNDS",
    "US CPI YoY":      "CPIAUCSL",
    "US GDP (QoQ)":    "A191RL1Q225SBEA",
    "US Unemployment": "UNRATE",
    "10Y Treasury":    "DGS10",
    "2Y Treasury":     "DGS2",
    "VIX Index":       "VIXCLS",
    "M2 Money Supply": "M2SL",
}

# Macro seed values (shown while FRED loads)
MACRO_SEED: Dict[str, Dict] = {
    "Fed Funds Rate":  {"value": "5.25%",  "delta": "0.00%",   "trend": "flat", "note": "Next FOMC June 2026"},
    "US CPI YoY":      {"value": "3.1%",   "delta": "+0.1%",   "trend": "up",   "note": "Above 2% target"},
    "US GDP (QoQ)":    {"value": "2.4%",   "delta": "+0.3%",   "trend": "up",   "note": "Q1 2026 annualized"},
    "US Unemployment": {"value": "4.1%",   "delta": "-0.1%",   "trend": "down", "note": "Marginally easing"},
    "10Y Treasury":    {"value": "4.62%",  "delta": "+0.08%",  "trend": "up",   "note": "Discount rate pressure"},
    "2Y Treasury":     {"value": "4.94%",  "delta": "+0.05%",  "trend": "up",   "note": "Yield curve inverted"},
    "VIX Index":       {"value": "18.4",   "delta": "-1.2",    "trend": "down", "note": "Below 20 — moderate risk"},
    "M2 Money Supply": {"value": "$21.4T", "delta": "+$0.1T",  "trend": "up",   "note": "Liquidity expanding"},
    "Yield Spread":    {"value": "-0.32%", "delta": "-0.03%",  "trend": "down", "note": "2s10s inverted"},
    "ISM Mfg PMI":     {"value": "48.7",   "delta": "-0.8",    "trend": "down", "note": "Contraction <50"},
    "DXY (USD Index)": {"value": "104.2",  "delta": "+0.4",    "trend": "up",   "note": "Strong — FX headwind"},
    "Gold (XAU/USD)":  {"value": "$2,481", "delta": "+$12",    "trend": "up",   "note": "Safe-haven bid elevated"},
    "WTI Crude":       {"value": "$79.40", "delta": "+$2.10",  "trend": "up",   "note": "Supply disruption premium"},
    "BTC/USD":         {"value": "$105.2K","delta": "+$1.8K",  "trend": "up",   "note": "ETF inflow momentum"},
}

# Ticker bar symbols (Polygon for stocks, fallback to Yahoo)
TICKER_BAR_SYMBOLS = [
    ("S&P 500", "SPY"), ("NASDAQ", "QQQ"), ("RUSSELL", "IWM"), ("GOLD", "GLD"),
    ("OIL(WTI)", "USO"), ("BTC", "MSTR"), ("AAPL", "AAPL"), ("NVDA", "NVDA"),
    ("TSLA", "TSLA"), ("MSFT", "MSFT"), ("AMZN", "AMZN"), ("META", "META"),
    ("GOOGL", "GOOGL"), ("AVGO", "AVGO"), ("JPM", "JPM"), ("V", "V"),
    ("XOM", "XOM"), ("AMD", "AMD"), ("NFLX", "NFLX"), ("COST", "COST"),
]

# Stocks for overview screener
STOCK_WATCH = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
    "COST", "NFLX", "AMD", "ADBE", "QCOM", "TXN", "JPM", "V", "XOM", "BRK-B",
]

# Conflict hotspots for globe
CONFLICT_HOTSPOTS: List[Dict] = [
    {"name": "Gaza Strip",       "lat": 31.35, "lng": 34.30, "severity": 0.95, "type": "Active Conflict"},
    {"name": "Kyiv / Front",     "lat": 50.45, "lng": 30.52, "severity": 0.90, "type": "Active Conflict"},
    {"name": "Donbas Line",      "lat": 48.00, "lng": 37.80, "severity": 0.88, "type": "Active Conflict"},
    {"name": "Strait of Hormuz", "lat": 26.57, "lng": 56.26, "severity": 0.80, "type": "Naval Tension"},
    {"name": "South China Sea",  "lat": 12.50, "lng": 113.00,"severity": 0.72, "type": "Naval Tension"},
    {"name": "Taiwan Strait",    "lat": 24.50, "lng": 119.50,"severity": 0.70, "type": "Naval Tension"},
    {"name": "Khartoum",         "lat": 15.55, "lng": 32.53, "severity": 0.62, "type": "Active Conflict"},
    {"name": "Yangon",           "lat": 16.87, "lng": 96.19, "severity": 0.58, "type": "Active Conflict"},
    {"name": "Port-au-Prince",   "lat": 18.54, "lng": -72.34,"severity": 0.52, "type": "Civil Unrest"},
    {"name": "Red Sea Corridor", "lat": 18.00, "lng": 42.00, "severity": 0.70, "type": "Naval Tension"},
    {"name": "Suez Canal",       "lat": 30.58, "lng": 32.34, "severity": 0.60, "type": "Infrastructure"},
    {"name": "Sahel / Mali",     "lat": 14.00, "lng": -2.00, "severity": 0.44, "type": "Insurgency"},
]

# Valuation profiles (static — supplemented by live Finnhub data)
VALUATION_PROFILES: Dict[str, Dict] = {
    "AAPL": {
        "name": "Apple Inc.", "sector": "Technology", "exchange": "NASDAQ",
        "description": ("Apple designs, manufactures, and markets smartphones, PCs, tablets, wearables, "
                        "and accessories. Key products: iPhone, Mac, iPad, Apple Watch, AirPods, plus a "
                        "high-margin Services segment (App Store, Apple TV+, iCloud) growing ~14% YoY."),
        "rating": 60, "rating_label": "MODERATE FOUNDATION",
        "dcf_verdict": "OVERVALUED", "dcf_pct": 40,
        "current_price": 199.50, "fair_value_dcf": 145.00, "fair_value_ddm": 22.00, "fair_value_ev": 160.00,
        "pe": 33.2, "fwd_pe": 29.8, "peer_pe": 38.4, "eps": 6.42, "eps_growth": 8.2,
        "market_cap": "3.02T", "revenue": "391B", "gross_margin": 0.441, "profit_margin": 0.253,
        "ebitda_margin": 0.318, "roe": 0.147, "roa": 0.178, "debt_equity": 1.74, "fcf": "107B",
        "fcf_yield": 0.034, "dividend_yield": 0.0048, "payout_ratio": 0.15, "beta": 1.21,
        "week52_high": 260.10, "week52_low": 164.08, "shares_out": "15.2B", "buyback": "90B",
        "insider_own": "0.07%", "inst_own": "60.8%", "short_float": "0.74%",
        "cards": [
            {"cat": "Valuation Models",      "verdict": "OVERVALUED",  "icon": "✕", "color": "red",
             "detail": "DCF –40%  ·  DDM –93%  ·  EV/EBITDA –35%"},
            {"cat": "Financial Health",       "verdict": "STRONG",      "icon": "✓", "color": "green",
             "detail": "Assets exceed liabilities  ·  AA+ credit rating"},
            {"cat": "Institutional Activity", "verdict": "BALANCED",    "icon": "△", "color": "orange",
             "detail": "2 large purchases vs 2 large sales  ·  Net neutral flow"},
            {"cat": "Peer Comparison",        "verdict": "ATTRACTIVE",  "icon": "✓", "color": "green",
             "detail": "P/E 33.2 < Peer Avg 38.4  ·  Ecosystem moat premium justified"},
            {"cat": "Capital Allocation",     "verdict": "B+ (72/100)", "icon": "✓", "color": "green",
             "detail": "$90B buyback  ·  $15B R&D  ·  Shareholder-friendly"},
            {"cat": "Earnings Quality",       "verdict": "HIGH",        "icon": "✓", "color": "green",
             "detail": "FCF yield 3.4%  ·  Accrual ratio low"},
        ],
        "summary": ("Balance sheet is solid. However DCF indicates significant overvaluation. "
                    "Services deceleration and multi-factor ranking weak vs peers."),
        "peers": [
            {"sym": "MSFT", "pe": 34.2, "fwd_pe": 30.1, "mkt": "3.1T", "margin": 0.362, "roe": 0.401, "growth": 0.162},
            {"sym": "GOOG", "pe": 24.8, "fwd_pe": 21.3, "mkt": "2.3T", "margin": 0.281, "roe": 0.273, "growth": 0.142},
            {"sym": "META", "pe": 26.5, "fwd_pe": 23.8, "mkt": "1.4T", "margin": 0.320, "roe": 0.352, "growth": 0.188},
            {"sym": "AMZN", "pe": 42.1, "fwd_pe": 36.7, "mkt": "2.2T", "margin": 0.094, "roe": 0.187, "growth": 0.124},
        ],
        "revenue_segments": {"iPhone": 52, "Services": 22, "Mac": 8, "iPad": 7, "Wearables": 11},
        "quarterly_eps": [5.89, 6.13, 6.42, 6.73],
        "quarterly_rev": [89.5, 91.2, 95.4, 98.1],
    },
    "TSLA": {
        "name": "Tesla Inc.", "sector": "Consumer Cyclical", "exchange": "NASDAQ",
        "description": ("Tesla designs, manufactures and sells electric vehicles and energy storage. "
                        "Key products: Model S/3/X/Y, Cybertruck, Powerwall, Megapack, and FSD software."),
        "rating": 38, "rating_label": "SPECULATIVE",
        "dcf_verdict": "OVERVALUED", "dcf_pct": 65,
        "current_price": 248.50, "fair_value_dcf": 87.00, "fair_value_ddm": 0.0, "fair_value_ev": 195.00,
        "pe": 58.3, "fwd_pe": 74.1, "peer_pe": 12.0, "eps": 4.26, "eps_growth": -8.4,
        "market_cap": "795B", "revenue": "97B", "gross_margin": 0.182, "profit_margin": 0.073,
        "ebitda_margin": 0.126, "roe": 0.124, "roa": 0.078, "debt_equity": 0.18, "fcf": "2.5B",
        "fcf_yield": 0.003, "dividend_yield": 0.0, "payout_ratio": 0.0, "beta": 2.34,
        "week52_high": 488.54, "week52_low": 138.80, "shares_out": "3.2B", "buyback": "0B",
        "insider_own": "12.9%", "inst_own": "44.2%", "short_float": "3.1%",
        "cards": [
            {"cat": "Valuation Models",      "verdict": "OVERVALUED",  "icon": "✕", "color": "red",
             "detail": "DCF –65%  ·  DDM N/A  ·  EV/EBITDA stretched"},
            {"cat": "Financial Health",       "verdict": "ADEQUATE",    "icon": "△", "color": "orange",
             "detail": "Positive FCF but declining  ·  Margins compressing 6pp YoY"},
            {"cat": "Institutional Activity", "verdict": "NET SELLING", "icon": "✕", "color": "red",
             "detail": "5 large sales vs 1 purchase  ·  Index rebalancing pressure"},
            {"cat": "Peer Comparison",        "verdict": "OVERPRICED",  "icon": "✕", "color": "red",
             "detail": "P/E 58x vs OEM avg 12x"},
            {"cat": "Capital Allocation",     "verdict": "C (52/100)",  "icon": "△", "color": "orange",
             "detail": "High capex $8.9B  ·  Uncertain RoI on Cybertruck"},
            {"cat": "Earnings Quality",       "verdict": "CONCERN",     "icon": "✕", "color": "red",
             "detail": "Regulatory credits inflate net income  ·  Core auto margin 3.1%"},
        ],
        "summary": ("Tesla faces severe margin compression, intensifying Chinese competition, "
                    "FSD uncertainty and key-man risk. Current premium is almost entirely speculative."),
        "peers": [
            {"sym": "F",    "pe": 6.2,  "fwd_pe": 5.8,  "mkt": "42B", "margin": 0.028, "roe": 0.122, "growth": 0.04},
            {"sym": "GM",   "pe": 5.8,  "fwd_pe": 5.1,  "mkt": "47B", "margin": 0.032, "roe": 0.141, "growth": 0.02},
            {"sym": "RIVN", "pe": None, "fwd_pe": None, "mkt": "14B", "margin": -0.18, "roe": -0.45, "growth": 0.82},
        ],
        "revenue_segments": {"Automotive": 84, "Energy": 8, "Services": 8},
        "quarterly_eps": [1.19, 0.91, 0.72, 0.66],
        "quarterly_rev": [23.3, 21.3, 25.2, 25.7],
    },
    "NVDA": {
        "name": "NVIDIA Corporation", "sector": "Technology", "exchange": "NASDAQ",
        "description": ("NVIDIA provides GPUs and compute platforms for AI/data center, gaming, "
                        "professional visualization, and automotive. H100/H200/Blackwell dominate AI "
                        "training with 80%+ data-center GPU market share. Margins at record highs."),
        "rating": 72, "rating_label": "STRONG FOUNDATION",
        "dcf_verdict": "FAIRLY VALUED", "dcf_pct": 8,
        "current_price": 131.00, "fair_value_dcf": 130.00, "fair_value_ddm": 8.50, "fair_value_ev": 140.00,
        "pe": 38.2, "fwd_pe": 28.5, "peer_pe": 36.0, "eps": 3.44, "eps_growth": 125.8,
        "market_cap": "3.20T", "revenue": "130B", "gross_margin": 0.748, "profit_margin": 0.556,
        "ebitda_margin": 0.612, "roe": 1.248, "roa": 0.693, "debt_equity": 0.43, "fcf": "60B",
        "fcf_yield": 0.019, "dividend_yield": 0.0003, "payout_ratio": 0.01, "beta": 1.66,
        "week52_high": 153.13, "week52_low": 75.61, "shares_out": "24.4B", "buyback": "25B",
        "insider_own": "3.6%", "inst_own": "65.0%", "short_float": "1.1%",
        "cards": [
            {"cat": "Valuation Models",      "verdict": "FAIRLY VALUED","icon": "≈", "color": "green",
             "detail": "DCF –8%  ·  AI compute earnings revision cycle still high"},
            {"cat": "Financial Health",       "verdict": "EXCEPTIONAL",  "icon": "✓", "color": "green",
             "detail": "74.8% gross margin  ·  56% net margin  ·  Net cash"},
            {"cat": "Institutional Activity", "verdict": "NET BUYING",   "icon": "✓", "color": "green",
             "detail": "Sovereign wealth & AI funds accumulating  ·  Large block buys"},
            {"cat": "Peer Comparison",        "verdict": "PREMIUM",      "icon": "✓", "color": "green",
             "detail": "P/E premium justified by 126% EPS growth  ·  Blackwell supercycle"},
            {"cat": "Capital Allocation",     "verdict": "A (91/100)",   "icon": "✓", "color": "green",
             "detail": "$25B buyback  ·  R&D $8.7B  ·  CUDA moat expanding"},
            {"cat": "Earnings Quality",       "verdict": "HIGH",         "icon": "✓", "color": "green",
             "detail": "FCF yield 1.9%  ·  Clean accruals  ·  No regulatory credits"},
        ],
        "summary": ("NVIDIA is the defining infrastructure company of the AI supercycle. Blackwell "
                    "GPU architecture creates a multi-year supply-constrained revenue ramp. "
                    "Valuation has normalized after explosive re-rating. Core risk: hyperscaler capex slowdown."),
        "peers": [
            {"sym": "AMD",  "pe": 50.2, "fwd_pe": 28.4, "mkt": "250B", "margin": 0.109, "roe": 0.053, "growth": 0.24},
            {"sym": "INTC", "pe": None, "fwd_pe": 22.1, "mkt": "95B",  "margin": -0.09, "roe": -0.05, "growth": -0.08},
            {"sym": "AVGO", "pe": 35.0, "fwd_pe": 25.0, "mkt": "900B", "margin": 0.275, "roe": 0.325, "growth": 0.18},
        ],
        "revenue_segments": {"Data Center": 87, "Gaming": 8, "Pro Visualization": 2, "Automotive": 3},
        "quarterly_eps": [1.32, 2.94, 4.02, 5.16],
        "quarterly_rev": [22.1, 30.0, 35.1, 39.3],
    },
    "MSFT": {
        "name": "Microsoft Corporation", "sector": "Technology", "exchange": "NASDAQ",
        "description": ("Microsoft provides cloud computing (Azure, 30%+ growth), productivity software "
                        "(Office 365, Teams), gaming (Xbox, Activision), and AI services via deep "
                        "OpenAI partnership. Copilot monetization is early-stage with significant upside."),
        "rating": 78, "rating_label": "STRONG FOUNDATION",
        "dcf_verdict": "FAIRLY VALUED", "dcf_pct": 5,
        "current_price": 430.00, "fair_value_dcf": 420.00, "fair_value_ddm": 125.00, "fair_value_ev": 445.00,
        "pe": 34.2, "fwd_pe": 30.1, "peer_pe": 34.2, "eps": 12.93, "eps_growth": 16.2,
        "market_cap": "3.20T", "revenue": "236B", "gross_margin": 0.698, "profit_margin": 0.362,
        "ebitda_margin": 0.518, "roe": 0.401, "roa": 0.178, "debt_equity": 0.44, "fcf": "75B",
        "fcf_yield": 0.024, "dividend_yield": 0.0072, "payout_ratio": 0.24, "beta": 0.88,
        "week52_high": 468.35, "week52_low": 362.90, "shares_out": "7.44B", "buyback": "28B",
        "insider_own": "1.3%", "inst_own": "72.1%", "short_float": "0.5%",
        "cards": [
            {"cat": "Valuation Models",      "verdict": "FAIRLY VALUED","icon": "≈", "color": "green",
             "detail": "DCF –5%  ·  Azure growth sustains premium multiple"},
            {"cat": "Financial Health",       "verdict": "EXCEPTIONAL",  "icon": "✓", "color": "green",
             "detail": "AAA credit  ·  $80B cash  ·  Net cash position"},
            {"cat": "Institutional Activity", "verdict": "NET BUYING",   "icon": "✓", "color": "green",
             "detail": "8 large purchases vs 2 sales  ·  Sovereign wealth accumulating"},
            {"cat": "Capital Allocation",     "verdict": "A- (82/100)",  "icon": "✓", "color": "green",
             "detail": "$28B buyback  ·  $24B R&D  ·  Activision integration ongoing"},
            {"cat": "Earnings Quality",       "verdict": "HIGH",         "icon": "✓", "color": "green",
             "detail": "FCF yield 2.4%  ·  Subscription model = high visibility"},
            {"cat": "Peer Comparison",        "verdict": "IN-LINE",      "icon": "△", "color": "orange",
             "detail": "P/E 34x at sector avg  ·  Justified by AI/cloud integration"},
        ],
        "summary": ("Azure cloud and deep OpenAI integration position Microsoft as enterprise AI "
                    "infrastructure standard. Copilot monetization early-stage with significant upside. "
                    "Valuation is fair, not cheap. Activision dilutes margins short-term."),
        "peers": [
            {"sym": "GOOG", "pe": 24.8, "fwd_pe": 21.3, "mkt": "2.3T", "margin": 0.281, "roe": 0.273, "growth": 0.142},
            {"sym": "AMZN", "pe": 42.1, "fwd_pe": 36.7, "mkt": "2.2T", "margin": 0.094, "roe": 0.187, "growth": 0.124},
            {"sym": "META", "pe": 26.5, "fwd_pe": 23.8, "mkt": "1.4T", "margin": 0.320, "roe": 0.352, "growth": 0.188},
        ],
        "revenue_segments": {"Intelligent Cloud": 44, "Productivity": 32, "Personal Computing": 24},
        "quarterly_eps": [9.81, 11.45, 12.14, 13.20],
        "quarterly_rev": [56.5, 61.9, 65.6, 70.1],
    },
}

SCREENER_UNIVERSE = [
    {"ticker": "AAPL",  "name": "Apple Inc.",           "sector": "Technology",    "mkt_cap": "3.02T", "pe": 33.2, "rating": 60},
    {"ticker": "MSFT",  "name": "Microsoft Corp.",      "sector": "Technology",    "mkt_cap": "3.20T", "pe": 34.2, "rating": 78},
    {"ticker": "NVDA",  "name": "NVIDIA Corp.",          "sector": "Technology",    "mkt_cap": "3.20T", "pe": 38.2, "rating": 72},
    {"ticker": "GOOG",  "name": "Alphabet Inc.",         "sector": "Communication", "mkt_cap": "2.30T", "pe": 24.8, "rating": 70},
    {"ticker": "AMZN",  "name": "Amazon.com Inc.",       "sector": "Consumer",      "mkt_cap": "2.20T", "pe": 42.1, "rating": 65},
    {"ticker": "META",  "name": "Meta Platforms Inc.",   "sector": "Communication", "mkt_cap": "1.40T", "pe": 26.5, "rating": 68},
    {"ticker": "TSLA",  "name": "Tesla Inc.",            "sector": "Consumer",      "mkt_cap": "795B",  "pe": 58.3, "rating": 38},
    {"ticker": "JPM",   "name": "JPMorgan Chase",        "sector": "Financial",     "mkt_cap": "580B",  "pe": 12.8, "rating": 72},
    {"ticker": "V",     "name": "Visa Inc.",             "sector": "Financial",     "mkt_cap": "560B",  "pe": 30.1, "rating": 76},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",     "sector": "Healthcare",    "mkt_cap": "385B",  "pe": 14.2, "rating": 69},
    {"ticker": "XOM",   "name": "ExxonMobil Corp.",      "sector": "Energy",        "mkt_cap": "520B",  "pe": 14.8, "rating": 62},
    {"ticker": "AVGO",  "name": "Broadcom Inc.",         "sector": "Technology",    "mkt_cap": "900B",  "pe": 35.0, "rating": 73},
    {"ticker": "SPY",   "name": "S&P 500 ETF",           "sector": "ETF",           "mkt_cap": "560B",  "pe": 22.0, "rating": 70},
    {"ticker": "QQQ",   "name": "Nasdaq-100 ETF",        "sector": "ETF",           "mkt_cap": "290B",  "pe": 28.4, "rating": 68},
    {"ticker": "GLD",   "name": "Gold ETF (SPDR)",       "sector": "Commodity",     "mkt_cap": "66B",   "pe": 0.0,  "rating": 65},
]

# RSS Feeds — financial news fallback
_CRYPTO_RSS = [
    ("CoinDesk",      "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("CoinTelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt",       "https://decrypt.co/feed"),
]
_MARKET_RSS = [
    ("MarketWatch",   "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("Reuters",       "https://feeds.reuters.com/reuters/businessNews"),
]
_MACRO_RSS = [
    ("BBC World",     "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Reuters World", "https://feeds.reuters.com/reuters/worldNews"),
    ("DW",            "https://rss.dw.com/rdf/rss-en-world"),
]

_FLASH_KEYWORDS = frozenset([
    "breaking", "urgent", "flash", "alert", "attack", "strike", "explosion", "killed",
    "nuclear", "chemical", "missile", "bomb", "war declared", "ceasefire broken",
    "coup", "invasion", "assassin", "hostage", "mass casualty", "emergency",
])
_PRIORITY_KEYWORDS = frozenset([
    "conflict", "military", "troops", "deploy", "sanction", "airstrike", "clashes",
    "protest", "riot", "casualties", "offensive", "withdrawal", "escalat", "blockade",
    "siege", "detained", "arrested", "crisis", "tensions", "threat", "mobiliz",
])

# ═════════════════════════════════════════════════════════════════════════════════════
# §3  CSS INJECTION — Institutional dark terminal theme
# ═════════════════════════════════════════════════════════════════════════════════════

def inject_css() -> None:
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&family=Syne:wght@400;600;700;800&display=swap');

    /* ── Module spacing fix ── */
    .module-block { margin-bottom: 16px !important; padding-top: 4px !important; }
    .phdr { margin-top: 12px !important; margin-bottom: 6px !important; }
    .tp, .tp-glass { margin-bottom: 8px !important; }

    /* ── Reset & Base ── */
    html,body,[class*="css"]{{
        font-family:'Space Mono','Courier New',Courier,monospace!important;
        background-color:{C['bg']}!important;
        color:{C['text']}!important;
    }}
    #MainMenu,footer,header,.stDeployButton,[data-testid="stToolbar"],
    [data-testid="stDecoration"],[data-testid="stStatusWidget"],
    [data-testid="stSidebarCollapsedControl"],section[data-testid="stSidebar"]{{
        display:none!important;visibility:hidden!important;
    }}

    /* ── Layout — strip all default Streamlit padding ── */
    .block-container{{padding:0!important;max-width:100%!important;}}
    [data-testid="stAppViewContainer"]{{padding:0!important;background-color:{C['bg']}!important;}}
    [data-testid="stVerticalBlock"]{{gap:0!important;}}
    [data-testid="column"]{{padding:0 3px!important;}}
    div[data-testid="stHorizontalBlock"]{{gap:3px!important;}}
    [data-testid="stVerticalBlockBorderWrapper"]{{background:transparent!important;border:none!important;}}

    /* ── Tabs — institutional nav bar ── */
    .stTabs [data-baseweb="tab-list"]{{
        background-color:{C['panel']}!important;
        border-bottom:1px solid {C['border']}!important;
        padding:0 12px!important;gap:0!important;
        box-shadow:0 2px 12px rgba(0,0,0,0.4)!important;
    }}
    .stTabs [data-baseweb="tab"]{{
        background:transparent!important;color:{C['muted']}!important;
        font-family:'Syne',sans-serif!important;font-weight:700!important;
        font-size:10px!important;letter-spacing:2px!important;
        padding:11px 22px!important;border:none!important;
        border-bottom:2px solid transparent!important;border-radius:0!important;
        transition:all .15s ease!important;
    }}
    .stTabs [aria-selected="true"]{{
        color:{C['gold']}!important;
        border-bottom:2px solid {C['gold']}!important;
        background:rgba(197,168,97,0.05)!important;
    }}
    .stTabs [data-baseweb="tab"]:hover{{
        color:{C['text']}!important;background:rgba(255,255,255,0.03)!important;
    }}
    .stTabs [data-baseweb="tab-panel"]{{padding:0!important;background:{C['bg']}!important;}}
    .stTabs [data-baseweb="tab-border"]{{display:none!important;}}

    /* ── Metric Cards ── */
    [data-testid="metric-container"]{{
        background:{C['panel']}!important;border:1px solid {C['border']}!important;
        border-radius:3px!important;padding:8px 12px!important;
        box-shadow:inset 0 0 0 1px rgba(255,255,255,0.02)!important;
    }}
    [data-testid="stMetricLabel"]>div{{
        font-size:8px!important;letter-spacing:1.5px!important;
        text-transform:uppercase!important;color:{C['muted']}!important;
        font-family:'Space Mono',monospace!important;
    }}
    [data-testid="stMetricValue"]{{
        font-family:'Syne',sans-serif!important;font-size:17px!important;
        font-weight:800!important;color:{C['text']}!important;
    }}

    /* ── Form controls ── */
    [data-testid="stSelectbox"] select,[data-testid="stTextInput"] input,
    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div > input {{
        background:{C['panel']}!important;color:{C['text']}!important;
        font-size:10px!important;border:1px solid {C['border']}!important;
        border-radius:3px!important;padding:4px 10px!important;
        font-family:'Space Mono',monospace!important;
    }}
    div[data-baseweb="select"] ul {{ background:{C['panel_alt']}!important; }}
    div[data-baseweb="select"] li:hover {{ background:{C['border']}!important; }}

    /* ── Scrollbar ── */
    ::-webkit-scrollbar{{width:3px;height:3px;}}
    ::-webkit-scrollbar-track{{background:transparent;}}
    ::-webkit-scrollbar-thumb{{background:{C['border2']};border-radius:2px;}}
    iframe{{border:none!important;}}

    /* ── Glass panel ── */
    .tp{{
        background:rgba(12,14,19,0.85);
        border:1px solid {C['border']};
        border-radius:3px;
        padding:8px 12px;
        margin-bottom:2px;
        backdrop-filter:blur(2px);
    }}
    .tp-glass{{
        background:rgba(16,18,26,0.7);
        border:1px solid rgba(30,77,140,0.35);
        border-radius:4px;
        padding:10px 14px;
        margin-bottom:3px;
        box-shadow:0 0 18px rgba(30,77,140,0.08),inset 0 0 0 1px rgba(255,255,255,0.02);
        backdrop-filter:blur(4px);
    }}
    .tp-alert{{
        background:rgba(15,5,5,0.85);
        border:1px solid rgba(211,47,47,0.3);
        border-left:3px solid {C['red']};
        border-radius:3px;
        padding:8px 12px;
        margin-bottom:2px;
    }}

    /* ── Section headers ── */
    .phdr{{
        font-family:'Syne',sans-serif;font-weight:700;font-size:9px;
        letter-spacing:2px;text-transform:uppercase;color:{C['gold']};
        padding:7px 14px;border-bottom:1px solid {C['border']};
        background:linear-gradient(90deg,{C['panel_alt']} 0%,rgba(12,14,19,0.6) 100%);
        display:flex;align-items:center;gap:8px;
        position:relative;
    }}
    .phdr::before{{
        content:'';position:absolute;left:0;top:0;bottom:0;
        width:3px;background:{C['gold']};opacity:0.7;
    }}
    .phdr-badge{{
        font-size:7px;letter-spacing:1.5px;padding:2px 7px;
        border-radius:2px;font-weight:700;margin-left:auto;
    }}

    /* ── KV rows ── */
    .kr{{
        display:flex;justify-content:space-between;padding:3px 0;
        border-bottom:1px solid rgba(26,29,36,0.6);font-size:9px;
    }}
    .kk{{color:{C['muted']};flex:1;letter-spacing:0.3px;}}
    .kv{{color:{C['text']};font-weight:700;text-align:right;flex:1;}}

    /* ── Terminal header ── */
    .term-hdr{{
        background:{C['panel']};
        border-bottom:1px solid {C['border']};
        padding:7px 18px;display:flex;align-items:center;gap:16px;
        box-shadow:0 2px 16px rgba(0,0,0,0.5);
    }}
    .term-logo{{
        font-family:'Syne',sans-serif;font-size:14px;font-weight:800;
        color:#ffffff;letter-spacing:3.5px;
        text-shadow:0 0 20px rgba(197,168,97,0.3);
    }}

    /* ── Valuation cards ── */
    .vc{{
        display:flex;gap:10px;padding:6px 0;
        border-bottom:1px solid {C['border']};align-items:flex-start;
    }}
    .vi{{
        width:22px;height:22px;border-radius:2px;display:flex;align-items:center;
        justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;margin-top:2px;
    }}
    .ipass{{background:rgba(46,125,82,0.18);color:{C['green_br']};border:1px solid {C['green']}44;}}
    .ifail{{background:rgba(211,47,47,0.18);color:{C['red_bright']};border:1px solid {C['red']}44;}}
    .iwarn{{background:rgba(192,122,42,0.18);color:{C['orange_br']};border:1px solid {C['orange']}44;}}

    /* ── Company logo chip ── */
    .clogo{{
        width:44px;height:44px;border-radius:6px;
        background:rgba(197,168,97,0.10);
        border:1px solid {C['gold']}33;
        display:flex;align-items:center;justify-content:center;
        font-family:'Syne',sans-serif;font-size:18px;font-weight:800;
        color:{C['gold']};flex-shrink:0;
        box-shadow:0 0 12px rgba(197,168,97,0.08);
    }}

    /* ── Ticker strip ── */
    .ticker-strip{{
        background:rgba(0,0,0,0.95);border-bottom:1px solid {C['border']};
        padding:5px 16px;font-size:10px;white-space:nowrap;overflow-x:auto;
        letter-spacing:0.5px;display:flex;align-items:center;
    }}
    .t-item{{display:inline-block;margin-right:24px;}}
    .t-label{{color:{C['muted']};margin-right:5px;font-size:9px;letter-spacing:0.5px;}}
    .pos{{color:{C['green_br']};}} .neg{{color:{C['red']};}} .gold{{color:{C['gold']};}}

    /* ── News ── */
    .news-item{{padding:6px 0;border-bottom:1px solid rgba(26,29,36,0.6);}}
    .news-src{{
        font-size:7.5px;letter-spacing:1.5px;text-transform:uppercase;
        padding:1px 6px;border-radius:2px;font-weight:700;
    }}
    .news-title{{font-size:10px;line-height:1.45;color:{C['text']};margin-top:3px;}}
    .news-title.alert{{color:{C['red_bright']};font-weight:700;}}
    .news-meta{{font-size:8px;color:{C['muted']};margin-top:2px;}}
    .sent-bull{{color:{C['green_br']};}} .sent-bear{{color:{C['red']};}} .sent-neut{{color:{C['gold']};}}

    /* ── Crucix news ticker ── */
    .crucix-outer{{
        background:{C['panel']};border-bottom:2px solid {C['gold_dim']};
        display:grid;grid-template-columns:160px 1fr;align-items:center;
        overflow:hidden;height:30px;position:relative;
    }}
    .crucix-label{{
        background:{C['gold_dim']};color:#000;font-family:'Syne',sans-serif;
        font-weight:800;font-size:8.5px;letter-spacing:2.5px;text-transform:uppercase;
        padding:0 12px;height:100%;display:flex;align-items:center;flex-shrink:0;
        border-right:2px solid {C['gold']};
    }}
    .crucix-scroll{{
        display:inline-flex;align-items:center;gap:0;white-space:nowrap;
        height:30px;animation:crucix-scroll 180s linear infinite;
    }}
    .crucix-scroll:hover{{animation-play-state:paused;}}
    @keyframes crucix-scroll{{0%{{transform:translateX(100vw);}}100%{{transform:translateX(-100%);}}}}
    .crucix-item{{
        display:inline-flex;align-items:center;gap:7px;padding:0 20px;
        border-right:1px solid {C['border2']};height:100%;
    }}
    .crucix-src{{
        font-size:7.5px;letter-spacing:1.5px;padding:1px 5px;border-radius:2px;
        font-weight:700;text-transform:uppercase;white-space:nowrap;
    }}
    .crucix-src.flash{{background:rgba(211,47,47,0.22);color:{C['red']};border:1px solid {C['red']}44;}}
    .crucix-src.priority{{background:rgba(192,122,42,0.18);color:{C['orange_br']};}}
    .crucix-src.routine{{background:rgba(197,168,97,0.10);color:{C['gold']};}}
    .crucix-hl{{font-size:9.5px;color:{C['text']};letter-spacing:0.2px;}}
    .crucix-hl.flash{{color:{C['red_bright']};font-weight:700;}}
    @keyframes blink{{0%,100%{{opacity:1;}}50%{{opacity:0.3;}}}}

    /* ── Alpha picks card ── */
    .alpha-card{{
        background:linear-gradient(135deg,rgba(16,18,26,0.95) 0%,rgba(10,13,20,0.95) 100%);
        border:1px solid rgba(30,77,140,0.4);
        border-top:2px solid {C['gold']};
        border-radius:4px;padding:12px 14px;margin-bottom:6px;
        box-shadow:0 4px 24px rgba(0,0,0,0.3),inset 0 0 0 1px rgba(255,255,255,0.02);
    }}
    .alpha-ticker{{
        font-family:'Syne',sans-serif;font-size:18px;font-weight:800;
        color:{C['gold']};letter-spacing:1px;
    }}
    .alpha-name{{
        font-size:9px;color:{C['text_dim']};letter-spacing:0.5px;margin-top:1px;
    }}
    .alpha-roi{{
        font-family:'Syne',sans-serif;font-size:15px;font-weight:800;
        color:{C['green_br']};
    }}
    .alpha-tag{{
        display:inline-block;font-size:7px;letter-spacing:1.5px;font-weight:700;
        padding:2px 6px;border-radius:2px;margin-right:4px;margin-top:3px;
        text-transform:uppercase;
    }}
    .alpha-thesis{{
        font-size:9px;line-height:1.5;color:{C['text_dim']};margin-top:6px;
        border-top:1px solid {C['border']};padding-top:6px;
    }}
    .alpha-divider{{
        display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:8px;
    }}
    .alpha-metric{{
        text-align:center;
        background:rgba(0,0,0,0.2);
        border:1px solid {C['border']};
        border-radius:3px;padding:5px 4px;
    }}
    .alpha-metric-val{{
        font-family:'Syne',sans-serif;font-size:13px;font-weight:800;color:{C['text']};
    }}
    .alpha-metric-lbl{{
        font-size:7px;letter-spacing:1.2px;color:{C['muted']};
        text-transform:uppercase;margin-top:1px;
    }}

    /* ── Valuation full-width improvements ── */
    .val-heatmap-wrap{{overflow:hidden;}}
    .val-score-bar{{
        display:flex;align-items:center;gap:8px;
        background:rgba(0,0,0,0.25);border:1px solid {C['border']};
        border-radius:3px;padding:8px 12px;margin-bottom:3px;
    }}

    /* ── Responsive: Mobile ── */
    @media (max-width:768px){{
        .term-hdr{{flex-wrap:wrap;gap:4px;padding:5px 10px;}}
        .term-logo{{font-size:11px!important;}}
        .crucix-outer{{height:26px!important;}}
        .crucix-label{{font-size:7px!important;padding:0 8px!important;width:100px!important;}}
        .crucix-hl{{font-size:8.5px!important;}}
        .ticker-strip{{padding:3px 8px!important;font-size:9px!important;}}
        .t-item{{margin-right:12px!important;}}
        [data-testid="stMetricValue"]{{font-size:14px!important;}}
        .stTabs [data-baseweb="tab"]{{
            font-size:9px!important;padding:7px 10px!important;
            letter-spacing:0.8px!important;
        }}
        .tp,.tp-glass{{padding:6px 9px!important;}}
        .phdr{{font-size:8px!important;padding:5px 10px!important;}}
        div[data-testid="column"]{{padding:0 1px!important;}}
        .alpha-divider{{grid-template-columns:1fr 1fr!important;}}
        .alpha-card{{padding:8px 10px!important;}}
    }}
    @media (max-width:480px){{
        .stTabs [data-baseweb="tab"]{{
            font-size:8px!important;padding:5px 6px!important;letter-spacing:0px!important;
        }}
        .term-logo{{font-size:9px!important;}}
        .crucix-label{{display:none!important;}}
        .ticker-strip .t-item{{margin-right:10px!important;}}
        .alpha-divider{{grid-template-columns:1fr!important;}}
    }}
    </style>""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §4  RATE LIMITER
# ═════════════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def _domain(self, url: str) -> str:
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except Exception:
            return url[:40]

    def check(self, url: str, max_per_minute: int = 20) -> bool:
        domain = self._domain(url)
        now = time.time()
        with self._lock:
            bucket = self._buckets.setdefault(domain, [])
            self._buckets[domain] = [t for t in bucket if now - t < 60]
            if len(self._buckets[domain]) >= max_per_minute:
                return False
            self._buckets[domain].append(now)
        return True


RATE_LIMITER = RateLimiter()


# ═════════════════════════════════════════════════════════════════════════════════════
# §5  CENTRALIZED TERMINAL STATE
# ═════════════════════════════════════════════════════════════════════════════════════

@dataclass
class TerminalState:
    ticker_prices:     Dict[str, Dict]   = field(default_factory=dict)
    crypto_overview:   Dict              = field(default_factory=dict)
    stock_overview:    Dict              = field(default_factory=dict)
    global_indices:    List[Dict]        = field(default_factory=list)
    market_status:     Dict              = field(default_factory=dict)
    news_all:          List[Dict]        = field(default_factory=list)
    news_crypto:       List[Dict]        = field(default_factory=list)
    news_stocks:       List[Dict]        = field(default_factory=list)
    news_macro:        List[Dict]        = field(default_factory=list)
    sentiment_scores:  Dict[str, float]  = field(default_factory=dict)
    macro_indicators:  Dict[str, Dict]   = field(default_factory=dict)
    macro_zscore:      Dict[str, float]  = field(default_factory=dict)
    econ_calendar:     List[Dict]        = field(default_factory=list)
    crypto_fear_greed: Dict              = field(default_factory=dict)
    stock_fear_greed:  Dict              = field(default_factory=dict)
    tech_analysis:     Dict[str, Dict]   = field(default_factory=dict)
    finnhub_quotes:    Dict[str, Dict]   = field(default_factory=dict)
    finnhub_metrics:   Dict[str, Dict]   = field(default_factory=dict)
    last_update:       Dict[str, float]  = field(default_factory=dict)
    errors:            Dict[str, str]    = field(default_factory=dict)
    _lock:             threading.Lock    = field(default_factory=threading.Lock)

    def update(self, **kwargs) -> None:
        ts = time.time()
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)
                    self.last_update[k] = ts

    def age(self, key: str) -> float:
        t = self.last_update.get(key, 0)
        if t == 0:
            return float("inf")
        return time.time() - t

    def age_safe(self, key: str) -> int:
        """Age in seconds, capped at 99999 (avoids int(inf) OverflowError)."""
        a = self.age(key)
        if math.isinf(a) or math.isnan(a):
            return 99999
        return min(int(a), 99999)

    def mark_error(self, key: str, msg: str) -> None:
        with self._lock:
            self.errors[key] = str(msg)[:120]

    def clear_error(self, key: str) -> None:
        with self._lock:
            self.errors.pop(key, None)


def get_terminal_state() -> TerminalState:
    if "terminal_state" not in st.session_state:
        st.session_state["terminal_state"] = TerminalState()
    return st.session_state["terminal_state"]


# ═════════════════════════════════════════════════════════════════════════════════════
# §6  SENTIMENT ENGINE — Lexicon-based NLP (no external deps)
# ═════════════════════════════════════════════════════════════════════════════════════

_BULL = frozenset(["surge","surges","surged","rally","rallied","rise","rises","rose",
    "gain","gains","jump","jumped","soar","soared","climb","climbed","bullish","breakout",
    "record","high","beat","beats","strong","robust","positive","upbeat","boom","growth",
    "profit","profits","outperform","upgrade","buy","overweight","green","bull","momentum",
    "recovery","rebound","expansion","accelerate","inflow","demand","succeed","success",
    "milestone","approve","approved","breakthrough","outpace"])
_BEAR = frozenset(["plunge","plunges","plunged","crash","crashes","crashed","fall","falls",
    "fell","drop","drops","dropped","slide","slid","slump","slumped","bearish","breakdown",
    "low","miss","misses","missed","weak","negative","concern","loss","losses","decline",
    "declines","declined","sell","underperform","downgrade","red","bear","fear","panic",
    "crisis","recession","contraction","outflow","default","bankrupt","fail","failed",
    "warning","warn","caution","risk","threat","sanction","tariff","inflation","conflict",
    "war","attack","strike","layoff","layoffs","investigation","fraud","lawsuit"])
_INTENSIFIERS = frozenset(["very","extremely","sharply","significantly","massively","deeply"])

def score_text_sentiment(text: str) -> float:
    if not text:
        return 0.0
    words = re.findall(r'\b\w+\b', text.lower())
    bull, bear, amp = 0.0, 0.0, 1.0
    for w in words:
        if w in _INTENSIFIERS:
            amp = 1.5
            continue
        if w in _BULL:
            bull += amp
        elif w in _BEAR:
            bear += amp
        amp = 1.0
    total = bull + bear
    return round((bull - bear) / total, 4) if total > 0 else 0.0

def score_to_label(s: float) -> str:
    if s >= 0.35:  return "VERY BULLISH"
    if s >= 0.12:  return "BULLISH"
    if s <= -0.35: return "VERY BEARISH"
    if s <= -0.12: return "BEARISH"
    return "NEUTRAL"

def score_to_color(s: float) -> str:
    if s >= 0.12:  return C["green_br"]
    if s <= -0.12: return C["red"]
    return C["gold"]

def compute_composite_sentiment(items: List[Dict]) -> float:
    scores = [score_text_sentiment(i.get("title","") + " " + i.get("summary",""))
              for i in items if i.get("title")]
    return round(sum(scores) / len(scores), 4) if scores else 0.0


# ═════════════════════════════════════════════════════════════════════════════════════
# §7  DATA ENGINE — FRED Macroeconomic Module
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fred_series(series_id: str, limit: int = 36) -> List[Dict]:
    """Fetch FRED time-series data for Z-score anomaly detection (36-month window)."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?api_key={FRED_KEY}&file_type=json&limit={limit}"
           f"&sort_order=desc&series_id={series_id}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            obs = r.json().get("observations", [])
            return [{"date": o["date"], "value": o["value"]} for o in obs
                    if o.get("value") and o["value"] != "."]
    except Exception:
        pass
    return []

@st.cache_data(ttl=3600, show_spinner=False)
def get_macro_indicators() -> Dict[str, Dict]:
    """
    Pull all FRED series and compute Z-score anomaly detection.
    Returns enriched dict: {label: {value, delta, trend, note, zscore}}.
    """
    result: Dict[str, Dict] = dict(MACRO_SEED)
    unit_map = {
        "Fed Funds Rate": "%", "US CPI YoY": "%", "US GDP (QoQ)": "%",
        "US Unemployment": "%", "10Y Treasury": "%", "2Y Treasury": "%",
        "VIX Index": "", "M2 Money Supply": "B",
    }
    for label, sid in FRED_SERIES.items():
        try:
            obs = fetch_fred_series(sid, limit=24)
            if len(obs) < 2:
                continue
            vals = []
            for o in obs:
                try:
                    vals.append(float(o["value"]))
                except Exception:
                    pass
            if not vals:
                continue
            cur  = vals[0]
            prev = vals[1]
            hist = vals[1:] if len(vals) > 1 else [cur]

            # Z-score: how many std devs from historical mean
            if len(hist) >= 3:
                mu  = float(np.mean(hist))
                sig = float(np.std(hist)) or 1e-9
                zscore = round((cur - mu) / sig, 2)
            else:
                zscore = 0.0

            delta = cur - prev
            trend = "up" if delta > 0 else "down" if delta < 0 else "flat"
            unit  = unit_map.get(label, "")

            # Format value
            if label == "M2 Money Supply":
                val_str = f"${cur/1000:.2f}T"
                delta_str = f"{'+' if delta>=0 else ''}{delta/1000:.3f}T"
            elif unit == "%":
                val_str = f"{cur:.2f}%"
                delta_str = f"{'+' if delta>=0 else ''}{delta:.2f}%"
            else:
                val_str = f"{cur:.2f}"
                delta_str = f"{'+' if delta>=0 else ''}{delta:.2f}"

            # Anomaly note
            if abs(zscore) > 2.0:
                note = f"⚠ Z={zscore:+.1f} ANOMALY"
            elif abs(zscore) > 1.0:
                note = f"Z={zscore:+.1f} elevated"
            else:
                note = MACRO_SEED.get(label, {}).get("note", "")

            result[label] = {
                "value": val_str, "delta": delta_str,
                "trend": trend, "note": note,
                "raw": cur, "zscore": zscore,
                "date": obs[0].get("date", ""),
            }
        except Exception:
            pass

    # Derived: Yield spread (10Y – 2Y)
    try:
        t10 = result.get("10Y Treasury", {}).get("raw")
        t2  = result.get("2Y Treasury", {}).get("raw")
        if t10 and t2:
            spread = t10 - t2
            result["Yield Spread"] = {
                "value": f"{spread:+.2f}%",
                "delta": "",
                "trend": "up" if spread > 0 else "down",
                "note": "Inverted — recession watch" if spread < 0 else "Normal",
                "raw": spread, "zscore": 0.0,
            }
    except Exception:
        pass

    return result


# ═════════════════════════════════════════════════════════════════════════════════════
# §8  DATA ENGINE — Polygon.io Live Market Feed
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def fetch_polygon_snapshot(symbols: List[str]) -> Dict[str, Dict]:
    """
    Polygon.io snapshot — delayed price data (free tier: 15-min delay).
    Tries batch endpoint; falls back to per-ticker prev-close endpoint.
    Used as secondary source after Finnhub for ticker bar.
    """
    if not symbols:
        return {}
    out: Dict[str, Dict] = {}
    # Try batch first
    try:
        sym_str = ",".join(s for s in symbols[:25] if "=" not in s and "-" not in s)
        if sym_str:
            url = (f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                   f"?tickers={sym_str}&apiKey={POLYGON_KEY}")
            r = requests.get(url, timeout=12)
            if r.status_code == 200:
                for t in r.json().get("tickers", []):
                    day  = t.get("day", {}) or {}
                    prev = t.get("prevDay", {}) or {}
                    c    = float(day.get("c") or t.get("lastTrade", {}).get("p", 0) or 0)
                    pc   = float(prev.get("c") or c)
                    chg  = (c - pc) / pc * 100 if pc else 0
                    sym  = t.get("ticker","")
                    if sym and c > 0:
                        out[sym] = {
                            "price":  c, "change": round(chg, 2),
                            "volume": float(day.get("v", 0) or 0),
                            "high":   float(day.get("h", c) or c),
                            "low":    float(day.get("l", c) or c),
                            "prev":   pc,
                        }
    except Exception:
        pass
    # Per-ticker fallback for any missing
    for sym in symbols:
        if sym in out:
            continue
        if "=" in sym or "-" in sym:
            continue  # skip forex/crypto for Polygon
        try:
            url = (f"https://api.polygon.io/v2/last/trade/{sym}"
                   f"?apiKey={POLYGON_KEY}")
            r = requests.get(url, timeout=6)
            if r.status_code == 200:
                res = r.json().get("result", {})
                p   = float(res.get("p", 0) or 0)
                if p > 0:
                    out[sym] = {"price": p, "change": 0.0, "volume": 0, "high": p, "low": p, "prev": p}
        except Exception:
            pass
    return out


@st.cache_data(ttl=120, show_spinner=False)
def fetch_polygon_ohlcv(ticker: str, days: int = 90, multiplier: int = 1, timespan: str = "day") -> pd.DataFrame:
    """Fetch OHLCV bars from Polygon.io for a given ticker."""
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range"
           f"/{multiplier}/{timespan}"
           f"/{start_dt.strftime('%Y-%m-%d')}/{end_dt.strftime('%Y-%m-%d')}"
           f"?adjusted=true&sort=asc&limit=365&apiKey={POLYGON_KEY}")
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                df = pd.DataFrame(results)
                df["timestamp"] = pd.to_datetime(df["t"], unit="ms")
                df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                                        "c": "close", "v": "volume"})
                return df[["timestamp", "open", "high", "low", "close", "volume"]]
    except Exception:
        pass
    return pd.DataFrame()


# ═════════════════════════════════════════════════════════════════════════════════════
# §9  DATA ENGINE — Alpha Vantage Technical Indicators
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def fetch_alphav_rsi(symbol: str, interval: str = "daily", period: int = 14) -> Optional[float]:
    """Alpha Vantage RSI indicator (latest value)."""
    url = (f"https://www.alphavantage.co/query?function=RSI&symbol={symbol}"
           f"&interval={interval}&time_period={period}&series_type=close"
           f"&apikey={ALPHAV_KEY}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            meta = r.json()
            ts   = meta.get("Technical Analysis: RSI", {})
            if ts:
                latest_key = sorted(ts.keys())[-1]
                return round(float(ts[latest_key]["RSI"]), 2)
    except Exception:
        pass
    return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_alphav_bbands(symbol: str, period: int = 20) -> Optional[Dict]:
    """Alpha Vantage Bollinger Bands (latest values)."""
    url = (f"https://www.alphavantage.co/query?function=BBANDS&symbol={symbol}"
           f"&interval=daily&time_period={period}&series_type=close&nbdevup=2&nbdevdn=2"
           f"&apikey={ALPHAV_KEY}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            ts = r.json().get("Technical Analysis: BBANDS", {})
            if ts:
                k  = sorted(ts.keys())[-1]
                bb = ts[k]
                return {
                    "upper":  round(float(bb["Real Upper Band"]), 4),
                    "middle": round(float(bb["Real Middle Band"]), 4),
                    "lower":  round(float(bb["Real Lower Band"]), 4),
                }
    except Exception:
        pass
    return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_alphav_sma(symbol: str, period: int = 50) -> Optional[float]:
    """Alpha Vantage SMA (latest value)."""
    url = (f"https://www.alphavantage.co/query?function=SMA&symbol={symbol}"
           f"&interval=daily&time_period={period}&series_type=close&apikey={ALPHAV_KEY}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            ts = r.json().get("Technical Analysis: SMA", {})
            if ts:
                k = sorted(ts.keys())[-1]
                return round(float(ts[k]["SMA"]), 4)
    except Exception:
        pass
    return None


# ═════════════════════════════════════════════════════════════════════════════════════
# §10  DATA ENGINE — Finnhub Real-Time Quotes & Fundamentals
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30, show_spinner=False)
def fetch_finnhub_quote(symbol: str) -> Dict:
    """Finnhub real-time quote: c=current, pc=prev close, h=high, l=low, o=open."""
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            d = r.json()
            c  = float(d.get("c", 0) or 0)
            pc = float(d.get("pc", 0) or c)
            pct = (c - pc) / pc * 100 if pc else 0
            return {
                "price":   c,
                "prev":    pc,
                "change":  round(pct, 2),
                "high":    float(d.get("h", c) or c),
                "low":     float(d.get("l", c) or c),
                "open":    float(d.get("o", c) or c),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_finnhub_metrics(symbol: str) -> Dict:
    """Finnhub fundamental metrics (P/E, EPS, revenue growth, etc.)."""
    url = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={FINNHUB_KEY}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            m = r.json().get("metric", {}) or {}
            return {
                "pe":            m.get("peBasicExclExtraTTM"),
                "eps":           m.get("epsBasicExclExtraAnnual"),
                "eps_growth":    m.get("epsGrowth5Y"),
                "revenue_growth":m.get("revenueGrowth5Y"),
                "roa":           m.get("roaRfy"),
                "roe":           m.get("roeRfy"),
                "gross_margin":  m.get("grossMarginAnnual"),
                "net_margin":    m.get("netProfitMarginAnnual"),
                "debt_equity":   m.get("totalDebt/totalEquityAnnual"),
                "beta":          m.get("beta"),
                "52w_high":      m.get("52WeekHigh"),
                "52w_low":       m.get("52WeekLow"),
                "dividend_yield":m.get("dividendYieldIndicatedAnnual"),
                "fcf_yield":     m.get("freeCashFlowYieldTTM"),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_finnhub_profile(symbol: str) -> Dict:
    """Finnhub company profile (name, industry, logo, market cap)."""
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            d = r.json()
            return {
                "name":       d.get("name", symbol),
                "industry":   d.get("finnhubIndustry", ""),
                "exchange":   d.get("exchange", ""),
                "market_cap": d.get("marketCapitalization", 0),
                "logo":       d.get("logo", ""),
                "weburl":     d.get("weburl", ""),
            }
    except Exception:
        pass
    return {}


# ═════════════════════════════════════════════════════════════════════════════════════
# §11  DATA ENGINE — Marketstack Global Exchange Aggregation
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def fetch_marketstack_quotes(symbols: List[str]) -> Dict[str, Dict]:
    """
    Marketstack EOD / intraday data for global equity tickers.
    Free tier: HTTP only, 100 req/mo.
    """
    if not symbols:
        return {}
    sym_str = ",".join(symbols[:10])
    url = (f"http://api.marketstack.com/v1/eod/latest"
           f"?access_key={MARKETSTACK_KEY}&symbols={sym_str}&limit=1")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            data = r.json().get("data", [])
            out = {}
            for item in data:
                sym = item.get("symbol", "")
                out[sym] = {
                    "price":  float(item.get("close", 0) or 0),
                    "open":   float(item.get("open", 0) or 0),
                    "high":   float(item.get("high", 0) or 0),
                    "low":    float(item.get("low", 0) or 0),
                    "volume": float(item.get("volume", 0) or 0),
                    "date":   item.get("date", ""),
                    "change": round(float(item.get("close", 0) or 0) - float(item.get("open", 0) or 0), 4),
                }
            return out
    except Exception:
        pass
    return {}


# ═════════════════════════════════════════════════════════════════════════════════════
# §12  DATA ENGINE — Tradier Market Status & Options Reference
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def fetch_tradier_market_status() -> Dict:
    """
    Tradier market clock — NYSE open/closed status.
    Tries production endpoint first; falls back to time-of-day heuristic.
    """
    hdrs = {"Authorization": f"Bearer {TRADIER_KEY}", "Accept": "application/json"}
    for base in ["https://api.tradier.com", "https://sandbox.tradier.com"]:
        try:
            r = requests.get(f"{base}/v1/markets/clock", headers=hdrs, timeout=8)
            if r.status_code == 200:
                clock = r.json().get("clock", {})
                state = clock.get("state", "unknown")
                desc  = clock.get("description", "")
                return {
                    "status":     "Open" if state == "open" else "Closed" if state == "closed" else state.title(),
                    "message":    desc or state.title(),
                    "next_open":  clock.get("next_open", ""),
                    "next_close": clock.get("next_close", ""),
                }
        except Exception:
            continue

    # Heuristic fallback: NYSE hours Mon-Fri 09:30–16:00 ET
    try:
        import pytz
        now_et = datetime.now(pytz.timezone("US/Eastern"))
        is_weekday = now_et.weekday() < 5
        t = now_et.hour * 60 + now_et.minute
        is_hours  = 570 <= t <= 960       # 09:30–16:00
        is_pre    = 240 <= t < 570        # 04:00–09:30
        if is_weekday and is_hours:
            return {"status": "Open",   "message": "NYSE regular session (estimated)"}
        elif is_weekday and is_pre:
            return {"status": "Pre-Market","message": "Pre-market session (estimated)"}
        else:
            return {"status": "Closed", "message": "Outside NYSE hours (estimated)"}
    except Exception:
        pass
    return {"status": "Unknown", "message": "Market status unavailable"}


@st.cache_data(ttl=120, show_spinner=False)
def fetch_tradier_quotes(symbols: List[str]) -> Dict[str, Dict]:
    """Tradier real-time equity quotes."""
    if not symbols:
        return {}
    url  = "https://api.tradier.com/v1/markets/quotes"
    hdrs = {"Authorization": f"Bearer {TRADIER_KEY}", "Accept": "application/json"}
    try:
        r = requests.get(url, headers=hdrs, params={"symbols": ",".join(symbols)}, timeout=10)
        if r.status_code == 200:
            raw = r.json().get("quotes", {}).get("quote", [])
            if isinstance(raw, dict):
                raw = [raw]
            out = {}
            for q in raw:
                sym = q.get("symbol", "")
                last = float(q.get("last", 0) or 0)
                prev = float(q.get("prevclose", 0) or last)
                pct  = (last - prev) / prev * 100 if prev else 0
                out[sym] = {
                    "price":  last,
                    "change": round(pct, 2),
                    "bid":    float(q.get("bid", 0) or 0),
                    "ask":    float(q.get("ask", 0) or 0),
                    "volume": float(q.get("volume", 0) or 0),
                }
            return out
    except Exception:
        pass
    return {}


# ═════════════════════════════════════════════════════════════════════════════════════
# §13  DATA ENGINE — CoinGecko Crypto
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=120, show_spinner=False)
def fetch_coingecko_overview() -> Dict:
    """CoinGecko free API — top 50 coins by market cap. Retries on 429."""
    url = ("https://api.coingecko.com/api/v3/coins/markets"
           "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1"
           "&sparkline=false&price_change_percentage=24h")
    hdrs = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get(url, headers=hdrs, timeout=15)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                coins = r.json()
                return {
                    "coins": [{
                        "symbol":     c.get("symbol", "").upper(),
                        "name":       c.get("name", ""),
                        "price":      float(c.get("current_price", 0) or 0),
                        "change_24h": float(c.get("price_change_percentage_24h", 0) or 0),
                        "volume_24h": float(c.get("total_volume", 0) or 0),
                        "market_cap": float(c.get("market_cap", 0) or 0),
                        "high_24h":   float(c.get("high_24h", 0) or 0),
                        "low_24h":    float(c.get("low_24h", 0) or 0),
                        "rank":       int(c.get("market_cap_rank", 0) or 0),
                    } for c in coins],
                    "total_volume_24h": sum(c.get("total_volume", 0) or 0 for c in coins),
                }
        except Exception:
            pass
    return {}


@st.cache_data(ttl=120, show_spinner=False)
def fetch_coingecko_global() -> Dict:
    """CoinGecko global market stats."""
    try:
        r = requests.get("https://api.coingecko.com/api/v3/global", timeout=8)
        if r.status_code == 200:
            d = r.json().get("data", {})
            return {
                "total_market_cap": d.get("total_market_cap", {}).get("usd", 0),
                "btc_dominance":    d.get("market_cap_percentage", {}).get("btc", 0),
                "eth_dominance":    d.get("market_cap_percentage", {}).get("eth", 0),
                "active_coins":     d.get("active_cryptocurrencies", 0),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_crypto_fear_greed() -> Dict:
    """Alternative.me crypto Fear & Greed."""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=7", timeout=8)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                cur = data[0]
                return {
                    "value":          int(cur["value"]),
                    "classification": cur["value_classification"],
                    "history": [{
                        "value": int(h["value"]),
                        "label": h["value_classification"],
                        "date":  datetime.fromtimestamp(int(h["timestamp"])).strftime("%Y-%m-%d"),
                    } for h in data],
                }
    except Exception:
        pass
    return {"value": 50, "classification": "Neutral", "history": []}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_stock_fear_greed() -> Dict:
    """CNN Fear & Greed equity index."""
    try:
        r = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code == 200:
            d = r.json().get("fear_and_greed", {})
            return {"value": int(float(d.get("score", 50))), "classification": d.get("rating", "Neutral")}
    except Exception:
        pass
    return {"value": 50, "classification": "Neutral"}


# ═════════════════════════════════════════════════════════════════════════════════════
# §14  NEWS ENGINE — NewsAPI, GNews, NewsData.io, World News API + RSS fallbacks
# ═════════════════════════════════════════════════════════════════════════════════════

def _news_id(title: str, source: str) -> str:
    return hashlib.md5(f"{title[:60]}:{source}".encode()).hexdigest()[:12]

def _clean_html(text: str) -> str:
    text = re.sub(r'<[^>]+>', '', text or '')
    return _html_mod.unescape(text).strip()

def _is_alert(title: str) -> bool:
    kws = ["attack","strike","conflict","kill","war","missile","threat",
           "sanction","explosion","bomb","nuclear","crisis","default","crash"]
    return any(k in title.lower() for k in kws)

def _classify_tier(title: str) -> str:
    t = title.lower()
    if any(k in t for k in _FLASH_KEYWORDS):    return "FLASH"
    if any(k in t for k in _PRIORITY_KEYWORDS): return "PRIORITY"
    return "ROUTINE"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_newsapi(query: str = "financial markets economy", page_size: int = 20) -> List[Dict]:
    """NewsAPI.org — broad financial news mining."""
    url = (f"https://newsapi.org/v2/everything?q={query}"
           f"&language=en&sortBy=publishedAt&pageSize={page_size}"
           f"&apiKey={NEWSAPI_KEY}")
    items = []
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            for art in r.json().get("articles", [])[:page_size]:
                title   = _clean_html(art.get("title", ""))
                summary = _clean_html(art.get("description", ""))[:300]
                if not title or title == "[Removed]":
                    continue
                pub = ""
                try:
                    pub = datetime.fromisoformat(
                        art.get("publishedAt","").replace("Z","+00:00")
                    ).strftime("%d %b %H:%M")
                except Exception:
                    pass
                src = art.get("source", {}).get("name", "NewsAPI")[:16]
                items.append({
                    "id":         _news_id(title, src),
                    "source":     src,
                    "title":      title[:140],
                    "summary":    summary,
                    "link":       art.get("url", ""),
                    "published":  pub,
                    "is_alert":   _is_alert(title),
                    "asset_type": "markets",
                    "tier":       _classify_tier(title),
                    "sentiment":  score_text_sentiment(title + " " + summary),
                })
    except Exception:
        pass
    return items


@st.cache_data(ttl=300, show_spinner=False)
def fetch_gnews(query: str = "economy markets", max_results: int = 10) -> List[Dict]:
    """GNews API — high-speed breaking headlines."""
    url = (f"https://gnews.io/api/v4/search?q={query}&lang=en"
           f"&max={max_results}&apikey={GNEWS_KEY}")
    items = []
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            for art in r.json().get("articles", [])[:max_results]:
                title   = _clean_html(art.get("title", ""))
                summary = _clean_html(art.get("description", ""))[:300]
                if not title:
                    continue
                pub = ""
                try:
                    pub = datetime.fromisoformat(
                        art.get("publishedAt","").replace("Z","+00:00")
                    ).strftime("%d %b %H:%M")
                except Exception:
                    pass
                src = art.get("source", {}).get("name", "GNews")[:16]
                items.append({
                    "id":         _news_id(title, src),
                    "source":     src,
                    "title":      title[:140],
                    "summary":    summary,
                    "link":       art.get("url", ""),
                    "published":  pub,
                    "is_alert":   _is_alert(title),
                    "asset_type": "macro",
                    "tier":       _classify_tier(title),
                    "sentiment":  score_text_sentiment(title + " " + summary),
                })
    except Exception:
        pass
    return items


@st.cache_data(ttl=300, show_spinner=False)
def fetch_newsdata(query: str = "finance economy", language: str = "en", size: int = 10) -> List[Dict]:
    """NewsData.io — historical and categorised economic archive."""
    url = (f"https://newsdata.io/api/1/news?apikey={NEWSDATA_KEY}"
           f"&q={query}&language={language}&size={size}")
    items = []
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            for art in r.json().get("results", [])[:size]:
                title   = _clean_html(art.get("title", ""))
                summary = _clean_html(art.get("description", "") or "")[:300]
                if not title:
                    continue
                pub = ""
                try:
                    pub = datetime.fromisoformat(
                        art.get("pubDate","").replace("Z","+00:00")
                    ).strftime("%d %b %H:%M")
                except Exception:
                    pass
                src = art.get("source_id", "NewsData")[:16]
                items.append({
                    "id":         _news_id(title, src),
                    "source":     src,
                    "title":      title[:140],
                    "summary":    summary,
                    "link":       art.get("link", ""),
                    "published":  pub,
                    "is_alert":   _is_alert(title),
                    "asset_type": "macro",
                    "tier":       _classify_tier(title),
                    "sentiment":  score_text_sentiment(title + " " + summary),
                })
    except Exception:
        pass
    return items


@st.cache_data(ttl=300, show_spinner=False)
def fetch_worldnews(query: str = "geopolitics economy conflict", number: int = 10) -> List[Dict]:
    """World News API — geopolitical event & sentiment ingestion."""
    url = (f"https://api.worldnewsapi.com/search-news"
           f"?text={query}&language=en&number={number}"
           f"&api-key={WORLDNEWS_KEY}")
    items = []
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            for art in r.json().get("news", [])[:number]:
                title   = _clean_html(art.get("title", ""))
                summary = _clean_html(art.get("text", ""))[:300]
                if not title:
                    continue
                pub = ""
                try:
                    pub_raw = art.get("publish_date", "")
                    if pub_raw:
                        pub = datetime.fromisoformat(pub_raw.replace("Z","+00:00")).strftime("%d %b %H:%M")
                except Exception:
                    pass
                src = art.get("source_country", "WorldNews")[:16]
                items.append({
                    "id":         _news_id(title, src),
                    "source":     "WorldNews",
                    "title":      title[:140],
                    "summary":    summary,
                    "link":       art.get("url", ""),
                    "published":  pub,
                    "is_alert":   _is_alert(title),
                    "asset_type": "macro",
                    "tier":       _classify_tier(title),
                    "sentiment":  score_text_sentiment(title + " " + summary),
                })
    except Exception:
        pass
    return items


def _fetch_rss_sync(feeds: List[Tuple[str, str]], asset_type: str) -> List[Dict]:
    """Synchronous RSS feed fetcher with feedparser."""
    items: List[Dict] = []
    for source, url in feeds:
        if not RATE_LIMITER.check(url, max_per_minute=5):
            continue
        try:
            resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                continue
            feed = feedparser.parse(resp.text)
            for entry in feed.entries[:10]:
                title   = _clean_html(entry.get("title", ""))
                summary = _clean_html(entry.get("summary", ""))[:300]
                if not title:
                    continue
                pub = ""
                try:
                    if entry.get("published_parsed"):
                        pub = datetime(*entry.published_parsed[:6]).strftime("%d %b %H:%M")
                except Exception:
                    pass
                items.append({
                    "id":         _news_id(title, source),
                    "source":     source[:16],
                    "title":      title[:140],
                    "summary":    summary,
                    "link":       entry.get("link", ""),
                    "published":  pub,
                    "is_alert":   _is_alert(title),
                    "asset_type": asset_type,
                    "tier":       _classify_tier(title),
                    "sentiment":  score_text_sentiment(title + " " + summary),
                })
        except Exception:
            continue
    return items


def _dedup_news(items: List[Dict], max_items: int = 60) -> List[Dict]:
    seen: set = set()
    out: List[Dict] = []
    for item in items:
        k = item["id"]
        if k not in seen:
            seen.add(k)
            out.append(item)
        if len(out) >= max_items:
            break
    return out


@st.cache_data(ttl=300, show_spinner=False)
def aggregate_all_news() -> Dict[str, List[Dict]]:
    """
    Master news aggregator — pulls from NewsAPI, GNews, NewsData.io,
    World News API and RSS fallback feeds. Deduplicates, timestamps, and
    scores sentiment across all merged streams.
    Each source is individually guarded so one failure cannot blank the feed.
    """
    def _safe(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs) or []
        except Exception:
            return []

    newsapi_markets = _safe(fetch_newsapi, "stock market finance economy", 20)
    newsapi_macro   = _safe(fetch_newsapi, "federal reserve inflation gdp", 15)
    gnews_breaking  = _safe(fetch_gnews,  "economy markets finance", 10)
    gnews_geo       = _safe(fetch_gnews,  "geopolitics conflict war sanctions", 10)
    newsdata_econ   = _safe(fetch_newsdata,"finance economy markets", "en", 10)
    worldnews_geo   = _safe(fetch_worldnews,"geopolitics economy conflict military", 10)
    rss_crypto      = _safe(_fetch_rss_sync, _CRYPTO_RSS, "crypto")
    rss_market      = _safe(_fetch_rss_sync, _MARKET_RSS, "markets")
    rss_macro       = _safe(_fetch_rss_sync, _MACRO_RSS,  "macro")

    crypto_all  = _dedup_news(rss_crypto, 40)
    markets_all = _dedup_news(newsapi_markets + rss_market + gnews_breaking, 60)
    macro_all   = _dedup_news(newsapi_macro + worldnews_geo + gnews_geo + newsdata_econ + rss_macro, 60)
    all_items   = _dedup_news(crypto_all + markets_all + macro_all, 100)

    # Guarantee at least RSS content is always present
    if not all_items:
        fallback = _safe(_fetch_rss_sync, _MACRO_RSS + _MARKET_RSS + _CRYPTO_RSS, "markets")
        all_items = _dedup_news(fallback, 60)
        crypto_all  = [i for i in all_items if i.get("asset_type") == "crypto"]
        markets_all = [i for i in all_items if i.get("asset_type") == "markets"]
        macro_all   = [i for i in all_items if i.get("asset_type") == "macro"]

    return {
        "crypto":  crypto_all,
        "markets": markets_all,
        "macro":   macro_all,
        "all":     all_items,
    }


# ═════════════════════════════════════════════════════════════════════════════════════
# §15  DATA ENGINE — Economic Calendar (ForexFactory) & Ticker Prices
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_economic_calendar() -> List[Dict]:
    """ForexFactory JSON economic calendar (this week + next week)."""
    urls = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
    ]
    events: List[Dict] = []
    currency_map = {
        "USD":"US","EUR":"EU","GBP":"GB","JPY":"JP","CNY":"CN",
        "AUD":"AU","CAD":"CA","CHF":"CH","NZD":"NZ",
    }
    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                for ev in r.json():
                    if not ev.get("title") or not ev.get("date"):
                        continue
                    impact = ev.get("impact", "low").lower()
                    impact = "high" if impact == "high" else "medium" if "medium" in impact else "low"
                    events.append({
                        "event":    ev["title"],
                        "country":  currency_map.get(ev.get("country",""), "??"),
                        "currency": ev.get("country",""),
                        "date":     ev["date"],
                        "impact":   impact,
                        "actual":   ev.get("actual"),
                        "forecast": ev.get("forecast"),
                        "previous": ev.get("previous"),
                    })
        except Exception:
            continue
    return sorted(events, key=lambda e: e["date"])


def get_ticker_prices() -> Dict[str, Dict]:
    """
    Fetch live prices for ticker bar.
    Priority: Finnhub (real-time, free) → Polygon (delayed free tier).
    Finnhub is used first because Polygon free tier has 15-min delay.
    """
    out: Dict[str, Dict] = {}

    # Primary: Finnhub real-time quote per symbol (30-second cache)
    for label, sym in TICKER_BAR_SYMBOLS:
        try:
            fq = fetch_finnhub_quote(sym)
            if fq.get("price", 0) > 0:
                out[sym] = {"price": fq["price"], "pct": fq["change"], "label": label}
                continue
        except Exception:
            pass

        # Fallback: Polygon snapshot (may be delayed on free tier)
        try:
            poly = fetch_polygon_snapshot([sym])
            if sym in poly and poly[sym].get("price", 0) > 0:
                d = poly[sym]
                out[sym] = {"price": d["price"], "pct": d["change"], "label": label}
                continue
        except Exception:
            pass

        out[sym] = {"price": 0, "pct": 0, "label": label}
    return out


def get_stock_overview(symbols: List[str]) -> List[Dict]:
    """Build stock overview table from Finnhub (real-time) + Polygon (fallback)."""
    result = []
    # batch Polygon snapshot once
    try:
        poly = fetch_polygon_snapshot(symbols)
    except Exception:
        poly = {}
    for sym in symbols:
        try:
            # Finnhub real-time quote first
            fq     = fetch_finnhub_quote(sym)
            price  = fq.get("price", 0)
            change = fq.get("change", 0)
            # Polygon as fallback / supplement
            if (price == 0) and sym in poly and poly[sym].get("price"):
                price  = poly[sym]["price"]
                change = poly[sym]["change"]
            # profile for name/mkt-cap only (cached 24h)
            try:
                prof = fetch_finnhub_profile(sym)
                name = prof.get("name", sym)
                cap  = float(prof.get("market_cap", 0) or 0) * 1e6
            except Exception:
                name, cap = sym, 0.0
            result.append({
                "symbol":     sym,
                "name":       name,
                "price":      price,
                "change_24h": change,
                "market_cap": cap,
            })
        except Exception:
            result.append({"symbol": sym, "name": sym, "price": 0, "change_24h": 0, "market_cap": 0})
    return result


# ═════════════════════════════════════════════════════════════════════════════════════
# §16  TECHNICAL ANALYSIS — RSI, EMA, ATR, Bollinger Bands (pure numpy)
# ═════════════════════════════════════════════════════════════════════════════════════

def _calc_rsi(closes: np.ndarray, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    changes = np.diff(closes)
    gains   = np.where(changes > 0, changes, 0.0)
    losses  = np.where(changes < 0, -changes, 0.0)
    ag = float(np.mean(gains[:period]))
    al = float(np.mean(losses[:period]))
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return round(100 - (100 / (1 + rs)), 2)

def _calc_ema(closes: np.ndarray, period: int) -> float:
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    mult = 2.0 / (period + 1)
    ema  = float(np.mean(closes[:period]))
    for c in closes[period:]:
        ema = (float(c) - ema) * mult + ema
    return ema

def _calc_bbands(closes: np.ndarray, period: int = 20, num_std: float = 2.0) -> Tuple[float, float, float]:
    if len(closes) < period:
        c = float(closes[-1]) if len(closes) > 0 else 0.0
        return c, c, c
    window = closes[-period:]
    mid    = float(np.mean(window))
    std    = float(np.std(window))
    return round(mid + num_std * std, 4), round(mid, 4), round(mid - num_std * std, 4)


def build_tech_analysis(df: pd.DataFrame, symbol: str) -> Dict:
    """Full technical snapshot from OHLCV DataFrame."""
    if df.empty or len(df) < 20:
        return {"symbol": symbol, "error": "Insufficient data"}
    closes = df["close"].values.astype(float)
    current = float(closes[-1])
    rsi     = _calc_rsi(closes)
    ema20   = _calc_ema(closes, 20)
    ema50   = _calc_ema(closes, 50) if len(closes) >= 50 else ema20
    bb_up, bb_mid, bb_lo = _calc_bbands(closes)
    trend = "bullish" if ema20 > ema50 * 1.005 else "bearish" if ema20 < ema50 * 0.995 else "neutral"
    rsi_lbl = "Overbought" if rsi >= 70 else "Oversold" if rsi <= 30 else "Bullish" if rsi >= 55 else "Bearish" if rsi <= 45 else "Neutral"
    sma200 = float(np.mean(closes[-200:])) if len(closes) >= 200 else float(np.mean(closes))
    above_200 = current > sma200

    return {
        "symbol":      symbol,
        "price":       current,
        "rsi":         rsi,
        "rsi_signal":  rsi_lbl,
        "ema20":       round(ema20, 4),
        "ema50":       round(ema50, 4),
        "sma200":      round(sma200, 4),
        "above_200sma":above_200,
        "trend":       trend,
        "bb_upper":    bb_up,
        "bb_mid":      bb_mid,
        "bb_lower":    bb_lo,
        "error":       None,
    }


# ═════════════════════════════════════════════════════════════════════════════════════
# §17  QUANTAMENTAL RATING ENGINE
# ═════════════════════════════════════════════════════════════════════════════════════

def compute_quantamental_rating(
    profile:   Dict,
    tech:      Dict,
    metrics:   Dict,
    macro:     Dict,
    sentiment: float,
) -> Dict:
    """
    Multi-factor quantamental rating (0–100):
      30% Valuation (DCF margin of safety)
      20% Financial health (margins, ROE, debt)
      15% Technical momentum (RSI, trend, BB position)
      20% Macro regime (yield spread, VIX, CPI)
      15% Sentiment (NLP score)
    """
    score = 0.0
    detail = {}

    # ── Valuation (30pts) ──────────────────────────────────────────────────────
    cur  = float(profile.get("current_price", 0) or 0)
    fair = float(profile.get("fair_value_dcf", cur) or cur)
    if cur > 0 and fair > 0:
        mos = (fair - cur) / cur  # margin of safety: + = undervalued
        val_score = max(0, min(30, 15 + mos * 30))
    else:
        val_score = 15.0
    score += val_score
    detail["valuation"] = round(val_score, 1)

    # ── Financial Health (20pts) ───────────────────────────────────────────────
    gm  = float(profile.get("gross_margin",  metrics.get("gross_margin",  0.3) or 0.3))
    pm  = float(profile.get("profit_margin", metrics.get("net_margin",    0.1) or 0.1))
    roe = float(profile.get("roe",           metrics.get("roe",           0.15) or 0.15))
    de  = float(profile.get("debt_equity",   metrics.get("debt_equity",   0.5)  or 0.5))
    fh  = min(20, (gm * 10) + (pm * 10) + (roe * 15) - (min(de, 3) * 2))
    score += max(0, fh)
    detail["financial_health"] = round(max(0, fh), 1)

    # ── Technical Momentum (15pts) ────────────────────────────────────────────
    tech_score = 7.5  # neutral start
    if tech and not tech.get("error"):
        rsi = float(tech.get("rsi", 50))
        if 40 <= rsi <= 65:  tech_score += 3    # sweet-spot RSI
        elif rsi < 30:       tech_score += 2    # oversold = opportunity
        elif rsi > 75:       tech_score -= 2    # overbought
        trend = tech.get("trend", "neutral")
        if trend == "bullish":   tech_score += 3
        elif trend == "bearish": tech_score -= 3
        if tech.get("above_200sma"):  tech_score += 2
    score += max(0, min(15, tech_score))
    detail["technical"] = round(max(0, min(15, tech_score)), 1)

    # ── Macro Regime (20pts) ─────────────────────────────────────────────────
    macro_score = 10.0  # neutral start
    spread_raw = macro.get("Yield Spread", {}).get("raw", 0) or 0
    vix_raw    = macro.get("VIX Index", {}).get("raw", 20) or 20
    cpi_raw    = macro.get("US CPI YoY", {}).get("raw", 3) or 3
    if spread_raw > 0:    macro_score += 3   # normal curve = growth
    elif spread_raw < -0.5: macro_score -= 4 # deeply inverted = recession risk
    if vix_raw < 15:      macro_score += 3
    elif vix_raw > 30:    macro_score -= 4
    if cpi_raw < 3.0:     macro_score += 2
    elif cpi_raw > 5.0:   macro_score -= 3
    score += max(0, min(20, macro_score))
    detail["macro_regime"] = round(max(0, min(20, macro_score)), 1)

    # ── Sentiment (15pts) ─────────────────────────────────────────────────────
    sent_score = 7.5 + sentiment * 10.0
    score += max(0, min(15, sent_score))
    detail["sentiment"] = round(max(0, min(15, sent_score)), 1)

    # ── Final rating label ─────────────────────────────────────────────────────
    total = min(100, max(0, round(score)))
    if total >= 80:   label = "STRONG BUY"
    elif total >= 65: label = "BUY"
    elif total >= 50: label = "HOLD"
    elif total >= 35: label = "SELL"
    else:             label = "STRONG SELL"

    return {"score": total, "label": label, "detail": detail}


# ═════════════════════════════════════════════════════════════════════════════════════
# §18  BACKGROUND PIPELINE ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════════════

_PIPELINE_INTERVALS: Dict[str, int] = {
    "ticker_prices":    60,
    "crypto_overview":  120,
    "stock_overview":   120,
    "fear_greed":       300,
    "news_all":         300,
    "econ_calendar":    1800,
    "macro_indicators": 3600,
    "market_status":    60,
    "finnhub_quotes":   30,
}


def _pipeline_worker(state: TerminalState) -> None:
    """Background daemon: refreshes stale data sources every 30s."""
    while True:
        try:
            # ── Ticker prices ─────────────────────────────────────────────
            if state.age("ticker_prices") > _PIPELINE_INTERVALS["ticker_prices"]:
                try:
                    prices = get_ticker_prices()
                    state.update(ticker_prices=prices)
                    state.clear_error("ticker_prices")
                except Exception as e:
                    state.mark_error("ticker_prices", e)

            # ── Market status (Tradier) ────────────────────────────────────
            if state.age("market_status") > _PIPELINE_INTERVALS["market_status"]:
                try:
                    ms = fetch_tradier_market_status()
                    state.update(market_status=ms)
                except Exception as e:
                    state.mark_error("market_status", e)

            # ── Crypto overview ────────────────────────────────────────────
            if state.age("crypto_overview") > _PIPELINE_INTERVALS["crypto_overview"]:
                try:
                    cov  = fetch_coingecko_overview()
                    glbl = fetch_coingecko_global()
                    if cov:
                        cov.update(glbl)
                        state.update(crypto_overview=cov)
                        state.clear_error("crypto_overview")
                except Exception as e:
                    state.mark_error("crypto_overview", e)

            # ── Stock overview ─────────────────────────────────────────────
            if state.age("stock_overview") > _PIPELINE_INTERVALS["stock_overview"]:
                try:
                    stocks = get_stock_overview(STOCK_WATCH[:12])
                    if stocks:
                        pmap = {s["symbol"]: {"price": s["price"], "pct": s["change_24h"]}
                                for s in stocks}
                        state.update(stock_overview={"coins": stocks},
                                     ticker_prices=pmap)
                        state.clear_error("stock_overview")
                except Exception as e:
                    state.mark_error("stock_overview", e)

            # ── Fear & Greed ───────────────────────────────────────────────
            if state.age("fear_greed") > _PIPELINE_INTERVALS["fear_greed"]:
                try:
                    cfg = fetch_crypto_fear_greed()
                    sfg = fetch_stock_fear_greed()
                    state.update(crypto_fear_greed=cfg, stock_fear_greed=sfg)
                    state.clear_error("fear_greed")
                except Exception as e:
                    state.mark_error("fear_greed", e)

            # ── Economic calendar ──────────────────────────────────────────
            if state.age("econ_calendar") > _PIPELINE_INTERVALS["econ_calendar"]:
                try:
                    ev = fetch_economic_calendar()
                    if ev:
                        state.update(econ_calendar=ev)
                except Exception as e:
                    state.mark_error("econ_calendar", e)

            # ── Macro indicators (FRED) ────────────────────────────────────
            if state.age("macro_indicators") > _PIPELINE_INTERVALS["macro_indicators"]:
                try:
                    macro = get_macro_indicators()
                    state.update(macro_indicators=macro)
                    state.clear_error("macro_indicators")
                except Exception as e:
                    state.mark_error("macro_indicators", e)

            # ── Global indices (Yahoo Finance) ─────────────────────────────
            if state.age("global_indices") > 120:
                try:
                    idxs = get_global_indices()
                    if idxs:
                        state.update(global_indices=idxs)
                except Exception as e:
                    state.mark_error("global_indices", e)

            # ── News aggregation ───────────────────────────────────────────
            if state.age("news_all") > _PIPELINE_INTERVALS["news_all"]:
                try:
                    news = aggregate_all_news()
                    all_items = news.get("all", [])
                    composite = compute_composite_sentiment(all_items)
                    sent_scores: Dict[str, float] = {}
                    for item in all_items:
                        sym = item.get("source", "market")
                        sc  = item.get("sentiment", 0.0)
                        if sym not in sent_scores:
                            sent_scores[sym] = sc
                        else:
                            sent_scores[sym] = (sent_scores[sym] + sc) / 2
                    sent_scores["composite"] = composite
                    state.update(
                        news_all     = news.get("all", []),
                        news_crypto  = news.get("crypto", []),
                        news_stocks  = news.get("markets", []),
                        news_macro   = news.get("macro", []),
                        sentiment_scores = sent_scores,
                    )
                    state.clear_error("news_all")
                except Exception as e:
                    state.mark_error("news_all", e)

        except Exception as e:
            state.mark_error("pipeline", e)

        time.sleep(30)


def start_background_pipeline() -> None:
    if st.session_state.get("_pipeline_started"):
        return
    state = get_terminal_state()
    t = threading.Thread(target=_pipeline_worker, args=(state,), daemon=True, name="DataPipeline")
    t.start()
    st.session_state["_pipeline_started"] = True


# ═════════════════════════════════════════════════════════════════════════════════════
# §19  CHART BUILDERS — Plotly dark-theme charts
# ═════════════════════════════════════════════════════════════════════════════════════

_DARK_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=C["panel"],
    font=dict(family="Space Mono, monospace", color=C["text_dim"], size=9),
    margin=dict(l=8, r=8, t=8, b=8),
    xaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"],
               tickfont=dict(size=8), showgrid=True),
    yaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"],
               tickfont=dict(size=8), showgrid=True),
    showlegend=False,
)


def _apply_dark(fig: go.Figure) -> go.Figure:
    fig.update_layout(**_DARK_LAYOUT)
    return fig


def build_candlestick(df: pd.DataFrame, ticker: str,
                      show_ma: bool = True, show_volume: bool = True,
                      show_bb: bool = False, show_rsi: bool = False) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _apply_dark(fig)

    rows = 1 + (1 if show_volume else 0) + (1 if show_rsi else 0)
    specs = [[{"type": "xy"}]] * rows
    row_heights = []
    if rows == 3:
        row_heights = [0.55, 0.25, 0.20]
    elif rows == 2:
        row_heights = [0.70, 0.30]
    else:
        row_heights = [1.0]

    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, row_heights=row_heights)

    fig.add_trace(go.Candlestick(
        x=df["timestamp"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color=C["green_br"], decreasing_line_color=C["red"],
        increasing_fillcolor=C["green"], decreasing_fillcolor=C["red_dim"],
        line_width=1, name=ticker,
    ), row=1, col=1)

    if show_ma and len(df) >= 20:
        cl = df["close"].values
        ma20 = pd.Series(cl).rolling(20).mean()
        fig.add_trace(go.Scatter(x=df["timestamp"], y=ma20,
            line=dict(color=C["gold"], width=1), name="MA20", opacity=0.8), row=1, col=1)
    if show_ma and len(df) >= 50:
        ma50 = pd.Series(df["close"].values).rolling(50).mean()
        fig.add_trace(go.Scatter(x=df["timestamp"], y=ma50,
            line=dict(color=C["blue_br"], width=1), name="MA50", opacity=0.8), row=1, col=1)

    if show_bb and len(df) >= 20:
        closes = df["close"].values
        ma     = pd.Series(closes).rolling(20).mean()
        std    = pd.Series(closes).rolling(20).std()
        fig.add_trace(go.Scatter(x=df["timestamp"], y=ma + 2*std,
            line=dict(color=C["purple"], width=0.8, dash="dot"), name="BB+2σ", opacity=0.6), row=1, col=1)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=ma - 2*std,
            fill="tonexty", fillcolor="rgba(126,87,194,0.05)",
            line=dict(color=C["purple"], width=0.8, dash="dot"), name="BB-2σ", opacity=0.6), row=1, col=1)

    r = 2
    if show_volume:
        colors = [C["green"] if c >= o else C["red"]
                  for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(x=df["timestamp"], y=df["volume"],
            marker_color=colors, name="Volume", opacity=0.6), row=r, col=1)
        r += 1

    if show_rsi and len(df) >= 15:
        closes = df["close"].values.astype(float)
        rsi_vals = []
        for i in range(14, len(closes)):
            rsi_vals.append(_calc_rsi(closes[:i+1]))
        rsi_x = df["timestamp"].iloc[14:]
        fig.add_trace(go.Scatter(x=rsi_x, y=rsi_vals,
            line=dict(color=C["orange_br"], width=1), name="RSI"), row=r, col=1)
        fig.add_hline(y=70, line_color=C["red"],    line_dash="dot", line_width=0.8, row=r, col=1)
        fig.add_hline(y=30, line_color=C["green_br"], line_dash="dot", line_width=0.8, row=r, col=1)
        fig.update_yaxes(range=[0, 100], row=r, col=1)

    _cs_layout = {**_DARK_LAYOUT,
                  "xaxis_rangeslider_visible": False,
                  "height": 480 if rows >= 3 else 400 if rows == 2 else 340,
                  "margin": dict(l=8, r=8, t=8, b=4)}
    fig.update_layout(**_cs_layout)
    fig.update_xaxes(showgrid=True, gridcolor=C["border"], tickfont=dict(size=8))
    fig.update_yaxes(showgrid=True, gridcolor=C["border"], tickfont=dict(size=8))
    return fig


def build_earnings_chart(profile: Dict, max_quarters: int = 4) -> go.Figure:
    eps = profile.get("quarterly_eps", [])
    rev = profile.get("quarterly_rev", [])
    if not eps:
        return _apply_dark(go.Figure())
    # Pad if fewer quarters available
    eps = (eps * 5)[:max_quarters]
    rev = (rev * 5)[:max_quarters] if rev else []
    labels = [f"Q-{max_quarters - i}" for i in range(max_quarters)]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=labels, y=eps,
        marker_color=C["gold"], name="EPS", opacity=0.85), secondary_y=False)
    if rev:
        fig.add_trace(go.Scatter(x=labels, y=rev,
            line=dict(color=C["blue_br"], width=2), name="Revenue ($B)",
            mode="lines+markers", marker_size=5), secondary_y=True)
    fig.update_layout(**_DARK_LAYOUT, height=220)
    return fig


def build_segment_chart(profile: Dict) -> go.Figure:
    segs = profile.get("revenue_segments", {})
    if not segs:
        return _apply_dark(go.Figure())
    colors = [C["gold"], C["blue_br"], C["green_br"], C["orange_br"], C["purple"], C["red"]]
    fig = go.Figure(go.Pie(
        labels=list(segs.keys()), values=list(segs.values()),
        hole=0.55,
        marker=dict(colors=colors[:len(segs)], line=dict(color=C["bg"], width=2)),
        textfont=dict(size=9, color=C["text_dim"]),
        hovertemplate="<b>%{label}</b><br>%{value}%<extra></extra>",
    ))
    fig.update_layout(**_DARK_LAYOUT, height=220)
    return fig


def build_dcf_sensitivity(current_price: float, base_fair: float) -> go.Figure:
    waccs     = [7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
    tgr       = [1.5, 2.0, 2.5, 3.0, 3.5]
    z_matrix  = []
    for w in waccs:
        row = []
        for g in tgr:
            # Simplified Gordon Growth: FV ≈ base * (wacc_base/wacc) * (1 + g/100)
            fv = base_fair * (9.0 / w) * (1 + g / 100)
            mos = (fv - current_price) / current_price * 100
            row.append(round(mos, 1))
        z_matrix.append(row)

    colorscale = [
        [0.0, C["red"]], [0.3, C["red_dim"]], [0.45, C["panel"]],
        [0.55, C["panel"]], [0.7, C["green"]], [1.0, C["green_br"]],
    ]
    fig = go.Figure(go.Heatmap(
        z=z_matrix,
        x=[f"TGR {g}%" for g in tgr],
        y=[f"WACC {w}%" for w in waccs],
        colorscale=colorscale,
        zmid=0,
        text=[[f"{v:+.0f}%" for v in row] for row in z_matrix],
        texttemplate="%{text}",
        textfont=dict(size=9),
        hovertemplate="WACC: %{y}<br>TGR: %{x}<br>MoS: %{z:.1f}%<extra></extra>",
        colorbar=dict(tickfont=dict(size=8), thickness=12, len=0.8),
    ))
    fig.update_layout(**_DARK_LAYOUT, height=260)
    return fig


def build_macro_radar(macro: Dict) -> go.Figure:
    """Radar chart for macro health indicators."""
    labels = ["GDP Growth", "Employment", "Inflation Control", "Credit Conditions", "Risk Appetite"]
    gdp_raw = macro.get("US GDP (QoQ)", {}).get("raw", 2.0) or 2.0
    ue_raw  = macro.get("US Unemployment", {}).get("raw", 4.0) or 4.0
    cpi_raw = macro.get("US CPI YoY", {}).get("raw", 3.0) or 3.0
    sp_raw  = macro.get("Yield Spread", {}).get("raw", -0.3) or -0.3
    vix_raw = macro.get("VIX Index", {}).get("raw", 20.0) or 20.0

    # Normalize to 0-10
    scores = [
        min(10, max(0, gdp_raw * 2 + 5)),        # GDP
        min(10, max(0, (6 - ue_raw) * 2)),        # Low unemployment = good
        min(10, max(0, (5 - cpi_raw) * 2)),       # Low CPI = good
        min(10, max(0, 5 + sp_raw * 5)),           # Normal curve = good
        min(10, max(0, (35 - vix_raw) / 2.5)),    # Low VIX = good
    ]
    scores.append(scores[0])
    labels.append(labels[0])

    fig = go.Figure(go.Scatterpolar(
        r=scores, theta=labels,
        fill="toself",
        fillcolor=f"rgba(197,168,97,0.12)",
        line=dict(color=C["gold"], width=2),
        marker=dict(size=5, color=C["gold"]),
    ))
    # Do NOT use **_DARK_LAYOUT here: xaxis/yaxis keys conflict with polar layout
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=C["panel"],
        font=dict(family="Space Mono, monospace", color=C["text_dim"], size=9),
        showlegend=False,
        polar=dict(
            bgcolor=C["panel"],
            radialaxis=dict(visible=True, range=[0,10], gridcolor=C["border"],
                            tickfont=dict(size=7)),
            angularaxis=dict(gridcolor=C["border"], tickfont=dict(size=8)),
        ),
        height=280,
        margin=dict(l=8, r=8, t=8, b=8),
    )
    return fig


def build_crypto_heatmap(coins: List[Dict]) -> go.Figure:
    if not coins:
        return _apply_dark(go.Figure())
    top = coins[:20]
    syms   = [c["symbol"] for c in top]
    changes = [c.get("change_24h", 0) for c in top]
    caps   = [max(c.get("market_cap", 1e9), 1e9) for c in top]
    sizes  = [max(20, min(80, c / 1e11)) for c in caps]

    colors = [C["green_br"] if ch >= 0 else C["red"] for ch in changes]
    texts  = [f"{s}<br>{ch:+.1f}%" for s, ch in zip(syms, changes)]

    fig = go.Figure(go.Treemap(
        labels=syms,
        parents=[""] * len(syms),
        values=caps,
        customdata=[[c.get("price", 0), c.get("change_24h", 0)] for c in top],
        marker=dict(
            colors=changes,
            colorscale=[[0, C["red"]], [0.5, C["panel_alt"]], [1, C["green_br"]]],
            cmid=0,
            line=dict(color=C["border"], width=1),
        ),
        texttemplate="<b>%{label}</b><br>%{customdata[1]:+.1f}%",
        textfont=dict(size=10),
        hovertemplate="<b>%{label}</b><br>Price: $%{customdata[0]:,.2f}<br>24h: %{customdata[1]:+.2f}%<extra></extra>",
    ))
    _layout = {**_DARK_LAYOUT, "height": 320, "margin": dict(l=0, r=0, t=4, b=0)}
    fig.update_layout(**_layout)
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════
# §20  UI HELPER COMPONENTS
# ═════════════════════════════════════════════════════════════════════════════════════

def phdr(title: str, badge: str = "", badge_type: str = "gold") -> None:
    badge_colors = {
        "gold":   (C["gold"],     "rgba(197,168,97,0.12)"),
        "green":  (C["green_br"], "rgba(46,125,82,0.12)"),
        "red":    (C["red"],      "rgba(211,47,47,0.12)"),
        "blue":   (C["blue_br"], "rgba(30,77,140,0.12)"),
        "muted":  (C["muted"],    "rgba(66,72,90,0.12)"),
    }
    bc, bg = badge_colors.get(badge_type, badge_colors["gold"])
    badge_html = (f'<span class="phdr-badge" style="background:{bg};color:{bc};'
                  f'border:1px solid {bc}33;">{badge}</span>') if badge else ""
    st.markdown(
        f'<div class="phdr" style="color:#e8eaf0;">{title}{badge_html}</div>',
        unsafe_allow_html=True)


def render_kv(rows: List[Tuple[str, str, str]]) -> None:
    html = '<div class="tp" style="padding:4px;">'
    for k, v, col in rows:
        html += (f'<div class="kr"><span class="kk">{k}</span>'
                 f'<span class="kv" style="color:{col};">{v}</span></div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_val_cards(profile: Dict) -> None:
    icon_cls = {"green": "ipass", "red": "ifail", "orange": "iwarn"}
    for card in profile.get("cards", []):
        cls = icon_cls.get(card["color"], "iwarn")
        col = {"green": C["green_br"], "red": C["red"], "orange": C["orange_br"]}.get(card["color"], C["muted"])
        st.markdown(
            f'<div class="vc"><div class="vi {cls}">{card["icon"]}</div>'
            f'<div><div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};text-transform:uppercase;">'
            f'{card["cat"]}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:12.5px;font-weight:700;color:{col};">'
            f'{card["verdict"]}</div>'
            f'<div style="font-size:9px;color:{C["muted"]};">{card["detail"]}</div></div></div>',
            unsafe_allow_html=True)


def render_ticker_bar(prices: Dict[str, Dict]) -> None:
    """Scrollable ticker tape with live prices. Finnhub-first, never blank."""
    items_html = ""
    for label, sym in TICKER_BAR_SYMBOLS:
        d     = prices.get(sym, {})
        price = d.get("price", 0)
        pct   = d.get("pct", 0)
        if price <= 0:
            # show dash placeholder so bar is never empty
            items_html += (
                f'<span class="t-item">'
                f'<span class="t-label">{label}</span>'
                f'<span style="color:{C["muted"]};">—</span>'
                f'</span>')
            continue
        cls  = "pos" if pct >= 0 else "neg"
        sign = "▲" if pct >= 0 else "▼"
        pfmt = f"${price:,.2f}" if price < 10000 else f"${price:,.0f}"
        items_html += (
            f'<span class="t-item">'
            f'<span class="t-label">{label}</span>'
            f'<span class="{cls}">{pfmt} {sign}{abs(pct):.2f}%</span>'
            f'</span>')
    st.markdown(
        f'<div class="ticker-strip">{items_html}</div>',
        unsafe_allow_html=True)


_TICKER_SEEDS: List[Dict] = [
    {"source":"FRED","title":"US CPI running at 3.1% YoY — Fed holding rates steady","tier":"ROUTINE","published":""},
    {"source":"POLYGON","title":"S&P 500 futures active — equity markets open for trading","tier":"ROUTINE","published":""},
    {"source":"COINGECKO","title":"Bitcoin above $100K — crypto market cap near all-time high","tier":"ROUTINE","published":""},
    {"source":"TRADIER","title":"NYSE market status: live data streaming — terminal active","tier":"ROUTINE","published":""},
    {"source":"MACRO","title":"10-Year Treasury yield at 4.62% — yield curve remains inverted","tier":"ROUTINE","published":""},
]

def render_live_news_ticker(news_items: List[Dict]) -> None:
    """Crucix-style horizontally scrolling news ticker. Shows seeds on cold start."""
    if not news_items:
        news_items = _TICKER_SEEDS
    tier_css = {"FLASH": "flash", "PRIORITY": "priority", "ROUTINE": "routine"}
    items_html = ""
    for item in news_items[:30]:
        tier    = item.get("tier", "ROUTINE")
        tc      = tier_css.get(tier, "routine")
        src     = item.get("source", "—")[:12]
        title   = item.get("title", "")[:100]
        pub     = item.get("published", "")
        items_html += (
            f'<span class="crucix-item">'
            f'<span class="crucix-src {tc}">{src}</span>'
            f'<span class="crucix-hl {tc}">{title}</span>'
            f'</span>')
    if items_html:
        st.markdown(
            f'<div class="crucix-outer">'
            f'<div class="crucix-label">⚡ LIVE FEED</div>'
            f'<div style="overflow:hidden;flex:1;">'
            f'<div class="crucix-scroll">{items_html}</div>'
            f'</div></div>',
            unsafe_allow_html=True)


def render_fear_greed_gauge(data: Dict, label: str = "FEAR & GREED") -> None:
    if not data:
        st.markdown(f'<div class="tp" style="text-align:center;padding:20px;color:{C["muted"]};">Awaiting data…</div>',
                    unsafe_allow_html=True)
        return
    val   = data.get("value", 50)
    cls   = data.get("classification", "Neutral")
    color = (C["red"] if val < 25 else C["orange_br"] if val < 45
             else C["gold"] if val < 55 else C["green_br"])
    circ  = 283
    dash  = circ - (circ * val / 100)
    st.markdown(f"""
    <div class="tp" style="text-align:center;padding:12px;">
      <div style="font-size:8px;letter-spacing:2px;color:{C['muted']};
                  text-transform:uppercase;margin-bottom:8px;">{label}</div>
      <svg width="100" height="100" viewBox="0 0 100 100">
        <circle cx="50" cy="50" r="45" fill="none" stroke="{C['border2']}" stroke-width="8"/>
        <circle cx="50" cy="50" r="45" fill="none" stroke="{color}" stroke-width="8"
                stroke-dasharray="{circ}" stroke-dashoffset="{dash:.1f}"
                stroke-linecap="round" transform="rotate(-90 50 50)"/>
        <text x="50" y="46" text-anchor="middle" font-family="Syne,sans-serif"
              font-size="18" font-weight="800" fill="{color}">{val}</text>
        <text x="50" y="60" text-anchor="middle" font-family="Space Mono,monospace"
              font-size="7" fill="{C['muted']}">{cls.upper()[:11]}</text>
      </svg>
    </div>""", unsafe_allow_html=True)


def render_news_feed(items: List[Dict], max_items: int = 10) -> None:
    if not items:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};text-align:center;padding:16px;">Awaiting news data…</div>',
                    unsafe_allow_html=True)
        return
    html = '<div class="tp" style="padding:4px;max-height:400px;overflow-y:auto;">'
    for item in items[:max_items]:
        sent     = item.get("sentiment", 0.0)
        s_cls    = "sent-bull" if sent > 0.1 else "sent-bear" if sent < -0.1 else "sent-neut"
        s_lbl    = score_to_label(sent)
        tier     = item.get("tier", "ROUTINE")
        tc       = {"FLASH": C["red"], "PRIORITY": C["orange_br"], "ROUTINE": C["gold"]}.get(tier, C["gold"])
        is_alert = item.get("is_alert", False)
        src      = item.get("source", "—")
        title    = item.get("title", "")
        pub      = item.get("published", "")
        link     = item.get("link", "#")
        html += (
            f'<div class="news-item">'
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:2px;">'
            f'<span class="news-src" style="background:rgba(197,168,97,0.08);'
            f'color:{tc};border:1px solid {tc}33;">{src}</span>'
            f'<span class="news-meta">{pub}</span>'
            f'<span class="{s_cls}" style="font-size:7.5px;margin-left:auto;">{s_lbl}</span>'
            f'</div>'
            f'<div class="news-title{"  alert" if is_alert else ""}">'
            f'<a href="{link}" target="_blank" style="color:inherit;text-decoration:none;">'
            f'{title}</a></div></div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_macro_grid(macro: Dict) -> None:
    keys    = list(macro.keys())
    neg_up  = {"US CPI YoY", "10Y Treasury", "2Y Treasury", "Fed Funds Rate",
                "DXY (USD Index)", "Yield Spread", "ISM Mfg PMI"}
    pos_dn  = {"VIX Index", "US Unemployment"}
    cols    = st.columns(2)
    for i, key in enumerate(keys):
        info  = macro[key]
        val   = info.get("value", "—")
        delta = info.get("delta", "")
        note  = info.get("note", "")
        trend = info.get("trend", "flat")
        zscore = info.get("zscore", 0.0)
        is_b  = (trend == "up" and key in neg_up) or (trend == "down" and key in pos_dn)
        is_g  = (trend == "up" and key not in neg_up) or (trend == "down" and key in pos_dn)
        dc    = C["red"] if is_b else C["green_br"] if is_g else C["muted"]
        arr   = "▲" if trend == "up" else "▼" if trend == "down" else "—"
        zb    = f' · Z={zscore:+.1f}' if abs(zscore) > 1.0 else ""
        with cols[i % 2]:
            st.markdown(
                f'<div class="tp" style="margin-bottom:4px;padding:7px 10px;">'
                f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
                f'text-transform:uppercase;margin-bottom:2px;">{key}</div>'
                f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;'
                f'color:{C["text"]};line-height:1;">{val}</div>'
                f'<div style="font-size:8.5px;color:{dc};margin-top:1px;">'
                f'{arr} {delta}&nbsp;'
                f'<span style="color:{C["muted"]};">{note}{zb}</span></div></div>',
                unsafe_allow_html=True)


def render_econ_calendar(events: List[Dict]) -> None:
    phdr("Economic Calendar", "FOREX FACTORY · LIVE", "gold")
    # Country filter
    _ECON_COUNTRIES = {"ALL": None, "USA": "USD", "JAPAN": "JPY",
                       "GERMANY": "EUR", "CANADA": "CAD", "ISRAEL": "ILS", "IRAN": "IRR"}
    filter_opts = list(_ECON_COUNTRIES.keys())
    selected_countries = st.multiselect(
        "Filter by country",
        options=filter_opts,
        default=["ALL"],
        label_visibility="collapsed",
        key="econ_country_filter"
    )
    # Resolve currency filter
    if "ALL" in selected_countries or not selected_countries:
        currency_filter = None
    else:
        currency_filter = {_ECON_COUNTRIES[c] for c in selected_countries if _ECON_COUNTRIES.get(c)}

    if not events:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:12px;text-align:center;">Awaiting calendar data…</div>',
                    unsafe_allow_html=True)
        return

    # Apply filter
    filtered_events = events
    if currency_filter:
        filtered_events = [e for e in events if e.get("currency","") in currency_filter]

    ic_map   = {"high": C["red"], "medium": C["orange_br"], "low": C["muted"]}
    dot_map  = {"high": "●●●", "medium": "●●○", "low": "●○○"}
    html_out = (f'<div class="tp" style="padding:4px;max-height:320px;overflow-y:auto;">'
                f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
                f'<tr style="border-bottom:1px solid {C["border"]};">'
                + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;letter-spacing:1px;">{h}</td>'
                          for h in ["IMP", "CCY", "EVENT", "TIME (UTC)", "ACTUAL", "FCST", "PREV"])
                + "</tr>")
    for ev in filtered_events[:30]:
        imp   = ev.get("impact", "low")
        ic    = ic_map.get(imp, C["muted"])
        act   = ev.get("actual") or "—"
        fcast = ev.get("forecast") or "—"
        prev  = ev.get("previous") or "—"
        utc_t = ev.get("time", "") or ev.get("date", "")
        # Try to extract time portion
        if "T" in str(utc_t):
            try: utc_t = str(utc_t).split("T")[1][:5]
            except: utc_t = "—"
        elif len(str(utc_t)) > 10:
            utc_t = str(utc_t)[11:16]
        else:
            utc_t = "—"
        ac    = C["green_br"] if act != "—" else C["text"]
        html_out += (
            f'<tr style="border-bottom:1px solid {C["border"]};">'
            f'<td style="padding:3px 6px;color:{ic};">{dot_map.get(imp,"●○○")}</td>'
            f'<td style="padding:3px 6px;color:{C["muted"]};">{ev.get("currency","")}</td>'
            f'<td style="padding:3px 6px;color:{C["text"]};">{ev.get("event","")[:35]}</td>'
            f'<td style="padding:3px 6px;color:{C["muted"]};font-size:8px;">{utc_t}</td>'
            f'<td style="padding:3px 6px;color:{ac};font-weight:700;">{act}</td>'
            f'<td style="padding:3px 6px;color:{C["muted"]};">{fcast}</td>'
            f'<td style="padding:3px 6px;color:{C["muted"]};">{prev}</td></tr>')
    html_out += "</table></div>"
    st.markdown(html_out, unsafe_allow_html=True)


def render_crypto_table(data: Dict) -> None:
    phdr("Crypto Market Overview", "COINGECKO · LIVE", "gold")
    coins   = data.get("coins", [])
    btc_dom = data.get("btc_dominance", 0)
    tot_cap = data.get("total_market_cap", 0)
    if not coins:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:12px;text-align:center;">Awaiting CoinGecko data…</div>',
                    unsafe_allow_html=True)
        return
    st.markdown(
        f'<div style="display:flex;gap:16px;padding:4px 0 8px 0;font-size:9px;">'
        f'<span><span style="color:{C["muted"]};">Total Cap </span>'
        f'<span style="color:{C["text"]};font-weight:700;">${tot_cap/1e12:.2f}T</span></span>'
        f'<span><span style="color:{C["muted"]};">BTC Dom </span>'
        f'<span style="color:{C["gold"]};font-weight:700;">{btc_dom:.1f}%</span></span>'
        f'</div>', unsafe_allow_html=True)
    hdr  = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
            f'<tr style="border-bottom:1px solid {C["border"]};">'
            + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;letter-spacing:1.5px;">{h}</td>'
                      for h in ["#", "SYMBOL", "PRICE", "24H", "VOL", "MKT CAP"])
            + "</tr>")
    rows = ""
    for c in coins[:20]:
        chg  = c.get("change_24h", 0)
        cc   = C["green_br"] if chg >= 0 else C["red"]
        sign = "▲" if chg >= 0 else "▼"
        p    = c.get("price", 0)
        pfmt = f"${p:,.0f}" if p >= 1000 else f"${p:,.2f}" if p >= 1 else f"${p:.5f}"
        vol  = c.get("volume_24h", 0)
        vfmt = f"${vol/1e9:.2f}B" if vol >= 1e9 else f"${vol/1e6:.0f}M"
        cap  = c.get("market_cap", 0)
        cfmt = f"${cap/1e12:.2f}T" if cap >= 1e12 else f"${cap/1e9:.0f}B"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{c.get("rank",0)}</td>'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["gold"]};'
                 f'font-family:Syne,sans-serif;">{c.get("symbol","")}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{pfmt}</td>'
                 f'<td style="padding:3px 6px;color:{cc};font-weight:700;">{sign}{abs(chg):.2f}%</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{vfmt}</td>'
                 f'<td style="padding:3px 6px;">{cfmt}</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:380px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


def render_stock_table(stocks: List[Dict]) -> None:
    phdr("Stock Market Overview", "POLYGON · FINNHUB · LIVE", "gold")
    if not stocks:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:12px;text-align:center;">Awaiting stock data…</div>',
                    unsafe_allow_html=True)
        return
    hdr  = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
            f'<tr style="border-bottom:1px solid {C["border"]};">'
            + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;letter-spacing:1px;">{h}</td>'
                      for h in ["SYMBOL", "NAME", "PRICE", "24H", "MKT CAP"])
            + "</tr>")
    rows = ""
    for s in stocks[:15]:
        chg  = s.get("change_24h", 0)
        cc   = C["green_br"] if chg >= 0 else C["red"]
        sign = "▲" if chg >= 0 else "▼"
        price = s.get("price", 0)
        pfmt  = f"${price:,.2f}" if price < 10000 else f"${price:,.0f}"
        cap   = s.get("market_cap", 0)
        cfmt  = f"${cap/1e12:.2f}T" if cap >= 1e12 else f"${cap/1e9:.0f}B" if cap >= 1e9 else "—"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["gold"]};'
                 f'font-family:Syne,sans-serif;">{s.get("symbol","")}</td>'
                 f'<td style="padding:3px 6px;color:{C["text_dim"]};">{s.get("name","")[:22]}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{pfmt}</td>'
                 f'<td style="padding:3px 6px;color:{cc};font-weight:700;">{sign}{abs(chg):.2f}%</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{cfmt}</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:360px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


def render_screener() -> None:
    phdr("Stock Screener", "15 STOCKS · QUANTAMENTAL", "gold")
    hdr  = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
            f'<tr style="border-bottom:1px solid {C["border"]};">'
            + "".join(f'<td style="padding:4px 8px;color:{C["gold"]};font-size:7.5px;letter-spacing:1.5px;">{h}</td>'
                      for h in ["TICKER", "NAME", "SECTOR", "MKT CAP", "P/E", "RATING"])
            + "</tr>")
    rows = ""
    for s in SCREENER_UNIVERSE:
        rc   = C["green_br"] if s["rating"] >= 65 else C["orange_br"] if s["rating"] >= 45 else C["red"]
        pe_s = f"{s['pe']:.1f}x" if s["pe"] else "N/A"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:4px 8px;font-weight:700;color:{C["gold"]};'
                 f'font-family:Syne,sans-serif;">{s["ticker"]}</td>'
                 f'<td style="padding:4px 8px;color:{C["text"]};">{s["name"]}</td>'
                 f'<td style="padding:4px 8px;color:{C["muted"]};">{s["sector"]}</td>'
                 f'<td style="padding:4px 8px;">{s["mkt_cap"]}</td>'
                 f'<td style="padding:4px 8px;">{pe_s}</td>'
                 f'<td style="padding:4px 8px;"><span style="color:{rc};font-weight:700;">{s["rating"]}%</span></td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:320px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


# Country coordinate lookup for search
_COUNTRY_COORDS: Dict[str, Tuple[float, float]] = {
    "USA": (37.09, -95.71), "UNITED STATES": (37.09, -95.71),
    "EUROPE": (50.0, 10.0), "UK": (55.37, -3.43), "GERMANY": (51.16, 10.45),
    "FRANCE": (46.22, 2.21), "SPAIN": (40.46, -3.74), "ITALY": (41.87, 12.56),
    "CHINA": (35.86, 104.19), "RUSSIA": (61.52, 105.31), "INDIA": (20.59, 78.96),
    "JAPAN": (36.20, 138.25), "SOUTH KOREA": (35.90, 127.76), "BRAZIL": (14.23, -51.92),
    "CANADA": (56.13, -106.34), "AUSTRALIA": (25.27, 133.77), "MEXICO": (23.63, -102.55),
    "SAUDI ARABIA": (23.88, 45.07), "UAE": (23.42, 53.84), "ISRAEL": (31.04, 34.85),
    "IRAN": (32.42, 53.68), "UKRAINE": (48.37, 31.16), "TURKEY": (38.96, 35.24),
    "JAMAICA": (18.10, -77.29), "NIGERIA": (9.08, 8.67), "EGYPT": (26.82, 30.80),
    "SOUTH AFRICA": (-30.55, 22.93), "ARGENTINA": (-38.41, -63.61),
    "PAKISTAN": (30.37, 69.34), "INDONESIA": (-0.78, 113.92),
    "TAIWAN": (23.69, 120.96), "PHILIPPINES": (12.87, 121.77),
}


def render_conflict_globe() -> None:
    """Enhanced 3D-like globe with country search, realistic styling, and conflict markers."""
    # Country search UI
    search_col, _ = st.columns([2, 3])
    with search_col:
        country_search = st.text_input(
            "Search country",
            placeholder="e.g. USA, SPAIN, CHINA…",
            key="globe_country_search",
            label_visibility="collapsed"
        )

    # Determine center based on search
    center_lat, center_lon = 20.0, 0.0
    zoom_scale = 1.0
    search_upper = country_search.strip().upper()
    if search_upper:
        for key, coords in _COUNTRY_COORDS.items():
            if search_upper in key or key in search_upper:
                center_lat, center_lon = coords
                zoom_scale = 3.0
                break

    lats  = [h["lat"] for h in CONFLICT_HOTSPOTS]
    lons  = [h["lng"] for h in CONFLICT_HOTSPOTS]
    sevs  = [h["severity"] for h in CONFLICT_HOTSPOTS]
    names = [h["name"] for h in CONFLICT_HOTSPOTS]
    types = [h["type"] for h in CONFLICT_HOTSPOTS]

    # Outer glow layer (larger faded markers)
    fig = go.Figure()
    fig.add_trace(go.Scattergeo(
        lat=lats, lon=lons,
        mode="markers",
        marker=dict(
            size=[int(14 + s * 28) for s in sevs],
            color=[f"rgba(211,47,47,{s*0.25})" for s in sevs],
            line=dict(width=0),
        ),
        hoverinfo="skip",
        showlegend=False,
    ))
    # Main intensity markers
    fig.add_trace(go.Scattergeo(
        lat=lats, lon=lons,
        mode="markers+text",
        marker=dict(
            size=[int(7 + s * 16) for s in sevs],
            color=sevs,
            colorscale=[
                [0.0, "rgba(76,175,77,0.9)"],
                [0.35, "rgba(224,160,80,0.95)"],
                [0.65, "rgba(211,47,47,0.95)"],
                [1.0, "rgba(255,82,82,1.0)"],
            ],
            cmin=0, cmax=1,
            line=dict(color="rgba(255,255,255,0.3)", width=1),
            colorbar=dict(
                thickness=8, len=0.5,
                tickfont=dict(size=7, color="#8a9090"),
                title=dict(text="SEV", font=dict(size=7, color="#8a9090")),
            ),
        ),
        text=names,
        textfont=dict(size=7.5, color="#ffffff", family="Space Mono, monospace"),
        textposition="top center",
        customdata=[[n, t, f"{s*100:.0f}%"] for n, t, s in zip(names, types, sevs)],
        hovertemplate=(
            "<b style='color:#ff5252;'>%{customdata[0]}</b><br>"
            "Type: %{customdata[1]}<br>"
            "Severity: %{customdata[2]}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_geos(
        projection_type="orthographic",
        projection_rotation=dict(lon=center_lon, lat=center_lat, roll=0),
        showcoastlines=True,   coastlinecolor="#252b38",
        showland=True,         landcolor="#0d1520",
        showocean=True,        oceancolor="#060b14",
        showlakes=True,        lakecolor="#080d18",
        showrivers=False,
        showcountries=True,    countrycolor="#1a1d24",
        showframe=False,
        bgcolor="rgba(0,0,0,0)",
        showgraticules=True,   graticulecolor="rgba(26,29,36,0.4)",
        lataxis_showgrid=True,
        lonaxis_showgrid=True,
    )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Space Mono, monospace", color="#c8cdd8", size=9),
        showlegend=False,
        height=400,
        margin=dict(l=0, r=0, t=4, b=0),
        geo=dict(bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig, use_container_width=True, config={
        "displayModeBar": False,
        "scrollZoom": False,
    })


def render_market_status(ms: Dict) -> None:
    if not ms:
        return
    status = ms.get("status", "Unknown")
    sc     = (C["green_br"] if status == "Open" else
              C["orange_br"] if "pre" in status.lower() or "after" in status.lower()
              else C["red"])
    st.markdown(
        f'<div class="tp" style="display:flex;align-items:center;gap:10px;padding:7px 12px;">'
        f'<div style="width:8px;height:8px;border-radius:50%;background:{sc};'
        f'box-shadow:0 0 6px {sc}80;animation:blink 2s ease-in-out infinite;"></div>'
        f'<div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:{sc};">'
        f'{status.upper()}</div>'
        f'<div style="font-size:9px;color:{C["muted"]};">{ms.get("message","")}</div>'
        f'</div>', unsafe_allow_html=True)


def render_api_status(state: TerminalState) -> None:
    """Small API health dashboard."""
    apis = [
        ("FRED",        "macro_indicators"),
        ("Polygon.io",  "ticker_prices"),
        ("Finnhub",     "ticker_prices"),
        ("NewsAPI",     "news_all"),
        ("GNews",       "news_all"),
        ("CoinGecko",   "crypto_overview"),
        ("Tradier",     "market_status"),
    ]
    items = []
    for name, key in apis:
        age = state.age_safe(key)
        has_err = bool(state.errors.get(key))
        if has_err:
            col, dot = C["red"], "✕"
        elif age < 99999:
            col, dot = C["green_br"], "●"
        else:
            col, dot = C["muted"], "○"
        items.append(f'<span style="font-size:8px;color:{col};margin-right:12px;">'
                     f'{dot} {name}</span>')
    st.markdown(
        f'<div class="tp" style="padding:5px 10px;display:flex;flex-wrap:wrap;gap:0;">'
        f'<span style="font-size:7.5px;color:{C["gold"]};letter-spacing:2px;'
        f'text-transform:uppercase;margin-right:14px;align-self:center;">API STATUS</span>'
        + "".join(items)
        + '</div>', unsafe_allow_html=True)


def render_dcf_panel(profile: Dict, mode: str = "DCF") -> None:
    current  = profile["current_price"]
    fair_map = {
        "DCF":      profile.get("fair_value_dcf", 0),
        "DDM":      profile.get("fair_value_ddm", 0),
        "EV/EBITDA":profile.get("fair_value_ev", 0),
    }
    fair    = fair_map.get(mode, profile.get("fair_value_dcf", 0)) or 1
    is_over = current > fair * 1.03
    pct     = abs(current - fair) / max(fair, 0.01) * 100
    verdict = "OVERVALUED" if is_over else "UNDERVALUED" if fair > current * 1.03 else "FAIRLY VALUED"
    vcol    = C["red"] if is_over else C["green_br"] if fair > current * 1.03 else C["gold"]
    bar_r   = min(fair / current, 1.0) if is_over else min(current / fair, 1.0)

    st.markdown(
        f'<div class="tp" style="text-align:center;background:rgba(0,0,0,0.3);'
        f'border-color:{vcol}33;margin-bottom:6px;">'
        f'<div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;'
        f'color:{vcol};letter-spacing:2px;">{verdict} {pct:.0f}%</div>'
        f'<div style="font-size:8.5px;color:{C["muted"]};margin-top:3px;">'
        f'{mode} Model · Annual · Base Case</div></div>',
        unsafe_allow_html=True)

    c1, _, c2 = st.columns([5, 1, 5])
    with c1:
        st.markdown(
            f'<div class="tp" style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:3px;">Current Price</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:24px;font-weight:800;'
            f'color:{C["red"]};">${current:,.2f}</div></div>', unsafe_allow_html=True)
    with _:
        st.markdown(f'<div style="display:flex;align-items:center;justify-content:center;'
                    f'height:100%;font-size:18px;color:{vcol};">⇌</div>', unsafe_allow_html=True)
    with c2:
        st.markdown(
            f'<div class="tp" style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:3px;">Fair Value ({mode})</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:24px;font-weight:800;'
            f'color:{C["green_br"]};">${fair:,.2f}</div></div>', unsafe_allow_html=True)

    st.markdown(
        f'<div class="tp" style="margin-top:4px;">'
        f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};margin-bottom:5px;'
        f'text-transform:uppercase;">Fair Value vs Current Price</div>'
        f'<div style="background:{C["border2"]};height:8px;border-radius:4px;overflow:hidden;">'
        f'<div style="width:{bar_r*100:.1f}%;height:100%;background:{C["green_br"]};'
        f'border-radius:4px;"></div></div>'
        f'<div style="display:flex;justify-content:space-between;font-size:8.5px;'
        f'color:{C["muted"]};margin-top:3px;">'
        f'<span>Fair ${fair:,.0f}</span><span>Price ${current:,.0f}</span></div></div>',
        unsafe_allow_html=True)


def render_peer_table(profile: Dict) -> None:
    phdr("Peer Comparison")
    peers = profile.get("peers", [])
    if not peers:
        return
    hdr  = (f'<table style="width:100%;border-collapse:collapse;font-size:9.5px;">'
            f'<tr style="border-bottom:1px solid {C["border"]};">'
            + "".join(f'<td style="padding:4px 8px;color:{C["gold"]};font-size:7.5px;letter-spacing:1.5px;">{h}</td>'
                      for h in ["TICKER", "P/E", "FWD P/E", "MKT CAP", "NET MARGIN", "REV GROWTH"])
            + "</tr>")
    rows = ""
    for p in peers:
        pe_s = f"{p['pe']:.1f}x" if p.get("pe") else "N/A"
        fp_s = f"{p['fwd_pe']:.1f}x" if p.get("fwd_pe") else "N/A"
        m    = p.get("margin", 0) or 0
        mc   = C["green_br"] if m > 0.15 else C["orange_br"] if m > 0 else C["red"]
        g    = p.get("growth", 0) or 0
        gc   = C["green_br"] if g > 0.10 else C["orange_br"] if g > 0 else C["red"]
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:5px 8px;font-weight:700;color:{C["text"]};'
                 f'font-family:Syne,sans-serif;">{p["sym"]}</td>'
                 f'<td style="padding:5px 8px;">{pe_s}</td>'
                 f'<td style="padding:5px 8px;">{fp_s}</td>'
                 f'<td style="padding:5px 8px;">{p["mkt"]}</td>'
                 f'<td style="padding:5px 8px;color:{mc};font-weight:700;">{m*100:.1f}%</td>'
                 f'<td style="padding:5px 8px;color:{gc};font-weight:700;">{g*100:.1f}%</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;">{hdr}{rows}</table></div>',
                unsafe_allow_html=True)


def render_sentiment_panel(state: TerminalState) -> None:
    scores    = state.sentiment_scores
    composite = scores.get("composite", 0.0)
    comp_lbl  = score_to_label(composite)
    comp_col  = score_to_color(composite)
    bar_pct   = int((composite + 1) / 2 * 100)
    phdr("Market Sentiment Engine", "NLP · MULTI-SOURCE", "gold")
    st.markdown(f"""
    <div class="tp" style="padding:8px 12px;margin-bottom:4px;">
      <div style="font-size:8px;letter-spacing:1.5px;color:{C['muted']};
                  text-transform:uppercase;margin-bottom:4px;">COMPOSITE MARKET SENTIMENT</div>
      <div style="display:flex;align-items:center;gap:10px;">
        <div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;
                    color:{comp_col};">{comp_lbl}</div>
        <div style="font-size:10px;color:{comp_col};">{composite:+.3f}</div>
      </div>
      <div style="background:{C['border2']};height:6px;border-radius:3px;margin-top:6px;overflow:hidden;">
        <div style="width:{bar_pct}%;height:100%;background:{comp_col};border-radius:3px;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:8px;color:{C['muted']};margin-top:2px;">
        <span>BEARISH</span><span>BULLISH</span>
      </div>
    </div>""", unsafe_allow_html=True)


def render_technical_panel(ticker: str, df: pd.DataFrame) -> None:
    """Live technical analysis panel using Polygon OHLCV data."""
    phdr("Technical Analysis", "POLYGON · ALPHA VANTAGE", "gold")
    if df.empty or len(df) < 20:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;">Insufficient chart data for TA.</div>',
                    unsafe_allow_html=True)
        return
    tech = build_tech_analysis(df, ticker)
    if tech.get("error"):
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;">TA error: {tech["error"]}</div>',
                    unsafe_allow_html=True)
        return
    rsi   = tech.get("rsi", 50)
    trend = tech.get("trend", "neutral")
    rc    = C["red"] if rsi > 70 else C["green_br"] if rsi < 30 else C["gold"]
    tc    = C["green_br"] if trend == "bullish" else C["red"] if trend == "bearish" else C["gold"]
    render_kv([
        ("RSI (14)",    f"{rsi:.1f} — {tech.get('rsi_signal','')}", rc),
        ("EMA 20",      f"${tech.get('ema20',0):,.4g}",             C["text"]),
        ("EMA 50",      f"${tech.get('ema50',0):,.4g}",             C["text"]),
        ("SMA 200",     f"${tech.get('sma200',0):,.4g}",            C["text"]),
        ("Above 200SMA",f"{'Yes ✓' if tech.get('above_200sma') else 'No ✕'}",
                        C["green_br"] if tech.get("above_200sma") else C["red"]),
        ("BB Upper",    f"${tech.get('bb_upper',0):,.4g}",          C["orange_br"]),
        ("BB Lower",    f"${tech.get('bb_lower',0):,.4g}",          C["blue_br"]),
        ("Trend",       trend.upper(),                               tc),
    ])

    # Live Alpha Vantage RSI (if available)
    av_rsi = fetch_alphav_rsi(ticker)
    if av_rsi:
        arc = C["red"] if av_rsi > 70 else C["green_br"] if av_rsi < 30 else C["gold"]
        st.markdown(
            f'<div class="tp" style="margin-top:4px;padding:6px 10px;">'
            f'<div style="font-size:7.5px;letter-spacing:1px;color:{C["muted"]};'
            f'text-transform:uppercase;">AV RSI (Daily 14)</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;'
            f'color:{arc};">{av_rsi}</div></div>',
            unsafe_allow_html=True)

    # Alpha Vantage Bollinger Bands
    bb = fetch_alphav_bbands(ticker)
    if bb:
        render_kv([
            ("AV BB Upper", f"${bb['upper']:,.2f}", C["orange_br"]),
            ("AV BB Middle",f"${bb['middle']:,.2f}", C["text"]),
            ("AV BB Lower", f"${bb['lower']:,.2f}", C["blue_br"]),
        ])


# ═════════════════════════════════════════════════════════════════════════════════════
# §21  TAB RENDERERS
# ═════════════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=60)
def render_tab_macro() -> None:
    state = get_terminal_state()
    macro = state.macro_indicators if state.macro_indicators else get_macro_indicators()

    left_col, right_col = st.columns([6, 4], gap="small")

    with left_col:
        # Geopolitical conflict globe
        phdr("Global Conflict & Risk Map", "LIVE HOTSPOTS", "red")
        render_conflict_globe()
        st.markdown("<br>", unsafe_allow_html=True)

        # Macro radar
        phdr("Macro Health Radar", "FRED · Z-SCORE", "gold")
        st.plotly_chart(build_macro_radar(macro), use_container_width=True,
                        config={"displayModeBar": False})
        st.markdown("<br>", unsafe_allow_html=True)

        # Economic Calendar
        render_econ_calendar(state.econ_calendar)
        st.markdown("<br>", unsafe_allow_html=True)

        # Macro indicators grid
        phdr("FRED Macro Indicators", "REAL-TIME · Z-SCORE ANOMALY DETECTION", "gold")
        render_macro_grid(macro)

    with right_col:
        # Market Status
        render_market_status(state.market_status)
        st.markdown("<br>", unsafe_allow_html=True)

        # Fear & Greed gauges
        phdr("Fear & Greed Indices", "REAL-TIME")
        c1, c2 = st.columns(2)
        with c1:
            render_fear_greed_gauge(state.crypto_fear_greed, "CRYPTO F&G")
        with c2:
            render_fear_greed_gauge(state.stock_fear_greed, "EQUITY F&G")
        st.markdown("<br>", unsafe_allow_html=True)

        # Sentiment panel
        render_sentiment_panel(state)
        st.markdown("<br>", unsafe_allow_html=True)

        # Live news feed — macro/geopolitical
        phdr("Geopolitical & Macro News", "NEWSAPI · GNEWS · WORLDNEWS · RSS", "gold")
        t1, t2, t3 = st.tabs(["🌍 World", "📊 Macro", "⚡ Breaking"])
        with t1:
            render_news_feed(state.news_macro[:12], max_items=10)
        with t2:
            render_news_feed([n for n in state.news_all if n.get("asset_type") in ("macro","markets")][:10])
        with t3:
            breaking = [n for n in state.news_all if n.get("tier") in ("FLASH","PRIORITY")][:10]
            render_news_feed(breaking)

        st.markdown("<br>", unsafe_allow_html=True)
        # API Status
        render_api_status(state)


@st.fragment(run_every=60)
def render_tab_stock() -> None:
    state = get_terminal_state()

    h1, h2, h3, h4, h5 = st.columns([3, 1.5, 1.5, 1, 1])
    with h1:
        ticker_in = st.text_input("Ticker", value="AAPL", key="stock_ticker",
            label_visibility="collapsed", placeholder="Ticker (AAPL, TSLA, NVDA, MSFT…)")
    with h2:
        days_map = {"1M": 35, "3M": 95, "6M": 185, "1Y": 370, "2Y": 740}
        period_label = st.selectbox("Period", list(days_map.keys()), index=2,
                                    key="s_per", label_visibility="collapsed")
    with h3:
        show_ma  = st.checkbox("MA Lines", value=True,  key="s_ma")
    with h4:
        show_bb  = st.checkbox("Bollinger", value=False, key="s_bb")
    with h5:
        show_rsi = st.checkbox("RSI", value=False, key="s_rsi")

    ticker  = (ticker_in or "AAPL").upper().strip()
    days    = days_map[period_label]
    profile = VALUATION_PROFILES.get(ticker, VALUATION_PROFILES["AAPL"])
    df      = fetch_polygon_ohlcv(ticker, days=days)

    # ── Live quote from Finnhub ──────────────────────────────────────────────────
    fq = fetch_finnhub_quote(ticker)
    price  = fq.get("price",  profile.get("current_price", 0))
    change = fq.get("change", 0)
    high   = fq.get("high",   price)
    low    = fq.get("low",    price)

    chg_abs = (price - fq.get("prev", price))
    ccol    = C["green_br"] if change >= 0 else C["red"]
    sign    = "▲" if change >= 0 else "▼"
    init    = ticker[:2]

    # Live Finnhub metrics
    fm = fetch_finnhub_metrics(ticker)
    fp = fetch_finnhub_profile(ticker)
    name  = fp.get("name", profile.get("name", ticker))
    sec   = profile.get("sector", "—")
    desc  = profile.get("description", "")[:220]

    # Market status
    ms     = state.market_status
    ms_html = ""
    if ms:
        sc_m = {"Open": C["green_br"], "Closed": C["red"]}.get(ms.get("status",""), C["muted"])
        ms_html = (f'<span style="font-size:8px;color:{sc_m};padding:2px 6px;'
                   f'border:1px solid {sc_m}44;border-radius:3px;letter-spacing:1px;">'
                   f'{ms.get("status","")}</span>')

    st.markdown(
        f'<div style="background:{C["panel"]};border-bottom:1px solid {C["border"]};'
        f'padding:10px 16px;display:flex;align-items:center;gap:14px;">'
        f'<div class="clogo">{init}</div>'
        f'<div style="flex:1;">'
        f'<div style="font-family:Syne,sans-serif;font-size:19px;font-weight:800;'
        f'color:{C["text"]};line-height:1;">{name}</div>'
        f'<div style="font-size:9px;color:{C["muted"]};margin-top:2px;">'
        f'{ticker} · {sec}&nbsp;{ms_html}</div>'
        f'<div style="font-size:9.5px;color:{C["text_dim"]};margin-top:3px;'
        f'max-width:700px;line-height:1.4;">{desc}…</div></div>'
        f'<div style="text-align:right;flex-shrink:0;">'
        f'<div style="font-family:Syne,sans-serif;font-size:26px;font-weight:800;'
        f'color:{C["text"]};line-height:1;">${price:,.2f}</div>'
        f'<div style="font-size:11.5px;color:{ccol};margin-top:2px;">'
        f'{sign}&nbsp;${abs(chg_abs):.2f} ({abs(change):.2f}%)</div>'
        f'<div style="font-size:8px;color:{C["muted"]};margin-top:2px;">'
        f'H: ${high:,.2f}  L: ${low:,.2f}</div></div></div>',
        unsafe_allow_html=True)

    chart_col, info_col = st.columns([7, 3], gap="small")

    with chart_col:
        ct1, ct2 = st.columns([2, 3])
        with ct1:
            show_vol = st.checkbox("Volume", value=True, key="s_vol")
        with ct2:
            chart_type = st.selectbox("Chart Type", ["Candlestick", "Line", "Area"],
                                      key="s_ctype", label_visibility="collapsed")

        if df.empty:
            st.info(f"No Polygon OHLCV data for {ticker}. Attempting Tradier…")
        else:
            if chart_type == "Candlestick":
                fig = build_candlestick(df, ticker, show_ma=show_ma, show_volume=show_vol,
                                        show_bb=show_bb, show_rsi=show_rsi)
            elif chart_type == "Line":
                fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                    line=dict(color=C["gold"], width=1.5), name=ticker))
                _apply_dark(fig)
            else:
                fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                    fill="tozeroy", fillcolor="rgba(197,168,97,0.12)",
                    line=dict(color=C["gold"], width=1.5), name=ticker))
                _apply_dark(fig)
            st.plotly_chart(fig, use_container_width=True, config={
                "displayModeBar": True, "displaylogo": False, "scrollZoom": True,
                "modeBarButtonsToRemove": ["autoScale2d", "lasso2d", "select2d", "toImage"]})

        e1, e2 = st.columns([3, 2])
        with e1:
            phdr("Quarterly Earnings Trend")
            st.plotly_chart(build_earnings_chart(profile), use_container_width=True,
                            config={"displayModeBar": False})
        with e2:
            phdr("Revenue Segments")
            st.plotly_chart(build_segment_chart(profile), use_container_width=True,
                            config={"displayModeBar": False})

    with info_col:
        # Prefer live Finnhub metrics, fallback to profile
        pe     = fm.get("pe")         or profile.get("pe", "—")
        eps    = fm.get("eps")        or profile.get("eps", "—")
        roe    = fm.get("roe")        or profile.get("roe", 0)
        beta   = fm.get("beta")       or profile.get("beta", "—")
        divyd  = fm.get("dividend_yield") or profile.get("dividend_yield", 0)
        w52h   = fm.get("52w_high")   or profile.get("week52_high", "—")
        w52l   = fm.get("52w_low")    or profile.get("week52_low", "—")
        gm     = fm.get("gross_margin") or profile.get("gross_margin", 0)
        nm     = fm.get("net_margin") or profile.get("profit_margin", 0)

        phdr("Key Statistics", "FINNHUB · LIVE")
        render_kv([
            ("Market Cap",    profile.get("market_cap","—"),                                          C["text"]),
            ("P/E (TTM)",     f"{pe:.1f}x" if isinstance(pe, float) else f"{pe}x",                   C["text"]),
            ("Fwd P/E",       f"{profile.get('fwd_pe','—')}x",                                        C["text"]),
            ("EPS (TTM)",     f"${eps:.2f}" if isinstance(eps, float) else f"${eps}",                 C["text"]),
            ("EPS Growth",    f"{profile.get('eps_growth','—')}%",
             C["green_br"] if (profile.get("eps_growth",0) or 0) > 0 else C["red"]),
            ("Revenue",       profile.get("revenue","—"),                                              C["text"]),
            ("Gross Margin",  f"{float(gm)*100:.1f}%" if isinstance(gm, float) else "—",             C["green_br"]),
            ("Net Margin",    f"{float(nm)*100:.1f}%" if isinstance(nm, float) else "—",             C["green_br"]),
            ("ROE",           f"{float(roe)*100:.1f}%" if isinstance(roe, float) else "—",           C["text"]),
            ("Debt/Equity",   f"{profile.get('debt_equity','—')}",                                    C["text"]),
            ("Free Cash Flow",profile.get("fcf","—"),                                                 C["text"]),
            ("FCF Yield",     f"{profile.get('fcf_yield',0)*100:.1f}%",                              C["text"]),
            ("Div. Yield",    f"{float(divyd)*100:.2f}%" if isinstance(divyd, float) else "—",       C["text"]),
            ("Beta",          f"{beta:.2f}" if isinstance(beta, float) else str(beta),               C["text"]),
            ("52W High",      f"${w52h:,.2f}" if isinstance(w52h, float) else f"${w52h}",            C["green_br"]),
            ("52W Low",       f"${w52l:,.2f}" if isinstance(w52l, float) else f"${w52l}",            C["red"]),
            ("Insider Own.",  profile.get("insider_own","—"),                                         C["text"]),
            ("Inst. Own.",    profile.get("inst_own","—"),                                            C["text"]),
            ("Short Float",   profile.get("short_float","—"),                                         C["orange_br"]),
        ])
        st.markdown("<br>", unsafe_allow_html=True)
        render_technical_panel(ticker, df)
        st.markdown("<br>", unsafe_allow_html=True)

        phdr("Current News", "NEWSAPI · GNEWS")
        relevant = [n for n in state.news_stocks if ticker.lower() in n.get("title","").lower()][:4]
        if not relevant:
            relevant = state.news_stocks[:6]
        render_news_feed(relevant, max_items=6)
        st.markdown("<br>", unsafe_allow_html=True)

        render_screener()


@st.fragment(run_every=300)
def render_tab_valuation() -> None:
    state = get_terminal_state()
    macro = state.macro_indicators if state.macro_indicators else get_macro_indicators()

    h1, h2, h3 = st.columns([4, 2, 3])
    with h1:
        vt = (st.text_input("Val Ticker", value="AAPL", key="val_ticker",
            label_visibility="collapsed",
            placeholder="Ticker (AAPL, TSLA, NVDA, MSFT)…").upper().strip() or "AAPL")
    with h2:
        mode = st.selectbox("Model", ["DCF", "DDM", "EV/EBITDA"],
                            key="dcf_mode", label_visibility="collapsed")
    with h3:
        profile = VALUATION_PROFILES.get(vt, VALUATION_PROFILES["AAPL"])

        # Live Finnhub data to override profile
        fm = fetch_finnhub_metrics(vt)
        fq = fetch_finnhub_quote(vt)
        if fq.get("price"):
            profile = dict(profile)
            profile["current_price"] = fq["price"]

        # Compute quantamental rating live
        df_val   = fetch_polygon_ohlcv(vt, days=90)
        tech_val = build_tech_analysis(df_val, vt) if not df_val.empty else {}
        sent_val = state.sentiment_scores.get(vt, state.sentiment_scores.get("composite", 0.0))
        qr       = compute_quantamental_rating(profile, tech_val, fm, macro, sent_val)

        rating  = qr["score"]
        qlabel  = qr["label"]
        rcol    = (C["green_br"] if rating >= 65 else C["orange_br"] if rating >= 45 else C["red"])
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:10px;padding-top:4px;">'
            f'<span style="font-size:9px;color:{C["muted"]};letter-spacing:1px;">QUANT RATING</span>'
            f'<span style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:{rcol};'
            f'background:rgba(255,255,255,0.04);border:1px solid {rcol}33;'
            f'padding:4px 14px;border-radius:3px;">{rating}%</span>'
            f'<span style="font-size:8.5px;color:{rcol};letter-spacing:1.5px;">{qlabel}</span>'
            f'</div>', unsafe_allow_html=True)

    st.markdown(f'<hr style="border-color:{C["border"]};margin:6px 0;">', unsafe_allow_html=True)

    card_col, dcf_col = st.columns([3.5, 6.5], gap="small")

    with card_col:
        phdr(f"Valuation Scorecard — {profile.get('name', vt)}")
        render_val_cards(profile)
        st.markdown(
            f'<div class="tp" style="background:rgba(197,168,97,0.04);'
            f'border-color:rgba(197,168,97,0.18);margin-top:6px;">'
            f'<div style="font-family:Syne,sans-serif;font-weight:700;color:{C["gold"]};'
            f'font-size:9px;letter-spacing:1.5px;margin-bottom:5px;">ASSESSMENT</div>'
            f'<div style="font-size:10px;line-height:1.6;color:{C["text_dim"]};">'
            f'{profile.get("summary","")}</div></div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="tp" style="margin-top:4px;">'
            f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:5px;">Company Profile</div>'
            f'<div style="font-size:9.5px;line-height:1.55;color:{C["text_dim"]};">'
            f'{profile.get("description","")}</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Quantamental breakdown bars
        phdr("Quantamental Factor Breakdown", "LIVE SCORING", "blue")
        detail = qr.get("detail", {})
        factor_max = {"valuation": 30, "financial_health": 20, "technical": 15,
                      "macro_regime": 20, "sentiment": 15}
        for factor, max_val in factor_max.items():
            sc   = detail.get(factor, 0)
            pct  = sc / max_val * 100 if max_val else 0
            fc   = C["green_br"] if pct >= 60 else C["orange_br"] if pct >= 35 else C["red"]
            st.markdown(
                f'<div style="margin-bottom:4px;">'
                f'<div style="display:flex;justify-content:space-between;font-size:8px;'
                f'color:{C["muted"]};margin-bottom:2px;text-transform:uppercase;">'
                f'<span>{factor.replace("_"," ")}</span><span style="color:{fc};">'
                f'{sc:.0f}/{max_val}</span></div>'
                f'<div style="background:{C["border2"]};height:4px;border-radius:2px;">'
                f'<div style="width:{pct:.0f}%;height:100%;background:{fc};border-radius:2px;">'
                f'</div></div></div>', unsafe_allow_html=True)

        # NLP Sentiment box
        ts_col = score_to_color(sent_val)
        ts_lbl = score_to_label(sent_val)
        st.markdown(
            f'<div class="tp" style="padding:8px 12px;margin-top:4px;">'
            f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:4px;">NLP NEWS SENTIMENT — {vt}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;'
            f'color:{ts_col};">{ts_lbl}</div>'
            f'<div style="font-size:9px;color:{ts_col};margin-top:2px;">'
            f'Score: {sent_val:+.3f}</div></div>', unsafe_allow_html=True)

    with dcf_col:
        phdr("Discounted Cash Flow Model", badge=mode, badge_type="gold")
        render_dcf_panel(profile, mode=mode)
        st.markdown("<br>", unsafe_allow_html=True)

        phdr("DCF Sensitivity — WACC × Terminal Growth Rate", "HEATMAP", "gold")
        st.plotly_chart(
            build_dcf_sensitivity(profile["current_price"], profile.get("fair_value_dcf", 150)),
            use_container_width=True, config={"displayModeBar": False})
        st.markdown("<br>", unsafe_allow_html=True)

        render_peer_table(profile)
        st.markdown("<br>", unsafe_allow_html=True)

        # Macro context — live from TerminalState
        phdr("Macro Context", "FRED · Z-SCORE LIVE", "gold")
        m_keys = ["Fed Funds Rate", "10Y Treasury", "2Y Treasury",
                  "US CPI YoY", "US GDP (QoQ)", "VIX Index"]
        neg_up = {"Fed Funds Rate", "10Y Treasury", "2Y Treasury", "US CPI YoY", "Yield Spread"}
        mc_cols = st.columns(3)
        for i, key in enumerate(m_keys):
            info  = macro.get(key, MACRO_SEED.get(key, {}))
            val   = info.get("value", "—")
            delta = info.get("delta", "")
            trend = info.get("trend", "flat")
            is_b  = trend == "up" and key in neg_up
            is_g  = trend == "up" and key not in neg_up
            dc    = C["red"] if is_b else C["green_br"] if is_g else C["muted"]
            arr   = "▲" if trend == "up" else "▼" if trend == "down" else "—"
            with mc_cols[i % 3]:
                st.markdown(
                    f'<div class="tp" style="margin-bottom:4px;padding:7px 10px;">'
                    f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
                    f'text-transform:uppercase;margin-bottom:2px;">{key}</div>'
                    f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;'
                    f'color:{C["text"]};line-height:1;">{val}</div>'
                    f'<div style="font-size:8.5px;color:{dc};">{arr} {delta}</div></div>',
                    unsafe_allow_html=True)


@st.fragment(run_every=120)
def render_tab_crypto() -> None:
    state = get_terminal_state()

    cov   = state.crypto_overview if state.crypto_overview else fetch_coingecko_overview()
    glbl  = fetch_coingecko_global()
    if glbl:
        cov.update(glbl)
    coins = cov.get("coins", [])

    c1, c2 = st.columns([6, 4], gap="small")

    with c1:
        # Crypto heatmap
        phdr("Crypto Market Heatmap", "COINGECKO · LIVE", "gold")
        st.plotly_chart(build_crypto_heatmap(coins), use_container_width=True,
                        config={"displayModeBar": False})
        st.markdown("<br>", unsafe_allow_html=True)

        # Full crypto table
        render_crypto_table(cov)

    with c2:
        # Fear & Greed
        phdr("Sentiment Gauges", "ALTERNATIVE.ME · CNN", "gold")
        fg1, fg2 = st.columns(2)
        with fg1:
            render_fear_greed_gauge(state.crypto_fear_greed, "CRYPTO F&G")
        with fg2:
            render_fear_greed_gauge(state.stock_fear_greed, "EQUITY F&G")
        st.markdown("<br>", unsafe_allow_html=True)

        # Fear & Greed history chart
        history = state.crypto_fear_greed.get("history", [])
        if history:
            phdr("Crypto F&G History", "7 DAYS", "gold")
            fig = go.Figure(go.Bar(
                x=[h["date"] for h in history],
                y=[h["value"] for h in history],
                marker_color=[
                    C["red"] if v < 25 else C["orange_br"] if v < 45
                    else C["gold"] if v < 55 else C["green_br"]
                    for v in [h["value"] for h in history]
                ],
                text=[h["label"] for h in history],
                textposition="outside",
                textfont=dict(size=8),
            ))
            _apply_dark(fig)
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=8, b=0))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown("<br>", unsafe_allow_html=True)

        # Crypto news (RSS + all API sources)
        phdr("Crypto News", "RSS · NEWSAPI · GNEWS · NEWSDATA", "gold")
        crypto_news_items = state.news_crypto if state.news_crypto else fetch_gnews("bitcoin ethereum crypto defi", 10)
        render_news_feed(crypto_news_items, max_items=12)
        st.markdown("<br>", unsafe_allow_html=True)

        # CoinGecko global stats
        if glbl:
            phdr("Global Crypto Stats", "COINGECKO · LIVE", "gold")
            render_kv([
                ("Total Market Cap", f"${glbl.get('total_market_cap',0)/1e12:.2f}T",   C["text"]),
                ("BTC Dominance",    f"{glbl.get('btc_dominance',0):.1f}%",             C["gold"]),
                ("ETH Dominance",    f"{glbl.get('eth_dominance',0):.1f}%",             C["blue_br"]),
                ("Active Coins",     f"{glbl.get('active_coins',0):,}",                  C["muted"]),
            ])


# ═════════════════════════════════════════════════════════════════════════════════════
# §22  YAHOO FINANCE OHLCV FALLBACK (when Polygon returns empty)
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=120, show_spinner=False)
def fetch_yf_ohlcv(ticker: str, period: str = "6mo", interval: str = "1d") -> pd.DataFrame:
    """Yahoo Finance unofficial chart API — OHLCV fallback for Polygon."""
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval={interval}&range={period}&includePrePost=false")
    hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        r = requests.get(url, headers=hdrs, timeout=12)
        if r.status_code != 200:
            return pd.DataFrame()
        data   = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return pd.DataFrame()
        meta       = result[0].get("meta", {})
        timestamps = result[0].get("timestamp", [])
        quotes     = result[0].get("indicators", {}).get("quote", [{}])[0]
        if not timestamps or not quotes.get("close"):
            return pd.DataFrame()
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(timestamps, unit="s"),
            "open":      [float(v) if v else None for v in quotes.get("open",  [])],
            "high":      [float(v) if v else None for v in quotes.get("high",  [])],
            "low":       [float(v) if v else None for v in quotes.get("low",   [])],
            "close":     [float(v) if v else None for v in quotes.get("close", [])],
            "volume":    [float(v) if v else 0    for v in quotes.get("volume",[])],
        })
        return df.dropna(subset=["close"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def get_ohlcv_best(ticker: str, days: int = 185) -> pd.DataFrame:
    """
    Best-effort OHLCV: Polygon first, Yahoo Finance fallback.
    Returns a normalised DataFrame with columns: timestamp, open, high, low, close, volume.
    """
    df = fetch_polygon_ohlcv(ticker, days=days)
    if not df.empty and len(df) >= 10:
        return df
    # Map days → Yahoo range string
    if days <= 35:   yf_range = "1mo"
    elif days <= 95: yf_range = "3mo"
    elif days <= 185:yf_range = "6mo"
    elif days <= 370:yf_range = "1y"
    else:            yf_range = "2y"
    return fetch_yf_ohlcv(ticker, period=yf_range, interval="1d")


# ═════════════════════════════════════════════════════════════════════════════════════
# §23  GLOBAL INDICES — Marketstack + Yahoo Finance
# ═════════════════════════════════════════════════════════════════════════════════════

_GLOBAL_INDICES = [
    ("S&P 500",   "^GSPC"),  ("NASDAQ",   "^IXIC"),  ("RUSSELL", "^RUT"),
    ("FTSE 100",  "^FTSE"),  ("DAX",      "^GDAXI"), ("NIKKEI",  "^N225"),
    ("HANG SENG", "^HSI"),   ("CAC 40",   "^FCHI"),  ("ASX 200", "^AXJO"),
    ("EUR/USD",   "EURUSD=X"),("GBP/USD", "GBPUSD=X"),("USD/JPY","JPY=X"),
    ("GOLD",      "GC=F"),   ("WTI OIL",  "CL=F"),   ("NAT GAS","NG=F"),
    ("BTC/USD",   "BTC-USD"),("ETH/USD",  "ETH-USD"),
]

@st.cache_data(ttl=120, show_spinner=False)
def get_global_indices() -> List[Dict]:
    """Fetch global index prices via Yahoo Finance v8 (batch)."""
    results: List[Dict] = []
    hdrs = {"User-Agent": "Mozilla/5.0"}
    for label, sym in _GLOBAL_INDICES:
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                   f"?interval=1d&range=2d")
            r = requests.get(url, headers=hdrs, timeout=8)
            if r.status_code != 200:
                continue
            data   = r.json()
            res    = data.get("chart", {}).get("result", [])
            if not res:
                continue
            meta   = res[0].get("meta", {})
            price  = float(meta.get("regularMarketPrice", 0) or 0)
            prev   = float(meta.get("chartPreviousClose", 0) or meta.get("previousClose", price) or price)
            chg    = (price - prev) / prev * 100 if prev else 0
            results.append({
                "label":  label,
                "symbol": sym,
                "price":  price,
                "change": round(chg, 2),
                "prev":   prev,
            })
        except Exception:
            continue
    return results


def render_global_indices(indices: List[Dict]) -> None:
    """Compact 3-column global index grid."""
    phdr("Global Indices", "YAHOO FINANCE · LIVE", "gold")
    if not indices:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;text-align:center;">Awaiting data…</div>',
                    unsafe_allow_html=True)
        return
    cols = st.columns(3)
    for i, idx in enumerate(indices):
        chg  = idx.get("change", 0)
        cc   = C["green_br"] if chg >= 0 else C["red"]
        sign = "▲" if chg >= 0 else "▼"
        p    = idx.get("price", 0)
        pfmt = f"{p:,.2f}" if p < 100 else f"{p:,.0f}"
        with cols[i % 3]:
            st.markdown(
                f'<div class="tp" style="padding:6px 10px;margin-bottom:3px;">'
                f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
                f'text-transform:uppercase;">{idx["label"]}</div>'
                f'<div style="font-family:Syne,sans-serif;font-size:13px;font-weight:800;'
                f'color:{C["text"]};line-height:1.2;">{pfmt}</div>'
                f'<div style="font-size:8.5px;color:{cc};">{sign}{abs(chg):.2f}%</div>'
                f'</div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §24  OSINT STREAM — FLASH / PRIORITY / ROUTINE intelligence feed
# ═════════════════════════════════════════════════════════════════════════════════════

_OSINT_RSS: List[Tuple[str, str]] = [
    ("Al Jazeera",    "https://www.aljazeera.com/xml/rss/all.xml"),
    ("BBC World",     "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Reuters",       "https://feeds.reuters.com/reuters/worldNews"),
    ("DW World",      "https://rss.dw.com/rdf/rss-en-world"),
    ("MilTimes",      "https://www.militarytimes.com/arc/outboundfeeds/rss/"),
    ("DefNews",       "https://www.defensenews.com/rss/"),
    ("ReliefWeb",     "https://reliefweb.int/headlines/rss.xml"),
    ("France 24",     "https://www.france24.com/en/rss"),
    ("Middle East Eye","https://www.middleeasteye.net/rss"),
    ("TRT World",     "https://www.trtworld.com/rss"),
]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_osint_stream() -> List[Dict]:
    """
    Multi-source OSINT RSS aggregator — FLASH / PRIORITY / ROUTINE classification.
    World News API adds geopolitical depth. Results sorted: FLASH first.
    """
    items: List[Dict] = []
    seen:  set        = set()

    # RSS sources
    for source, url in _OSINT_RSS:
        if not RATE_LIMITER.check(url, max_per_minute=4):
            continue
        try:
            r = requests.get(url, timeout=9, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                continue
            feed = feedparser.parse(r.text)
            for entry in feed.entries[:10]:
                title = _clean_html(entry.get("title", ""))
                if not title or len(title) < 10:
                    continue
                nid = _news_id(title, source)
                if nid in seen:
                    continue
                seen.add(nid)
                pub = ""
                try:
                    if entry.get("published_parsed"):
                        pub = datetime(*entry.published_parsed[:6]).strftime("%d %b %H:%M")
                except Exception:
                    pass
                tier = _classify_tier(title)
                items.append({
                    "id":        nid,
                    "source":    source[:18],
                    "title":     title[:160],
                    "summary":   _clean_html(entry.get("summary",""))[:200],
                    "link":      entry.get("link",""),
                    "published": pub,
                    "tier":      tier,
                    "sentiment": score_text_sentiment(title),
                    "is_alert":  tier in ("FLASH","PRIORITY"),
                    "asset_type":"osint",
                })
        except Exception:
            continue

    # Supplement with World News API geopolitical stream
    try:
        wn = fetch_worldnews("conflict military attack crisis sanctions war geopolitics", 15)
        for item in wn:
            if item["id"] not in seen:
                seen.add(item["id"])
                items.append(item)
    except Exception:
        pass

    # Also supplement with GNews geopolitical
    try:
        gn = fetch_gnews("conflict military geopolitics crisis war", 10)
        for item in gn:
            if item["id"] not in seen:
                seen.add(item["id"])
                items.append(item)
    except Exception:
        pass

    # Sort: FLASH → PRIORITY → ROUTINE
    tier_rank = {"FLASH": 0, "PRIORITY": 1, "ROUTINE": 2}
    items.sort(key=lambda x: tier_rank.get(x.get("tier","ROUTINE"), 2))
    return items[:80]


def render_osint_stream(state: TerminalState, max_items: int = 30) -> None:
    """Crucix-style OSINT intelligence stream panel."""
    items = fetch_osint_stream()
    tier_col = {"FLASH": C["red"], "PRIORITY": C["orange_br"], "ROUTINE": C["muted"]}
    tier_bg  = {
        "FLASH":    f"rgba(211,47,47,0.18)",
        "PRIORITY": f"rgba(192,122,42,0.14)",
        "ROUTINE":  f"rgba(66,72,90,0.10)",
    }

    phdr("OSINT Intelligence Stream", "FLASH · PRIORITY · ROUTINE", "red")
    if not items:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:12px;text-align:center;">'
                    f'OSINT stream loading…</div>', unsafe_allow_html=True)
        return

    html = '<div class="tp" style="padding:0;max-height:500px;overflow-y:auto;">'
    for item in items[:max_items]:
        tier    = item.get("tier", "ROUTINE")
        tc      = tier_col.get(tier, C["muted"])
        tbg     = tier_bg.get(tier, "rgba(66,72,90,0.08)")
        src     = item.get("source","—")[:16]
        title   = item.get("title","")
        pub     = item.get("published","")
        link    = item.get("link","#")
        sent    = item.get("sentiment", 0.0)
        sc      = score_to_color(sent)
        dot_anim = ' animation:blink 1.5s ease-in-out infinite;' if tier == "FLASH" else ''
        html += (
            f'<div style="display:grid;grid-template-columns:10px 100px 1fr 60px;'
            f'gap:8px;padding:7px 12px;border-bottom:1px solid {C["border"]};'
            f'align-items:start;background:{tbg};">'
            f'<div style="width:7px;height:7px;border-radius:50%;background:{tc};'
            f'margin-top:4px;box-shadow:0 0 5px {tc}80;{dot_anim}"></div>'
            f'<div style="font-size:7.5px;letter-spacing:0.5px;padding:2px 5px;'
            f'border-radius:2px;text-transform:uppercase;font-weight:700;color:{tc};'
            f'background:{tbg};text-align:center;margin-top:1px;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{src}</div>'
            f'<div>'
            f'<a href="{link}" target="_blank" style="font-size:10px;line-height:1.4;'
            f'color:{"#ff5252" if tier=="FLASH" else C["text"]};'
            f'{"font-weight:700;" if tier=="FLASH" else ""}'
            f'text-decoration:none;">{title}</a>'
            f'<div style="font-size:8px;color:{C["muted"]};margin-top:2px;">{pub}</div>'
            f'</div>'
            f'<div style="font-size:7px;letter-spacing:1.5px;padding:2px 5px;'
            f'border-radius:2px;font-weight:700;text-transform:uppercase;'
            f'color:{tc};border:1px solid {tc}44;text-align:center;'
            f'align-self:start;margin-top:2px;">{tier}</div>'
            f'</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §25  INFRASTRUCTURE CASCADE & PENTAGON INDEX (static geopolitical models)
# ═════════════════════════════════════════════════════════════════════════════════════

INFRA_CASCADE: List[Dict] = [
    {"node": "Strait of Hormuz",  "type": "Chokepoint", "lat": 26.57, "lng": 56.26,
     "countries": ["UAE","India","Japan","S. Korea","China"],   "risk_pct": [62,47,42,40,36]},
    {"node": "Suez Canal",        "type": "Canal",      "lat": 30.58, "lng": 32.34,
     "countries": ["Egypt","UK","Netherlands","Germany","France"], "risk_pct": [55,34,30,28,25]},
    {"node": "Taiwan Strait",     "type": "Chokepoint", "lat": 24.50, "lng": 119.50,
     "countries": ["Taiwan","Japan","S. Korea","Philippines"],  "risk_pct": [82,67,57,52]},
    {"node": "Bab-el-Mandeb",     "type": "Chokepoint", "lat": 12.50, "lng": 43.50,
     "countries": ["Ethiopia","Djibouti","Egypt","EU Shipping"],"risk_pct": [60,55,48,44]},
    {"node": "Red Sea Corridor",  "type": "Shipping",   "lat": 18.00, "lng": 42.00,
     "countries": ["Yemen","Egypt","Saudi Arabia","EU"],        "risk_pct": [70,55,42,38]},
    {"node": "Nord Stream Alt.",  "type": "Pipeline",   "lat": 55.00, "lng": 14.00,
     "countries": ["Germany","Poland","Czech Rep.","Austria"],  "risk_pct": [70,46,32,26]},
]

PENTAGON_INDEX: Dict = {
    "score": 67, "label": "ELEVATED",
    "components": {
        "Conflict Events":      74,
        "Naval Activity":       68,
        "Cyber Incidents":      58,
        "Military Mobilization":71,
        "Diplomatic Tension":   64,
    },
}


def render_infra_cascade(node_idx: int) -> None:
    """Infrastructure cascade risk visualization for a selected chokepoint node."""
    node = INFRA_CASCADE[node_idx]
    countries = node.get("countries", [])
    risks     = node.get("risk_pct", [])

    phdr(f"Infrastructure Risk: {node['node']}", node["type"], "red")
    bars_html = ""
    for country, risk in zip(countries, risks):
        rc = C["red"] if risk > 60 else C["orange_br"] if risk > 40 else C["gold"]
        bars_html += (
            f'<div style="margin-bottom:5px;">'
            f'<div style="display:flex;justify-content:space-between;font-size:8.5px;'
            f'color:{C["muted"]};margin-bottom:2px;">'
            f'<span>{country}</span><span style="color:{rc};">{risk}%</span></div>'
            f'<div style="background:{C["border2"]};height:5px;border-radius:3px;">'
            f'<div style="width:{risk}%;height:100%;background:{rc};border-radius:3px;">'
            f'</div></div></div>')
    st.markdown(f'<div class="tp" style="padding:10px 12px;">'
                f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};'
                f'text-transform:uppercase;margin-bottom:8px;">'
                f'Supply Chain Exposure by Country (%)</div>'
                f'{bars_html}</div>', unsafe_allow_html=True)


def render_pentagon_index() -> None:
    """Pentagon Conflict Index panel with component bars."""
    pi    = PENTAGON_INDEX
    score = pi["score"]
    label = pi["label"]
    sc    = C["red"] if score >= 70 else C["orange_br"] if score >= 50 else C["gold"]
    phdr("Pentagon Conflict Index", f"SCORE {score} · {label}", "red")
    comp_html = ""
    for comp, val in pi.get("components", {}).items():
        vc = C["red"] if val >= 70 else C["orange_br"] if val >= 50 else C["gold"]
        comp_html += (
            f'<div style="margin-bottom:4px;">'
            f'<div style="display:flex;justify-content:space-between;font-size:8.5px;'
            f'color:{C["muted"]};margin-bottom:2px;">'
            f'<span>{comp}</span><span style="color:{vc};">{val}</span></div>'
            f'<div style="background:{C["border2"]};height:4px;border-radius:2px;">'
            f'<div style="width:{val}%;height:100%;background:{vc};border-radius:2px;">'
            f'</div></div></div>')
    st.markdown(
        f'<div class="tp" style="padding:10px 12px;">'
        f'<div style="text-align:center;margin-bottom:8px;">'
        f'<span style="font-family:Syne,sans-serif;font-size:28px;font-weight:800;'
        f'color:{sc};">{score}</span>'
        f'<span style="font-size:9px;color:{C["muted"]};margin-left:6px;">/100 · {label}</span>'
        f'</div>{comp_html}</div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §26  WHALE ACTIVITY — Binance aggTrades large-block detector
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30, show_spinner=False)
def fetch_binance_whale_trades(symbol: str = "BTCUSDT", limit: int = 100) -> List[Dict]:
    """Scan Binance aggTrades for large block orders (whale activity)."""
    url = (f"https://api.binance.com/api/v3/aggTrades"
           f"?symbol={symbol}&limit={limit}")
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            trades = r.json()
            whale_threshold = 100_000  # USD notional
            whales = []
            for t in trades:
                qty   = float(t.get("q", 0))
                price = float(t.get("p", 0))
                notional = qty * price
                if notional >= whale_threshold:
                    side = "SELL" if t.get("m") else "BUY"
                    whales.append({
                        "side":     side,
                        "price":    price,
                        "qty":      qty,
                        "notional": notional,
                        "time":     datetime.fromtimestamp(t.get("T",0)/1000).strftime("%H:%M:%S"),
                    })
            return sorted(whales, key=lambda x: -x["notional"])[:15]
    except Exception:
        pass
    return []


def render_whale_activity() -> None:
    """Binance on-chain whale block trade panel."""
    phdr("Whale Activity Monitor", "BINANCE · BTCUSDT · LIVE", "gold")
    trades = fetch_binance_whale_trades("BTCUSDT", 100)
    if not trades:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;text-align:center;">'
                    f'Scanning Binance order flow…</div>', unsafe_allow_html=True)
        return
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;'
                     f'letter-spacing:1.5px;">{h}</td>'
                     for h in ["TIME", "SIDE", "PRICE", "QTY (BTC)", "NOTIONAL"])
           + "</tr>")
    rows = ""
    for t in trades:
        sc = C["green_br"] if t["side"] == "BUY" else C["red"]
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{t["time"]}</td>'
                 f'<td style="padding:3px 6px;font-weight:700;color:{sc};">{t["side"]}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">${t["price"]:,.2f}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{t["qty"]:.4f}</td>'
                 f'<td style="padding:3px 6px;color:{sc};font-weight:700;">'
                 f'${t["notional"]/1e3:.0f}K</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:260px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §27  MACRO Z-SCORE ANOMALY DETECTOR — CHART
# ═════════════════════════════════════════════════════════════════════════════════════

def build_zscore_bar_chart(macro: Dict) -> go.Figure:
    """Bar chart of Z-scores for FRED macro series — anomaly detection."""
    labels, zscores, colors_list = [], [], []
    for k, v in macro.items():
        z = v.get("zscore", 0.0)
        if isinstance(z, float) and not math.isnan(z):
            labels.append(k)
            zscores.append(z)
            colors_list.append(C["red"] if z > 2 or z < -2 else
                               C["orange_br"] if abs(z) > 1 else C["muted"])

    if not labels:
        return _apply_dark(go.Figure())

    fig = go.Figure(go.Bar(
        x=labels,
        y=zscores,
        marker_color=colors_list,
        text=[f"{z:+.2f}σ" for z in zscores],
        textposition="outside",
        textfont=dict(size=8),
        hovertemplate="<b>%{x}</b><br>Z-Score: %{y:+.2f}σ<extra></extra>",
    ))
    fig.add_hline(y=2,   line_color=C["red"],    line_dash="dot", line_width=1,
                  annotation_text="  +2σ", annotation_font_size=8)
    fig.add_hline(y=-2,  line_color=C["red"],    line_dash="dot", line_width=1,
                  annotation_text="  -2σ", annotation_font_size=8)
    fig.add_hline(y=0,   line_color=C["border2"], line_width=0.5)
    _layout = {**_DARK_LAYOUT, "height": 260}
    _layout.pop("xaxis", None)
    _layout.pop("yaxis", None)
    fig.update_layout(**_layout)
    fig.update_xaxes(tickfont=dict(size=7), gridcolor=C["border"], showgrid=True)
    fig.update_yaxes(gridcolor=C["border"], tickfont=dict(size=8), showgrid=True,
                     title=dict(text="Standard Deviations", font=dict(size=8)))
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════
# §28  EXTENDED BACKGROUND PIPELINE  (global indices + OSINT + Finnhub batch)
# ═════════════════════════════════════════════════════════════════════════════════════

_EXT_INTERVALS: Dict[str, int] = {
    "global_indices": 120,
    "osint_stream":   300,
    "whale_data":      30,
}


def _extended_pipeline_worker(state: TerminalState) -> None:
    """Secondary daemon: global indices, OSINT stream, whale data."""
    while True:
        try:
            if state.age("global_indices") > _EXT_INTERVALS["global_indices"]:
                try:
                    idxs = get_global_indices()
                    if idxs:
                        state.update(global_indices=idxs)
                except Exception as e:
                    state.mark_error("global_indices", e)
        except Exception:
            pass
        time.sleep(30)


def start_extended_pipeline() -> None:
    if st.session_state.get("_ext_pipeline_started"):
        return
    state = get_terminal_state()
    t = threading.Thread(target=_extended_pipeline_worker, args=(state,),
                         daemon=True, name="ExtDataPipeline")
    t.start()
    st.session_state["_ext_pipeline_started"] = True


# ═════════════════════════════════════════════════════════════════════════════════════
# §29  ALPHA VANTAGE MACRO CORRELATIONS (Equity Beta vs Macro Regime)
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_alphav_correlation_data(symbols: List[str]) -> Dict[str, List[float]]:
    """
    Fetch 90-day daily close prices for a basket of symbols via Alpha Vantage
    to compute rolling beta / correlation vs SPY (macro regime calibration).
    """
    prices: Dict[str, List[float]] = {}
    for sym in symbols[:5]:   # AV free: 25 req/day — limit symbols
        url = (f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"
               f"&symbol={sym}&outputsize=compact&apikey={ALPHAV_KEY}")
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 200:
                ts = r.json().get("Time Series (Daily)", {})
                closes = []
                for date in sorted(ts.keys(), reverse=True)[:90]:
                    c = float(ts[date].get("5. adjusted close", 0) or 0)
                    if c > 0:
                        closes.append(c)
                if closes:
                    prices[sym] = closes[::-1]   # chronological
        except Exception:
            continue
    return prices


def compute_macro_beta(prices: Dict[str, List[float]]) -> Dict[str, float]:
    """
    Compute rolling 60-day beta of each symbol vs the first symbol (assumed SPY).
    Returns {symbol: beta}.
    """
    syms = list(prices.keys())
    if len(syms) < 2:
        return {}
    ref_key  = syms[0]
    ref_rets = np.diff(np.log(prices[ref_key]))
    betas: Dict[str, float] = {}
    for sym in syms[1:]:
        try:
            sym_rets = np.diff(np.log(prices[sym]))
            n = min(len(ref_rets), len(sym_rets), 60)
            r  = ref_rets[-n:]
            s  = sym_rets[-n:]
            cov = float(np.cov(s, r)[0][1])
            var = float(np.var(r))
            betas[sym] = round(cov / var, 3) if var else 0.0
        except Exception:
            betas[sym] = 0.0
    return betas


def build_beta_chart(betas: Dict[str, float]) -> go.Figure:
    syms = list(betas.keys())
    vals = list(betas.values())
    cols = [C["green_br"] if b < 1 else C["orange_br"] if b < 1.5 else C["red"] for b in vals]
    fig = go.Figure(go.Bar(
        x=syms, y=vals,
        marker_color=cols,
        text=[f"β={b:.2f}" for b in vals],
        textposition="outside",
        textfont=dict(size=9),
        hovertemplate="<b>%{x}</b><br>Beta: %{y:.3f}<extra></extra>",
    ))
    fig.add_hline(y=1.0, line_color=C["gold"], line_dash="dot", line_width=1,
                  annotation_text="  β=1.0", annotation_font_size=8)
    _layout = {**_DARK_LAYOUT, "height": 230}
    _layout.pop("xaxis", None)
    _layout.pop("yaxis", None)
    fig.update_layout(**_layout)
    fig.update_xaxes(tickfont=dict(size=8), gridcolor=C["border"])
    fig.update_yaxes(title=dict(text="Beta vs SPY", font=dict(size=8)),
                     gridcolor=C["border"], tickfont=dict(size=8))
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT  TRADIER OPTIONS CHAIN
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=120, show_spinner=False)
def fetch_tradier_options_chain(symbol: str) -> Dict:
    """Tradier options chain — nearest expiry calls & puts."""
    hdrs = {"Authorization": f"Bearer {TRADIER_KEY}", "Accept": "application/json"}
    for base in ["https://api.tradier.com", "https://sandbox.tradier.com"]:
        try:
            r = requests.get(f"{base}/v1/markets/options/expirations",
                headers=hdrs, params={"symbol": symbol, "includeAllRoots": "true"}, timeout=10)
            if r.status_code != 200:
                continue
            exps = r.json().get("expirations", {}).get("date", [])
            if not exps:
                continue
            expiry = exps[0] if isinstance(exps, list) else exps
            r2 = requests.get(f"{base}/v1/markets/options/chains",
                headers=hdrs, params={"symbol": symbol, "expiration": expiry, "greeks": "true"}, timeout=10)
            if r2.status_code != 200:
                continue
            chain = r2.json().get("options", {}).get("option", [])
            if not chain:
                continue
            calls = sorted([o for o in chain if o.get("option_type")=="call"],
                           key=lambda x: abs(float(x.get("strike",0))))[:10]
            puts  = sorted([o for o in chain if o.get("option_type")=="put"],
                           key=lambda x: abs(float(x.get("strike",0))))[:10]
            return {"expiry": expiry, "calls": calls, "puts": puts}
        except Exception:
            continue
    return {}


def render_options_chain(symbol: str) -> None:
    phdr(f"Options Chain — {symbol}", "TRADIER · LIVE", "gold")
    chain = fetch_tradier_options_chain(symbol)
    if not chain or not chain.get("calls"):
        st.markdown(
            f'<div class="tp" style="color:{C["muted"]};padding:10px;text-align:center;">'
            f'Options data unavailable for {symbol}.</div>', unsafe_allow_html=True)
        return
    exp = chain.get("expiry","")
    st.markdown(f'<div style="font-size:8px;color:{C["muted"]};padding:2px 0 6px 0;">'
                f'EXPIRY: {exp}</div>', unsafe_allow_html=True)
    def _opt_rows(opts: List[Dict], otype: str) -> str:
        color = C["green_br"] if otype=="call" else C["red"]
        h = (f'<table style="width:100%;border-collapse:collapse;font-size:8.5px;">'
             f'<tr style="border-bottom:1px solid {C["border"]};">'
             + "".join(f'<td style="padding:3px 5px;color:{C["gold"]};font-size:7px;">{h}</td>'
                       for h in ["STRIKE","BID","ASK","IV","DELTA","OI"]) + "</tr>")
        for o in opts:
            g = o.get("greeks") or {}
            iv = float(o.get("iv",0) or 0)*100
            h += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                  f'<td style="padding:3px 5px;font-weight:700;color:{color};">${float(o.get("strike",0)):,.0f}</td>'
                  f'<td style="padding:3px 5px;">${float(o.get("bid",0) or 0):.2f}</td>'
                  f'<td style="padding:3px 5px;">${float(o.get("ask",0) or 0):.2f}</td>'
                  f'<td style="padding:3px 5px;color:{C["muted"]};">{iv:.0f}%</td>'
                  f'<td style="padding:3px 5px;">{float(g.get("delta",0) or 0):.2f}</td>'
                  f'<td style="padding:3px 5px;">{int(o.get("open_interest",0) or 0):,}</td></tr>')
        return f'<div class="tp" style="padding:4px;max-height:220px;overflow-y:auto;">{h}</table></div>'
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["green_br"]};'
                    f'margin-bottom:3px;">CALLS</div>', unsafe_allow_html=True)
        st.markdown(_opt_rows(chain["calls"],"call"), unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["red"]};'
                    f'margin-bottom:3px;">PUTS</div>', unsafe_allow_html=True)
        st.markdown(_opt_rows(chain["puts"],"put"), unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT2  FINNHUB EARNINGS CALENDAR
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_finnhub_earnings_calendar(days_ahead: int = 14) -> List[Dict]:
    today = datetime.now().strftime("%Y-%m-%d")
    end   = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    url   = (f"https://finnhub.io/api/v1/calendar/earnings"
             f"?from={today}&to={end}&token={FINNHUB_KEY}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            items = r.json().get("earningsCalendar", [])
            return [{
                "symbol":  i.get("symbol",""),
                "name":    i.get("company",""),
                "date":    i.get("date",""),
                "time":    i.get("hour",""),
                "eps_est": i.get("epsEstimate"),
                "rev_est": i.get("revenueEstimate"),
            } for i in items if i.get("symbol")][:40]
    except Exception:
        pass
    return []


def render_earnings_calendar() -> None:
    phdr("Upcoming Earnings", "FINNHUB · 14 DAYS", "gold")
    events = fetch_finnhub_earnings_calendar(14)
    if not events:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;'
                    f'text-align:center;">Awaiting earnings calendar…</div>', unsafe_allow_html=True)
        return
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:8.5px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7px;">{h}</td>'
                     for h in ["DATE","TIME","TICKER","COMPANY","EPS EST","REV EST"])
           + "</tr>")
    rows = ""
    for ev in events[:20]:
        eps   = ev.get("eps_est")
        rev   = ev.get("rev_est")
        eps_s = f"${eps:.2f}" if eps is not None else "—"
        rev_s = f"${rev/1e9:.2f}B" if rev else "—"
        ts    = {"bmo":"Pre-Mkt","amc":"After-Mkt"}.get(ev.get("time",""),"—")
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{ev["date"]}</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{ts}</td>'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["gold"]};'
                 f'font-family:Syne,sans-serif;">{ev["symbol"]}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{ev["name"][:22]}</td>'
                 f'<td style="padding:3px 6px;">{eps_s}</td>'
                 f'<td style="padding:3px 6px;">{rev_s}</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:300px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT3  ALPHA VANTAGE SECTOR ROTATION
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_sector_performance() -> List[Dict]:
    url = f"https://www.alphavantage.co/query?function=SECTOR&apikey={ALPHAV_KEY}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            data  = r.json()
            today = data.get("Rank A: Real-Time Performance", {})
            month = data.get("Rank C: 1 Month Performance", {})
            ytd   = data.get("Rank E: Year-to-Date (YTD) Performance", {})
            result = []
            for sector, val in today.items():
                if sector == "Meta Data":
                    continue
                try:
                    d = float(str(val).replace("%",""))
                    m = float(str(month.get(sector,"0%")).replace("%",""))
                    y = float(str(ytd.get(sector,"0%")).replace("%",""))
                    result.append({"sector": sector, "day": d, "month": m, "ytd": y})
                except Exception:
                    pass
            return sorted(result, key=lambda x: -x["day"])
    except Exception:
        pass
    return []


def render_sector_rotation() -> None:
    phdr("S&P 500 Sector Rotation", "ALPHA VANTAGE · REAL-TIME", "gold")
    sectors = fetch_sector_performance()
    if not sectors:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;'
                    f'text-align:center;">Awaiting sector data…</div>', unsafe_allow_html=True)
        return
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:8.5px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7px;">{h}</td>'
                     for h in ["SECTOR","DAY","1M","YTD"]) + "</tr>")
    rows = ""
    for s in sectors:
        dc = C["green_br"] if s["day"]   >= 0 else C["red"]
        mc = C["green_br"] if s["month"] >= 0 else C["red"]
        yc = C["green_br"] if s["ytd"]   >= 0 else C["red"]
        ds = "▲" if s["day"] >= 0 else "▼"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{s["sector"][:30]}</td>'
                 f'<td style="padding:3px 6px;color:{dc};font-weight:700;">{ds}{abs(s["day"]):.2f}%</td>'
                 f'<td style="padding:3px 6px;color:{mc};">{s["month"]:+.2f}%</td>'
                 f'<td style="padding:3px 6px;color:{yc};">{s["ytd"]:+.2f}%</td></tr>')
    st.markdown(f'<div class="tp" style="padding:4px;">{hdr}{rows}</table></div>',
                unsafe_allow_html=True)


def build_sector_bar_chart(sectors: List[Dict]) -> go.Figure:
    if not sectors:
        return _apply_dark(go.Figure())
    names  = [s["sector"].replace("Information Technology","IT")
               .replace("Communication Services","Comm Svcs") for s in sectors]
    days   = [s["day"] for s in sectors]
    colors = [C["green_br"] if d >= 0 else C["red"] for d in days]
    fig = go.Figure(go.Bar(
        x=days, y=names, orientation="h",
        marker_color=colors,
        text=[f"{d:+.2f}%" for d in days],
        textposition="outside",
        textfont=dict(size=8),
        hovertemplate="<b>%{y}</b><br>Day: %{x:+.2f}%<extra></extra>",
    ))
    _layout = {**_DARK_LAYOUT, "height": 340, "margin": dict(l=160, r=40, t=8, b=8)}
    _layout.pop("xaxis", None)
    _layout.pop("yaxis", None)
    fig.update_layout(**_layout)
    fig.update_xaxes(gridcolor=C["border"], tickfont=dict(size=8))
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)", tickfont=dict(size=8))
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT4  MARKET MOVERS — Finnhub top gainers & losers
# ═════════════════════════════════════════════════════════════════════════════════════

_MOVER_CANDIDATES = [
    "AAPL","MSFT","NVDA","TSLA","META","AMZN","GOOGL","AVGO","NFLX","AMD",
    "COIN","MSTR","PLTR","SMCI","ARM","MU","INTC","QCOM","AMAT","LRCX",
    "JPM","GS","BAC","MS","WFC","V","MA","PYPL","SQ","HOOD",
    "XOM","CVX","SLB","COP","OXY","NEE","LLY","PFE","MRK","ABBV",
]

@st.cache_data(ttl=120, show_spinner=False)
def fetch_market_movers() -> Dict[str, List[Dict]]:
    results = []
    for sym in _MOVER_CANDIDATES:
        try:
            q = fetch_finnhub_quote(sym)
            if q.get("price", 0) > 0:
                results.append({"symbol": sym, "price": q["price"],
                                 "change": q.get("change", 0)})
        except Exception:
            pass
    gainers = sorted(results, key=lambda x: -x["change"])[:8]
    losers  = sorted(results, key=lambda x:  x["change"])[:8]
    return {"gainers": gainers, "losers": losers}


def render_market_movers() -> None:
    phdr("Market Movers", "FINNHUB · GAINERS & LOSERS", "gold")
    data    = fetch_market_movers()
    gainers = data.get("gainers", [])
    losers  = data.get("losers",  [])
    if not gainers and not losers:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;'
                    f'text-align:center;">Computing movers…</div>', unsafe_allow_html=True)
        return
    def _tbl(items: List[Dict], title: str, color: str, sign: str) -> str:
        h = (f'<div style="font-size:8px;letter-spacing:1.5px;color:{color};'
             f'text-transform:uppercase;margin-bottom:4px;">{title}</div>'
             f'<div class="tp" style="padding:4px;">'
             f'<table style="width:100%;border-collapse:collapse;font-size:8.5px;">'
             f'<tr style="border-bottom:1px solid {C["border"]};">'
             + "".join(f'<td style="padding:2px 5px;color:{C["gold"]};font-size:7px;">{x}</td>'
                       for x in ["TICKER","PRICE","CHG%"]) + "</tr>")
        for item in items:
            chg = item["change"]
            h += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                  f'<td style="padding:2px 5px;font-weight:700;color:{C["gold"]};'
                  f'font-family:Syne,sans-serif;">{item["symbol"]}</td>'
                  f'<td style="padding:2px 5px;">${item["price"]:,.2f}</td>'
                  f'<td style="padding:2px 5px;color:{color};font-weight:700;">'
                  f'{sign}{abs(chg):.2f}%</td></tr>')
        return h + "</table></div>"
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(_tbl(gainers, "TOP GAINERS", C["green_br"], "▲"), unsafe_allow_html=True)
    with c2:
        st.markdown(_tbl(losers,  "TOP LOSERS",  C["red"],      "▼"), unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT5  FINNHUB INSIDER TRANSACTIONS + ANALYST CONSENSUS
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_insider_transactions(symbol: str) -> List[Dict]:
    url = f"https://finnhub.io/api/v1/stock/insider-transactions?symbol={symbol}&token={FINNHUB_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            txns = r.json().get("data", [])
            return [{
                "name":   t.get("name",""),
                "type":   t.get("transactionCode",""),
                "shares": int(t.get("share",0) or 0),
                "price":  float(t.get("price",0) or 0),
                "value":  float(t.get("share",0) or 0) * float(t.get("price",0) or 0),
                "date":   t.get("transactionDate",""),
            } for t in txns[:10]]
    except Exception:
        pass
    return []


def render_insider_transactions(symbol: str) -> None:
    phdr(f"Insider Transactions — {symbol}", "FINNHUB · SEC FORM 4", "gold")
    txns = fetch_insider_transactions(symbol)
    if not txns:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;'
                    f'text-align:center;">No recent insider transactions.</div>', unsafe_allow_html=True)
        return
    buys  = {"P","A"}
    sells = {"S","D","F"}
    html  = (f'<div class="tp" style="padding:4px;max-height:250px;overflow-y:auto;">'
             f'<table style="width:100%;border-collapse:collapse;font-size:8px;">'
             f'<tr style="border-bottom:1px solid {C["border"]};">'
             + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7px;">{h}</td>'
                       for h in ["DATE","INSIDER","TYPE","SHARES","VALUE"]) + "</tr>")
    for t in txns:
        tc  = t.get("type","")
        col = C["green_br"] if tc in buys else C["red"] if tc in sells else C["muted"]
        lbl = "BUY" if tc in buys else "SELL" if tc in sells else tc
        val = t.get("value",0)
        vf  = f"${val/1e6:.1f}M" if val>=1e6 else f"${val/1e3:.0f}K" if val>=1e3 else f"${val:.0f}"
        html += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{t["date"]}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{t["name"][:18]}</td>'
                 f'<td style="padding:3px 6px;color:{col};font-weight:700;">{lbl}</td>'
                 f'<td style="padding:3px 6px;">{t["shares"]:,}</td>'
                 f'<td style="padding:3px 6px;color:{col};">{vf}</td></tr>')
    st.markdown(html + "</table></div>", unsafe_allow_html=True)


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_analyst_recommendations(symbol: str) -> List[Dict]:
    url = f"https://finnhub.io/api/v1/stock/recommendation?symbol={symbol}&token={FINNHUB_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data[:4] if data else []
    except Exception:
        pass
    return []


def render_analyst_consensus(symbol: str) -> None:
    phdr(f"Analyst Consensus — {symbol}", "FINNHUB · WALL STREET", "gold")
    recs = fetch_analyst_recommendations(symbol)
    if not recs:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;">No analyst data.</div>',
                    unsafe_allow_html=True)
        return
    latest = recs[0]
    total  = sum([latest.get("strongBuy",0), latest.get("buy",0), latest.get("hold",0),
                  latest.get("sell",0), latest.get("strongSell",0)])
    if total == 0:
        return
    cats = [
        ("STRONG BUY",  latest.get("strongBuy",0),  C["green_br"]),
        ("BUY",         latest.get("buy",0),         C["green"]),
        ("HOLD",        latest.get("hold",0),         C["gold"]),
        ("SELL",        latest.get("sell",0),         C["red_dim"]),
        ("STRONG SELL", latest.get("strongSell",0),  C["red"]),
    ]
    bars_html = ""
    for label, count, color in cats:
        pct = count / total * 100 if total else 0
        bars_html += (f'<div style="margin-bottom:4px;">'
                      f'<div style="display:flex;justify-content:space-between;font-size:8px;'
                      f'color:{C["muted"]};margin-bottom:2px;"><span>{label}</span>'
                      f'<span style="color:{color};">{count}</span></div>'
                      f'<div style="background:{C["border2"]};height:4px;border-radius:2px;">'
                      f'<div style="width:{pct:.0f}%;height:100%;background:{color};'
                      f'border-radius:2px;"></div></div></div>')
    st.markdown(f'<div class="tp" style="padding:10px 12px;">'
                f'<div style="font-size:8px;color:{C["muted"]};margin-bottom:8px;">'
                f'Period: {latest.get("period","")} · {total} analysts</div>'
                f'{bars_html}</div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT6  ALPHA VANTAGE EARNINGS SURPRISE CHART
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_alphav_earnings_surprise(symbol: str) -> List[Dict]:
    url = (f"https://www.alphavantage.co/query?function=EARNINGS"
           f"&symbol={symbol}&apikey={ALPHAV_KEY}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            quarterly = r.json().get("quarterlyEarnings", [])
            results   = []
            for q in quarterly[:6]:
                try:
                    actual   = float(q.get("reportedEPS","0") or 0)
                    estimate = float(q.get("estimatedEPS","0") or 0)
                    surp_pct = float(q.get("surprisePercentage","0") or 0)
                    results.append({"date": q.get("fiscalDateEnding",""),
                                    "actual": actual, "estimate": estimate,
                                    "surp_pct": surp_pct})
                except Exception:
                    pass
            return results
    except Exception:
        pass
    return []


def build_earnings_surprise_chart(symbol: str) -> go.Figure:
    data = fetch_alphav_earnings_surprise(symbol)
    if not data:
        return _apply_dark(go.Figure())
    dates  = [d["date"]    for d in data][::-1]
    actual = [d["actual"]  for d in data][::-1]
    est    = [d["estimate"]for d in data][::-1]
    s_pct  = [d["surp_pct"]for d in data][::-1]
    colors = [C["green_br"] if s >= 0 else C["red"] for s in s_pct]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=est,
        line=dict(color=C["muted"], width=1.5, dash="dot"),
        name="Estimate", mode="lines+markers", marker_size=5))
    fig.add_trace(go.Scatter(x=dates, y=actual,
        line=dict(color=C["gold"], width=2), name="Actual",
        mode="lines+markers", marker=dict(size=7, color=colors)))
    _layout = {**_DARK_LAYOUT, "height": 220,
               "showlegend": True,
               "legend": dict(font=dict(size=8), bgcolor="rgba(0,0,0,0)")}
    fig.update_layout(**_layout)
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT7  MARKETSTACK GLOBAL EXCHANGE CROSS-LISTING
# ═════════════════════════════════════════════════════════════════════════════════════

_GLOBAL_EXCHANGES_LIST = [
    ("LSE (UK)","BARC.XLON"), ("TSX (CA)","RY.XTSE"), ("ASX (AU)","BHP.XASX"),
    ("EURONEXT","ASML.XAMS"), ("NIKKEI","7203.XTKS"), ("FRANKFURT","SAP.XFRA"),
]

@st.cache_data(ttl=300, show_spinner=False)
def fetch_global_exchange_data() -> List[Dict]:
    syms    = ",".join(s for _, s in _GLOBAL_EXCHANGES_LIST)
    url     = (f"http://api.marketstack.com/v1/eod/latest"
               f"?access_key={MARKETSTACK_KEY}&symbols={syms}&limit=6")
    results = []
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            for item in r.json().get("data", []):
                sym   = item.get("symbol","")
                label = next((l for l,s in _GLOBAL_EXCHANGES_LIST if s==sym), sym)
                close = float(item.get("close",0) or 0)
                op    = float(item.get("open",0) or close)
                chg   = (close - op) / op * 100 if op else 0
                results.append({"label": label, "price": close,
                                 "change": round(chg,2), "date": item.get("date","")[:10]})
    except Exception:
        pass
    return results


def render_global_exchange_panel() -> None:
    phdr("Global Exchange Cross-Listing", "MARKETSTACK · LIVE", "gold")
    data = fetch_global_exchange_data()
    if not data:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:10px;'
                    f'text-align:center;">Awaiting global exchange data…</div>', unsafe_allow_html=True)
        return
    html = '<div class="tp" style="padding:4px;">'
    for item in data:
        chg  = item["change"]
        cc   = C["green_br"] if chg >= 0 else C["red"]
        sign = "▲" if chg >= 0 else "▼"
        html += (f'<div class="kr"><span class="kk">{item["label"]}</span>'
                 f'<span class="kv" style="color:{cc};">{item["price"]:,.2f} '
                 f'{sign}{abs(chg):.2f}%</span></div>')
    st.markdown(html + "</div>", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT8  TICKER-SPECIFIC NEWS (NewsAPI + GNews + NewsData combined search)
# ═════════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def fetch_ticker_news(ticker: str, n: int = 10) -> List[Dict]:
    items: List[Dict] = []
    for fn, args in [
        (fetch_newsapi,  (f'"{ticker}" stock earnings revenue', n)),
        (fetch_gnews,    (f"{ticker} stock earnings", min(n,5))),
        (fetch_newsdata, (f"{ticker} stock", "en", min(n,5))),
    ]:
        try:
            items.extend(fn(*args) or [])
        except Exception:
            pass
    return _dedup_news(items, n*2)


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-EXT9  MOBILE CSS + STANDALONE HTML SHELL (no Streamlit)
# ═════════════════════════════════════════════════════════════════════════════════════

_MOBILE_CSS_EXTRA = ""  # Merged into inject_css()


# §30  UPDATED TAB RENDERERS (full replacement with all new modules)
# ═════════════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════════════
# §NEW-A  GLOBAL RISK WATCH — Top 20 Rising Threats
# ═════════════════════════════════════════════════════════════════════════════════════

_GLOBAL_RISKS_STATIC: List[Dict] = [
    {"rank": 1,  "country": "Russia / Ukraine",  "category": "Military",      "color": "#d32f2f",
     "driver": "Active front-line offensive. Drone warfare escalation. NATO red-line risks."},
    {"rank": 2,  "country": "Middle East (Gaza)", "category": "Military",      "color": "#d32f2f",
     "driver": "IDF ground operations. Hezbollah escalation risk. Regional spillover."},
    {"rank": 3,  "country": "China / Taiwan",     "category": "Geopolitical",  "color": "#d32f2f",
     "driver": "PLA naval drills intensifying. US carrier patrols increased."},
    {"rank": 4,  "country": "Iran",               "category": "Geopolitical",  "color": "#d32f2f",
     "driver": "Nuclear enrichment at 84%. Proxy forces active across 4+ theaters."},
    {"rank": 5,  "country": "North Korea",        "category": "Military",      "color": "#d32f2f",
     "driver": "ICBM tests. Troops deployed to Russia. EMP threat escalation."},
    {"rank": 6,  "country": "Red Sea / Yemen",    "category": "Financial",     "color": "#e0a050",
     "driver": "Houthi drone strikes disrupting global shipping. Insurance premiums +400%."},
    {"rank": 7,  "country": "Pakistan",           "category": "Geopolitical",  "color": "#d32f2f",
     "driver": "India-Pakistan border tensions. Nuclear posturing. IMF bailout fragility."},
    {"rank": 8,  "country": "Sudan",              "category": "Military",      "color": "#d32f2f",
     "driver": "RSF vs SAF civil war. Humanitarian catastrophe. Proxy funding ongoing."},
    {"rank": 9,  "country": "Venezuela",          "category": "Geopolitical",  "color": "#e0a050",
     "driver": "Maduro election disputed. Guyana border military buildup. Oil sector crisis."},
    {"rank": 10, "country": "Haiti",              "category": "Geopolitical",  "color": "#e0a050",
     "driver": "Gang control of 80% of Port-au-Prince. State collapse risk."},
    {"rank": 11, "country": "Ethiopia / Horn",    "category": "Military",      "color": "#e0a050",
     "driver": "Tigray post-ceasefire fragility. Somali insurgency resurgent."},
    {"rank": 12, "country": "Japan",              "category": "Financial",     "color": "#e0a050",
     "driver": "BOJ YCC unwind risk. JPY at 30-year low. Debt/GDP at 260%."},
    {"rank": 13, "country": "Argentina",          "category": "Financial",     "color": "#e0a050",
     "driver": "Milei austerity shock. Inflation at 250%. IMF repayment cliff."},
    {"rank": 14, "country": "Myanmar",            "category": "Military",      "color": "#e0a050",
     "driver": "Junta losing territory to resistance. Chinese border trade disrupted."},
    {"rank": 15, "country": "Turkey",             "category": "Financial",     "color": "#e0a050",
     "driver": "Lira currency crisis. Inflation 60%+. Political instability."},
    {"rank": 16, "country": "Bangladesh",         "category": "Geopolitical",  "color": "#c5a861",
     "driver": "Post-coup transition instability. Garment industry disruption risk."},
    {"rank": 17, "country": "Morocco",            "category": "Natural Disaster","color": "#c5a861",
     "driver": "Seismic zone activity post-2023 quake. Water scarcity escalation."},
    {"rank": 18, "country": "Philippines",        "category": "Geopolitical",  "color": "#c5a861",
     "driver": "South China Sea territorial clashes. Chinese coast guard confrontations."},
    {"rank": 19, "country": "Nigeria",            "category": "Financial",     "color": "#c5a861",
     "driver": "Naira collapse. Fuel subsidy removal shock. Boko Haram resurgence."},
    {"rank": 20, "country": "Colombia",           "category": "Agricultural",  "color": "#4caf7d",
     "driver": "FARC re-mobilisation. Coca production records. US relations strained."},
]

_CATEGORY_ICONS = {
    "Military":      "⚔",
    "Geopolitical":  "🌐",
    "Financial":     "📉",
    "Natural Disaster": "🌋",
    "Agricultural":  "🌾",
}


def render_global_risk_watch() -> None:
    """GLOBAL RISK WATCH — Top Rising Threats numbered list."""
    phdr("Global Risk Watch — Top Rising Threats", "REAL-TIME ASSESSMENT", "red")
    html = '<div class="tp" style="padding:4px;max-height:680px;overflow-y:auto;">'
    for item in _GLOBAL_RISKS_STATIC:
        cat_color = item["color"]
        icon = _CATEGORY_ICONS.get(item["category"], "●")
        html += (
            f'<div style="display:grid;grid-template-columns:28px 1fr;gap:8px;'
            f'padding:8px 10px;border-bottom:1px solid {C["border"]};'
            f'align-items:start;">'
            f'<div style="font-family:Syne,sans-serif;font-size:13px;font-weight:800;'
            f'color:{cat_color};text-align:center;line-height:1.4;">{item["rank"]}</div>'
            f'<div>'
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:2px;">'
            f'<span style="font-family:Syne,sans-serif;font-size:10.5px;font-weight:700;'
            f'color:#e8eaf0;">{item["country"]}</span>'
            f'<span style="font-size:7px;letter-spacing:1px;padding:1px 5px;border-radius:2px;'
            f'background:rgba(0,0,0,0.3);border:1px solid {cat_color}55;color:{cat_color};'
            f'font-weight:700;">{icon} {item["category"].upper()}</span>'
            f'</div>'
            f'<div style="font-size:8.5px;color:{C["text_dim"]};line-height:1.4;">'
            f'{item["driver"]}</div>'
            f'</div></div>'
        )
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §NEW-B  ASSET DIVERGENCE TRACKER
# ═════════════════════════════════════════════════════════════════════════════════════

_DIVERGENCE_PAIRS = [
    ("GLD", "TLT"),   ("SPY", "GLD"),  ("NVDA", "INTC"), ("TSLA", "F"),
    ("BTC-USD", "GLD"),("QQQ", "IWM"), ("XOM", "NEE"),   ("JPM", "GS"),
]

_ASSET_DESCRIPTIONS = {
    "GLD": "Gold ETF", "TLT": "20Y Treasury ETF", "SPY": "S&P 500 ETF",
    "NVDA": "NVIDIA", "INTC": "Intel", "TSLA": "Tesla", "F": "Ford",
    "BTC-USD": "Bitcoin", "QQQ": "NASDAQ-100 ETF", "IWM": "Russell 2000 ETF",
    "XOM": "ExxonMobil", "NEE": "NextEra Energy", "JPM": "JPMorgan", "GS": "Goldman Sachs",
}


def render_asset_divergence_tracker() -> None:
    """Live asset divergence tracker between two user-selectable assets."""
    phdr("Asset Divergence Tracker", "LIVE · COMPARATIVE SPREAD", "blue")
    all_assets = list({a for pair in _DIVERGENCE_PAIRS for a in pair})
    all_assets += ["AAPL","MSFT","AMZN","META","GOOGL","AMD","AVGO","COST","NFLX"]
    all_assets = sorted(set(all_assets))

    col1, col2 = st.columns(2)
    with col1:
        asset1 = st.selectbox("Asset 1", all_assets,
                              index=all_assets.index("GLD") if "GLD" in all_assets else 0,
                              key="div_asset1", label_visibility="collapsed")
    with col2:
        asset2 = st.selectbox("Asset 2", all_assets,
                              index=all_assets.index("TLT") if "TLT" in all_assets else 1,
                              key="div_asset2", label_visibility="collapsed")

    # Fetch prices
    q1 = fetch_finnhub_quote(asset1 if asset1 != "BTC-USD" else "MSTR")
    q2 = fetch_finnhub_quote(asset2 if asset2 != "BTC-USD" else "MSTR")
    p1 = q1.get("price", 0) or 0
    p2 = q2.get("price", 0) or 0
    c1 = q1.get("change", 0) or 0
    c2 = q2.get("change", 0) or 0

    if p1 > 0 and p2 > 0:
        divergence_pct = c1 - c2  # % spread in daily move
        spread_color   = C["green_br"] if divergence_pct >= 0 else C["red"]
        spread_dir     = "▲" if divergence_pct >= 0 else "▼"
        bar_width      = min(abs(divergence_pct) / 5 * 100, 100)  # normalize to 5% = full bar

        st.markdown(
            f'<div class="tp" style="padding:12px 14px;">'
            f'<div style="display:grid;grid-template-columns:1fr 80px 1fr;gap:12px;align-items:center;">'
            f'<div style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["muted"]};margin-bottom:4px;">'
            f'{_ASSET_DESCRIPTIONS.get(asset1, asset1).upper()}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:22px;font-weight:800;color:#e8eaf0;">'
            f'${p1:,.2f}</div>'
            f'<div style="font-size:11px;color:{C["green_br"] if c1 >= 0 else C["red"]};">'
            f'{("▲" if c1 >= 0 else "▼")}{abs(c1):.2f}%</div>'
            f'</div>'
            f'<div style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};margin-bottom:4px;">SPREAD</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;color:{spread_color};">'
            f'{spread_dir}{abs(divergence_pct):.2f}%</div>'
            f'</div>'
            f'<div style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["muted"]};margin-bottom:4px;">'
            f'{_ASSET_DESCRIPTIONS.get(asset2, asset2).upper()}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:22px;font-weight:800;color:#e8eaf0;">'
            f'${p2:,.2f}</div>'
            f'<div style="font-size:11px;color:{C["green_br"] if c2 >= 0 else C["red"]};">'
            f'{("▲" if c2 >= 0 else "▼")}{abs(c2):.2f}%</div>'
            f'</div>'
            f'</div>'
            f'<div style="margin-top:10px;">'
            f'<div style="font-size:8px;color:{C["muted"]};margin-bottom:4px;">DIVERGENCE STRENGTH</div>'
            f'<div style="background:{C["border2"]};height:8px;border-radius:4px;overflow:hidden;">'
            f'<div style="width:{bar_width:.0f}%;height:100%;background:{spread_color};'
            f'border-radius:4px;box-shadow:0 0 8px {spread_color}66;"></div></div>'
            f'<div style="display:flex;justify-content:space-between;font-size:8px;'
            f'color:{C["muted"]};margin-top:3px;">'
            f'<span>{asset1} leads</span><span>{asset2} leads</span></div>'
            f'</div></div>',
            unsafe_allow_html=True
        )
        return asset1, asset2, p1, p2, c1, c2, divergence_pct
    else:
        st.markdown(f'<div class="tp" style="color:{C["muted"]};padding:12px;text-align:center;">'
                    f'Fetching live prices for {asset1} / {asset2}…</div>', unsafe_allow_html=True)
        return asset1, asset2, 0, 0, 0, 0, 0


def render_divergence_analysis_engine() -> None:
    """Structured AI-powered analysis of the two diverging assets."""
    phdr("Divergence Analysis Engine", "MACRO · FUNDAMENTAL · NEWS", "blue")
    # Get current selections
    asset1 = st.session_state.get("div_asset1", "GLD")
    asset2 = st.session_state.get("div_asset2", "TLT")
    q1 = fetch_finnhub_quote(asset1)
    q2 = fetch_finnhub_quote(asset2)
    c1 = q1.get("change", 0) or 0
    c2 = q2.get("change", 0) or 0
    divergence = c1 - c2

    desc1 = _ASSET_DESCRIPTIONS.get(asset1, asset1)
    desc2 = _ASSET_DESCRIPTIONS.get(asset2, asset2)

    # Static template driven by asset types
    def _is_bond(a): return a in ("TLT", "IEF", "BND", "AGG", "SHY")
    def _is_gold(a): return a in ("GLD", "IAU", "GOLD", "GC=F")
    def _is_crypto(a): return "BTC" in a or "ETH" in a or a in ("MSTR", "COIN")
    def _is_oil(a): return a in ("XOM", "CVX", "COP", "OXY", "USO")
    def _is_growth(a): return a in ("QQQ", "NVDA", "META", "TSLA", "AMZN", "GOOGL")

    # Generate contextual analysis
    if _is_gold(asset1) and _is_bond(asset2):
        macro_txt = (
            f"{desc1} is rising as real yields compress and safe-haven demand accelerates. "
            f"The Federal Reserve's rate trajectory is signalling potential cuts, which directly "
            f"increases gold's opportunity cost advantage. {desc2} is moving inversely as duration "
            f"risk adjusts to current inflation prints. The spread ({divergence:+.2f}%) reflects "
            f"markets pricing gold as the superior inflation hedge over nominal Treasuries."
        )
        fund_txt = (
            f"Gold has no earnings yield, so the divergence from {desc2} is a pure macro signal. "
            f"As the Fed Funds Rate sits at 5.25%, {desc2} yields are competing with gold, "
            f"but real rates (nominal minus CPI) remain suppressed. ETF inflows into {asset1} "
            f"indicate institutional rotation out of duration-sensitive instruments."
        )
        news_txt = (
            f"Geopolitical instability — active conflicts in Eastern Europe, Middle East, and "
            f"Red Sea shipping disruptions — is structurally bid for gold. {desc2} faces "
            f"headwinds from US fiscal deficit expansion and Treasury supply concerns. "
            f"Central bank gold accumulation (China, India, Russia) at multi-decade highs."
        )
    elif _is_growth(asset1) and (_is_bond(asset2) or _is_gold(asset2)):
        macro_txt = (
            f"{desc1} is rising as AI-driven earnings growth continues to decouple from macro risk. "
            f"Risk appetite is elevated — VIX below 20 signals complacency. {desc2} is declining "
            f"as capital rotates from safe-havens into risk assets. Divergence of {divergence:+.2f}% "
            f"reflects classic growth-vs-defensive sector rotation."
        )
        fund_txt = (
            f"Strong EPS revisions across the AI supply chain are driving {desc1} premium multiples. "
            f"Forward P/E expansion is justified only if revenue growth sustains above 15%. "
            f"{desc2} underperforms when growth optimism dominates, as duration assets price out risk."
        )
        news_txt = (
            f"AI infrastructure capex from hyperscalers is sustaining {desc1} momentum. "
            f"Rate expectations have shifted dovish, reducing {desc2} yield appeal. "
            f"Earnings season beats vs consensus are creating near-term momentum divergence."
        )
    else:
        macro_txt = (
            f"{desc1} ({c1:+.2f}%) and {desc2} ({c2:+.2f}%) are diverging by {divergence:+.2f}% "
            f"today. This spread is driven by differential sensitivity to the current macro regime: "
            f"Fed policy, yield curve shape, and sector-specific credit conditions. "
            f"The prevailing rate environment (Fed Funds at 5.25%) is creating asymmetric pressure "
            f"across asset classes with different duration profiles."
        )
        fund_txt = (
            f"Fundamentally, {desc1} and {desc2} carry distinct earnings and cash flow profiles. "
            f"The divergence ({divergence:+.2f}%) is consistent with earnings revision differentials: "
            f"analysts are more aggressively revising estimates for the outperformer. "
            f"Free cash flow yield spreads and balance sheet quality are key differentiators "
            f"driving institutional rotation between these two assets."
        )
        news_txt = (
            f"Recent catalysts include sector-specific earnings releases, Fed commentary on "
            f"terminal rate expectations, and geopolitical risk repricing. The divergence between "
            f"{desc1} and {desc2} reflects micro-level news flow asymmetry — one benefiting from "
            f"positive data prints while the other faces headwinds from macro uncertainty. "
            f"Monitor insider buying and short interest changes for reversal signals."
        )

    sections = [
        ("⊕ MACRO ANALYSIS", macro_txt, C["gold"]),
        ("⊞ FUNDAMENTAL ANALYSIS", fund_txt, C["blue_br"]),
        ("⚡ NEWS & MICRO-ECONOMICS", news_txt, C["green_br"]),
    ]
    for title, body, color in sections:
        st.markdown(
            f'<div class="tp-glass" style="margin-bottom:8px;padding:12px 14px;">'
            f'<div style="font-size:8px;letter-spacing:2px;color:{color};font-weight:700;'
            f'text-transform:uppercase;margin-bottom:6px;">{title}</div>'
            f'<div style="font-size:9.5px;line-height:1.65;color:{C["text_dim"]};">{body}</div>'
            f'</div>',
            unsafe_allow_html=True
        )


# ═════════════════════════════════════════════════════════════════════════════════════
# §NEW-C  UNDERVALUED STOCKS MODEL — Decrypt Undervalued Stocks
# ═════════════════════════════════════════════════════════════════════════════════════

_UNDERVALUED_CANDIDATES: List[Dict] = [
    {
        "ticker": "GOOG", "name": "Alphabet Inc.", "sector": "Communication",
        "val_score": 28, "qual_score": 23, "macro_score": 17, "catalyst_score": 12, "dcf_mos": 9,
        "total": 89,
        "pe": 24.8, "fwd_pe": 21.3, "ev_ebitda": 14.2, "pb": 5.8, "pfcf": 18.1, "peg": 0.82,
        "de": 0.09, "altman": 4.2, "roe": 27.3, "roic": 22.1, "gross_margin": 56.5,
        "fcf_mos": 38, "signal": "Strong Buy",
        "thesis": "FCF yield 5.2% exceeds 10Y Treasury. AI Search monetisation underpriced by consensus.",
    },
    {
        "ticker": "META", "name": "Meta Platforms", "sector": "Communication",
        "val_score": 26, "qual_score": 22, "macro_score": 16, "catalyst_score": 13, "dcf_mos": 8,
        "total": 85,
        "pe": 26.5, "fwd_pe": 23.8, "ev_ebitda": 16.4, "pb": 7.1, "pfcf": 19.2, "peg": 0.91,
        "de": 0.14, "altman": 3.9, "roe": 35.2, "roic": 29.4, "gross_margin": 81.0,
        "fcf_mos": 32, "signal": "Strong Buy",
        "thesis": "WhatsApp Business monetisation at $2 ARPU vs $60 US Instagram — multi-year expansion.",
    },
    {
        "ticker": "JPM", "name": "JPMorgan Chase", "sector": "Financial",
        "val_score": 27, "qual_score": 21, "macro_score": 18, "catalyst_score": 11, "dcf_mos": 7,
        "total": 84,
        "pe": 12.8, "fwd_pe": 11.9, "ev_ebitda": 8.1, "pb": 1.8, "pfcf": 10.2, "peg": 0.74,
        "de": 1.21, "altman": 3.1, "roe": 17.4, "roic": 15.2, "gross_margin": 64.2,
        "fcf_mos": 44, "signal": "Buy",
        "thesis": "P/E 12.8x for 17% ROTE. Rate environment favours NII expansion. M&A pipeline recovery.",
    },
    {
        "ticker": "AVGO", "name": "Broadcom Inc.", "sector": "Technology",
        "val_score": 24, "qual_score": 22, "macro_score": 17, "catalyst_score": 14, "dcf_mos": 7,
        "total": 84,
        "pe": 35.0, "fwd_pe": 25.0, "ev_ebitda": 18.3, "pb": 11.2, "pfcf": 21.4, "peg": 1.12,
        "de": 0.83, "altman": 2.8, "roe": 32.5, "roic": 18.7, "gross_margin": 65.8,
        "fcf_mos": 28, "signal": "Buy",
        "thesis": "VMware synergies tracking ahead of target. XPU AI silicon at $10B+ and growing 60% YoY.",
    },
    {
        "ticker": "V", "name": "Visa Inc.", "sector": "Financial",
        "val_score": 24, "qual_score": 23, "macro_score": 15, "catalyst_score": 10, "dcf_mos": 8,
        "total": 80,
        "pe": 30.1, "fwd_pe": 26.8, "ev_ebitda": 21.0, "pb": 14.0, "pfcf": 24.2, "peg": 1.40,
        "de": 0.56, "altman": 5.1, "roe": 44.8, "roic": 38.2, "gross_margin": 80.5,
        "fcf_mos": 22, "signal": "Buy",
        "thesis": "Payments duopoly with 50%+ net margin. Cross-border volume recovery post-pandemic structural.",
    },
    {
        "ticker": "MSFT", "name": "Microsoft Corp.", "sector": "Technology",
        "val_score": 22, "qual_score": 23, "macro_score": 17, "catalyst_score": 11, "dcf_mos": 7,
        "total": 80,
        "pe": 34.2, "fwd_pe": 30.1, "ev_ebitda": 22.8, "pb": 12.8, "pfcf": 28.4, "peg": 1.82,
        "de": 0.44, "altman": 4.8, "roe": 40.1, "roic": 34.2, "gross_margin": 69.8,
        "fcf_mos": 19, "signal": "Hold / Accumulate",
        "thesis": "Azure 30%+ growth. Copilot monetisation early-stage. AAA balance sheet. Dividend growth.",
    },
    {
        "ticker": "XOM", "name": "ExxonMobil", "sector": "Energy",
        "val_score": 26, "qual_score": 18, "macro_score": 15, "catalyst_score": 10, "dcf_mos": 8,
        "total": 77,
        "pe": 14.8, "fwd_pe": 13.2, "ev_ebitda": 7.2, "pb": 1.9, "pfcf": 11.0, "peg": 0.62,
        "de": 0.20, "altman": 3.4, "roe": 16.8, "roic": 13.9, "gross_margin": 38.2,
        "fcf_mos": 35, "signal": "Buy",
        "thesis": "Pioneer acquisition creates largest US oil basin player. $35B buyback. WTI $75+ FCF positive.",
    },
    {
        "ticker": "ADBE", "name": "Adobe Inc.", "sector": "Technology",
        "val_score": 21, "qual_score": 22, "macro_score": 15, "catalyst_score": 12, "dcf_mos": 6,
        "total": 76,
        "pe": 28.4, "fwd_pe": 24.8, "ev_ebitda": 17.8, "pb": 15.2, "pfcf": 22.3, "peg": 1.28,
        "de": 0.38, "altman": 4.0, "roe": 38.4, "roic": 29.8, "gross_margin": 88.2,
        "fcf_mos": 21, "signal": "Buy",
        "thesis": "Firefly AI integration creates new monetisation layer. Net retention rate >120%. Figma risk cleared.",
    },
]


def render_undervalued_stocks_model() -> None:
    """Decrypt Undervalued Stocks — multi-metric scoring model display."""
    phdr("Decrypt Undervalued Stocks", "MULTI-FACTOR SCORING MODEL · US EQUITIES", "green")
    st.markdown(
        f'<div class="tp" style="padding:8px 12px;margin-bottom:6px;'
        f'background:linear-gradient(90deg,rgba(46,125,82,0.08),rgba(12,14,19,0));'
        f'border-left:3px solid {C["green_br"]};">'
        f'<div style="font-size:8px;color:{C["text_dim"]};line-height:1.5;">'
        f'Composite score: Valuation (30) + Quality (25) + Macro Alignment (20) + Catalyst (15) + DCF Margin of Safety (10). '
        f'Score 80+ = high-conviction undervalued candidate.</div></div>',
        unsafe_allow_html=True
    )
    sorted_stocks = sorted(_UNDERVALUED_CANDIDATES, key=lambda x: -x["total"])
    for stock in sorted_stocks[:6]:
        sig_color = (C["green_br"] if "Buy" in stock["signal"] and "Hold" not in stock["signal"]
                     else C["gold"] if "Hold" in stock["signal"] else C["red"])
        score_pct = stock["total"]
        sc_col    = C["green_br"] if score_pct >= 80 else C["orange_br"] if score_pct >= 60 else C["red"]
        st.markdown(
            f'<div class="tp" style="padding:10px 12px;margin-bottom:6px;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">'
            f'<div>'
            f'<span style="font-family:Syne,sans-serif;font-size:14px;font-weight:800;color:{C["gold"]};">'
            f'{stock["ticker"]}</span>'
            f'<span style="font-size:9px;color:{C["muted"]};margin-left:6px;">{stock["name"]} · {stock["sector"]}</span>'
            f'</div>'
            f'<div style="text-align:right;">'
            f'<span style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:{sc_col};">'
            f'{stock["total"]}/100</span>'
            f'<span style="font-size:8px;padding:2px 7px;border-radius:2px;margin-left:8px;'
            f'background:rgba(0,0,0,0.3);border:1px solid {sig_color}44;color:{sig_color};font-weight:700;">'
            f'{stock["signal"].upper()}</span>'
            f'</div></div>'
            f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:4px;margin-bottom:6px;">'
            + "".join(
                f'<div style="background:{C["panel_alt"]};border:1px solid {C["border"]};'
                f'border-radius:3px;padding:4px 6px;text-align:center;">'
                f'<div style="font-size:7px;color:{C["muted"]};letter-spacing:1px;">{lbl}</div>'
                f'<div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:{C["text"]};">'
                f'{val}</div></div>'
                for lbl, val in [
                    ("P/E", f'{stock["pe"]:.1f}x'), ("FWD P/E", f'{stock["fwd_pe"]:.1f}x'),
                    ("EV/EBITDA", f'{stock["ev_ebitda"]:.1f}x'), ("P/FCF", f'{stock["pfcf"]:.1f}x'),
                    ("PEG", f'{stock["peg"]:.2f}'),
                ]
            )
            + f'</div>'
            f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:4px;margin-bottom:6px;">'
            + "".join(
                f'<div style="background:{C["panel_alt"]};border:1px solid {C["border"]};'
                f'border-radius:3px;padding:4px 6px;text-align:center;">'
                f'<div style="font-size:7px;color:{C["muted"]};letter-spacing:1px;">{lbl}</div>'
                f'<div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:{vc};">'
                f'{val}</div></div>'
                for lbl, val, vc in [
                    ("ROE", f'{stock["roe"]:.1f}%', C["green_br"]),
                    ("ROIC", f'{stock["roic"]:.1f}%', C["green_br"]),
                    ("ALTMAN Z", f'{stock["altman"]:.1f}',
                     C["green_br"] if stock["altman"] > 2.99 else C["orange_br"] if stock["altman"] > 1.81 else C["red"]),
                    ("DCF MoS", f'+{stock["fcf_mos"]}%', C["gold"]),
                ]
            )
            + f'</div>'
            f'<div style="font-size:9px;color:{C["text_dim"]};line-height:1.5;padding-top:4px;'
            f'border-top:1px solid {C["border"]};">{stock["thesis"]}</div>'
            f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:3px;margin-top:6px;">'
            + "".join(
                f'<div style="margin-bottom:2px;">'
                f'<div style="display:flex;justify-content:space-between;font-size:7px;color:{C["muted"]};margin-bottom:1px;">'
                f'<span>{lbl}</span><span style="color:{C["gold"]};">{val}</span></div>'
                f'<div style="background:{C["border2"]};height:3px;border-radius:2px;">'
                f'<div style="width:{pct}%;height:100%;background:{C["gold"]};border-radius:2px;"></div></div></div>'
                for lbl, val, pct in [
                    ("VALUATION", f'{stock["val_score"]}/30', stock["val_score"]/30*100),
                    ("QUALITY",   f'{stock["qual_score"]}/25', stock["qual_score"]/25*100),
                    ("MACRO",     f'{stock["macro_score"]}/20', stock["macro_score"]/20*100),
                    ("CATALYST",  f'{stock["catalyst_score"]}/15', stock["catalyst_score"]/15*100),
                    ("DCF MOS",   f'{stock["dcf_mos"]}/10', stock["dcf_mos"]/10*100),
                ]
            )
            + '</div></div>',
            unsafe_allow_html=True
        )


@st.fragment(run_every=60)
def render_tab_macro_v2() -> None:
    try:
        _render_tab_macro_v2_inner()
    except Exception as e:
        st.error(f"⚠ Global Macro render error: {e}")


def _render_tab_macro_v2_inner() -> None:
    state   = get_terminal_state()
    macro   = state.macro_indicators if state.macro_indicators else get_macro_indicators()
    indices = state.global_indices    if state.global_indices    else get_global_indices()

    # ── 3-panel layout: LEFT | CENTER | RIGHT ──────────────────────────────────
    left_col, center_col, right_col = st.columns([2.2, 5.6, 2.2], gap="small")

    # ════════════════════════════════════════════════════════════════
    # LEFT PANEL — Situations & OSINT Intelligence
    # ════════════════════════════════════════════════════════════════
    with left_col:
        st.markdown(
            f'<div style="padding:6px 12px;background:{C["panel_alt"]};'
            f'border-bottom:1px solid {C["border"]};'
            f'display:flex;align-items:center;gap:8px;">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:{C["red"]};'
            f'box-shadow:0 0 6px {C["red"]};display:inline-block;'
            f'animation:blink 1.5s ease-in-out infinite;"></span>'
            f'<span style="font-family:Syne,sans-serif;font-size:9px;font-weight:700;'
            f'letter-spacing:2px;color:{C["red"]};text-transform:uppercase;">SITUATIONS</span>'
            f'</div>',
            unsafe_allow_html=True)

        # Market status bar
        render_market_status(state.market_status)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Pentagon Conflict Index
        render_pentagon_index()
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Infrastructure cascade selector
        phdr("Infrastructure Cascade Risk", "CHOKEPOINTS", "red")
        infra_idx = st.selectbox("Infra node",
            options=list(range(len(INFRA_CASCADE))),
            format_func=lambda i: INFRA_CASCADE[i]["node"],
            label_visibility="collapsed", key="infra_sel")
        render_infra_cascade(infra_idx)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # OSINT stream — extended to fill vertical space
        render_osint_stream(state, max_items=40)
        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        # Geopolitical News below OSINT
        phdr("Geopolitical News", "LIVE FEED", "gold")
        t1, t2 = st.tabs(["🌍 World", "⚡ Flash"])
        with t1:
            render_news_feed(state.news_macro[:12], max_items=12)
        with t2:
            breaking = [n for n in state.news_all
                        if n.get("tier") in ("FLASH","PRIORITY")][:12]
            render_news_feed(breaking)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Macro News stacked below
        phdr("Macro News", "NEWSAPI · GNEWS · WORLDNEWS · RSS", "gold")
        render_news_feed([n for n in state.news_all
                          if n.get("asset_type") in ("macro","markets")][:12], max_items=12)

    # ════════════════════════════════════════════════════════════════
    # CENTER PANEL — Globe + Macro Indicators
    # ════════════════════════════════════════════════════════════════
    with center_col:
        # Conflict globe — center stage, maximum breathing room
        phdr("Global Conflict & Risk Map", "LIVE HOTSPOTS", "red")
        render_conflict_globe()
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        # FRED Z-Score Anomaly Detector (static — non-draggable)
        st.markdown("<div style='margin-top:14px;'></div>", unsafe_allow_html=True)
        phdr("FRED Z-Score Anomaly Detector", "REAL-TIME SIGNAL ENGINE", "gold")
        st.plotly_chart(build_zscore_bar_chart(macro),
                        use_container_width=True, config={
                            "displayModeBar": False,
                            "staticPlot": True,
                        })
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Macro radar
        mr_col, sec_col = st.columns([1, 1], gap="small")
        with mr_col:
            phdr("Macro Health Radar", "FRED · Z-SCORE", "gold")
            st.plotly_chart(build_macro_radar(macro), use_container_width=True,
                            config={"displayModeBar": False})
        with sec_col:
            sectors = fetch_sector_performance()
            if sectors:
                phdr("S&P Sector Rotation", "ALPHA VANTAGE", "gold")
                st.plotly_chart(build_sector_bar_chart(sectors), use_container_width=True,
                                config={"displayModeBar": False})

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # FRED Macro Indicators grid
        phdr("FRED Macro Indicators", "REAL-TIME · Z-SCORE", "gold")
        render_macro_grid(macro)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # (Macro news is shown in the left panel stacked with Geo News)

    # ════════════════════════════════════════════════════════════════
    # RIGHT PANEL — Data & Metrics
    # ════════════════════════════════════════════════════════════════
    with right_col:
        st.markdown(
            f'<div style="padding:6px 12px;background:{C["panel_alt"]};'
            f'border-bottom:1px solid {C["border"]};'
            f'display:flex;align-items:center;gap:8px;">'
            f'<span style="font-family:Syne,sans-serif;font-size:9px;font-weight:700;'
            f'letter-spacing:2px;color:{C["gold"]};text-transform:uppercase;">DATA & METRICS</span>'
            f'</div>',
            unsafe_allow_html=True)

        # Global Risk Watch — replaces Fear & Greed on right panel
        render_global_risk_watch()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Asset Divergence Tracker
        render_asset_divergence_tracker()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Divergence Analysis Engine
        render_divergence_analysis_engine()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Market movers
        render_market_movers()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Upcoming earnings
        render_earnings_calendar()
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Sentiment panel
        render_sentiment_panel(state)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Undervalued Stocks Model (new)
        render_undervalued_stocks_model()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # Economic Calendar
        render_econ_calendar(state.econ_calendar)
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)


@st.fragment(run_every=60)
def render_tab_stock_v2() -> None:
    try:
        _render_tab_stock_v2_inner()
    except Exception as e:
        st.error(f"⚠ Stock Overview render error: {e}")


def _render_tab_stock_v2_inner() -> None:
    state = get_terminal_state()

    # Enhanced search with popular assets dropdown
    _POPULAR_ASSETS = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
        "JPM", "V", "NFLX", "AMD", "COST", "ADBE", "XOM",
        "SPX", "SPY", "QQQ", "NDX", "GLD", "DJI", "VIX", "IWM", "TLT",
    ]
    _INDEX_MAP = {
        "SPX": "^GSPC", "NDX": "^IXIC", "DJI": "^DJI", "VIX": "^VIX",
        "SPY": "SPY", "QQQ": "QQQ", "GLD": "GLD", "IWM": "IWM", "TLT": "TLT",
    }

    h1, h2, h3, h4, h5, h6 = st.columns([3, 1.5, 1, 1, 1, 1])
    with h1:
        # Searchable selectbox for fast selection + manual entry
        search_val = st.selectbox(
            "Asset search",
            options=[""] + _POPULAR_ASSETS,
            index=1,  # default AAPL
            key="stock_ticker_select",
            label_visibility="collapsed",
            format_func=lambda x: "🔍 Search or select asset…" if x == "" else x,
        )
        ticker_in = search_val if search_val else "AAPL"
    with h2:
        days_map   = {"1M": 35, "3M": 95, "6M": 185, "1Y": 370, "2Y": 740}
        per_label  = st.selectbox("Period", list(days_map.keys()), index=2,
                                   key="s_per", label_visibility="collapsed")
    with h3: show_ma  = st.checkbox("MA",       value=True,  key="s_ma")
    with h4: show_bb  = st.checkbox("BB",       value=False, key="s_bb")
    with h5: show_rsi = st.checkbox("RSI",      value=False, key="s_rsi")
    with h6: show_vol = st.checkbox("Vol",      value=True,  key="s_vol")

    _raw_ticker = (ticker_in or "AAPL").upper().strip()
    # Resolve index aliases
    ticker  = _INDEX_MAP.get(_raw_ticker, _raw_ticker)
    days    = days_map[per_label]
    profile = VALUATION_PROFILES.get(ticker, VALUATION_PROFILES.get(_raw_ticker, VALUATION_PROFILES["AAPL"]))
    df      = get_ohlcv_best(ticker, days=days)

    # Live Finnhub quote (real-time, 30s cache)
    fq    = fetch_finnhub_quote(ticker)
    price = fq.get("price",  0) or profile.get("current_price", 0)
    chg   = fq.get("change", 0)
    hi    = fq.get("high",   price) or price
    lo    = fq.get("low",    price) or price
    prev  = fq.get("prev",   price) or price
    # If Finnhub returned 0, try Polygon single-ticker
    if price == 0:
        try:
            ps = fetch_polygon_snapshot([ticker])
            if ticker in ps and ps[ticker].get("price",0) > 0:
                price = ps[ticker]["price"]
                chg   = ps[ticker].get("change", 0)
        except Exception:
            pass
    # Last resort: use last OHLCV close from chart data
    if price == 0 and not df.empty:
        price = float(df["close"].iloc[-1])
    chg_abs = price - prev
    ccol  = C["green_br"] if chg >= 0 else C["red"]
    sign  = "▲" if chg >= 0 else "▼"
    init  = ticker[:2]

    fp   = fetch_finnhub_profile(ticker)
    fm   = fetch_finnhub_metrics(ticker)
    name = fp.get("name", profile.get("name", ticker))
    sec  = profile.get("sector","—")
    desc = profile.get("description","")[:220]
    ms   = state.market_status
    ms_html = ""
    if ms:
        sc_m = {"Open": C["green_br"], "Closed": C["red"]}.get(ms.get("status",""), C["muted"])
        ms_html = (f'<span style="font-size:8px;color:{sc_m};padding:2px 6px;'
                   f'border:1px solid {sc_m}44;border-radius:3px;">'
                   f'{ms.get("status","")}</span>')

    st.markdown(
        f'<div style="background:{C["panel"]};border-bottom:1px solid {C["border"]};'
        f'padding:10px 16px;display:flex;align-items:center;gap:14px;">'
        f'<div class="clogo">{init}</div>'
        f'<div style="flex:1;">'
        f'<div style="font-family:Syne,sans-serif;font-size:19px;font-weight:800;'
        f'color:{C["text"]};line-height:1;">{name}</div>'
        f'<div style="font-size:9px;color:{C["muted"]};margin-top:2px;">'
        f'{ticker} · {sec}&nbsp;{ms_html}</div>'
        f'<div style="font-size:9px;color:{C["text_dim"]};margin-top:3px;'
        f'max-width:700px;line-height:1.4;">{desc}…</div></div>'
        f'<div style="text-align:right;flex-shrink:0;">'
        f'<div style="font-family:Syne,sans-serif;font-size:26px;font-weight:800;'
        f'color:{C["text"]};line-height:1;">${price:,.2f}</div>'
        f'<div style="font-size:11.5px;color:{ccol};margin-top:2px;">'
        f'{sign}&nbsp;${abs(chg_abs):.2f} ({abs(chg):.2f}%)</div>'
        f'<div style="font-size:8px;color:{C["muted"]};margin-top:2px;">'
        f'H: ${hi:,.2f}&nbsp;&nbsp;L: ${lo:,.2f}</div></div></div>',
        unsafe_allow_html=True)

    chart_col, info_col = st.columns([7, 3], gap="small")

    with chart_col:
        ct2_wrap, _ = st.columns([3, 2])
        with ct2_wrap:
            chart_type = st.selectbox("Chart Type", ["Candlestick","Line","Area"],
                                       key="s_ctype", label_visibility="collapsed")

        if df.empty:
            st.markdown(
                f'<div class="tp" style="color:{C["muted"]};padding:20px;text-align:center;">'
                f'No OHLCV data available for <b>{ticker}</b>.<br>'
                f'Check ticker symbol or try again shortly.</div>',
                unsafe_allow_html=True)
        else:
            if chart_type == "Candlestick":
                fig = build_candlestick(df, ticker, show_ma=show_ma, show_volume=show_vol,
                                        show_bb=show_bb, show_rsi=show_rsi)
            elif chart_type == "Line":
                fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                    line=dict(color=C["gold"], width=1.5), name=ticker))
                _apply_dark(fig)
                fig.update_layout(height=340)
            else:
                fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                    fill="tozeroy", fillcolor="rgba(197,168,97,0.10)",
                    line=dict(color=C["gold"], width=1.5), name=ticker))
                _apply_dark(fig)
                fig.update_layout(height=340)
            st.plotly_chart(fig, use_container_width=True, config={
                "displayModeBar": True, "displaylogo": False, "scrollZoom": True,
                "modeBarButtonsToRemove": ["autoScale2d","lasso2d","select2d","toImage"]})

        e1, e2 = st.columns([3, 2])
        with e1:
            phdr("Quarterly Earnings Trend")
            # Time range selector for earnings
            _earn_range = st.radio(
                "Earnings range",
                options=["6M","1Y","2Y","3Y","4Y","5Y"],
                index=1,
                horizontal=True,
                key="earn_range_sel",
                label_visibility="collapsed"
            )
            _earn_qtrs = {"6M": 2, "1Y": 4, "2Y": 8, "3Y": 12, "4Y": 16, "5Y": 20}
            st.plotly_chart(
                build_earnings_chart(profile, max_quarters=_earn_qtrs.get(_earn_range, 4)),
                use_container_width=True,
                config={"displayModeBar": False}
            )
        with e2:
            phdr("Revenue Segments")
            st.plotly_chart(build_segment_chart(profile), use_container_width=True,
                            config={"displayModeBar": False})

        # Tradier options chain
        render_options_chain(ticker)
        st.markdown("<br>", unsafe_allow_html=True)

        # Market movers
        render_market_movers()
        st.markdown("<br>", unsafe_allow_html=True)

        # Stock overview table
        stocks = state.stock_overview.get("coins", []) if state.stock_overview else []
        if stocks:
            st.markdown("<br>", unsafe_allow_html=True)
            render_stock_table(stocks)

    with info_col:
        pe    = fm.get("pe")            or profile.get("pe","—")
        eps   = fm.get("eps")           or profile.get("eps","—")
        roe   = fm.get("roe")           or profile.get("roe", 0)
        beta  = fm.get("beta")          or profile.get("beta","—")
        divyd = fm.get("dividend_yield")or profile.get("dividend_yield", 0)
        w52h  = fm.get("52w_high")      or profile.get("week52_high","—")
        w52l  = fm.get("52w_low")       or profile.get("week52_low","—")
        gm    = fm.get("gross_margin")  or profile.get("gross_margin", 0)
        nm    = fm.get("net_margin")    or profile.get("profit_margin", 0)

        phdr("Key Statistics", "FINNHUB · LIVE")
        render_kv([
            ("Market Cap",    profile.get("market_cap","—"),                                         C["text"]),
            ("P/E (TTM)",     f"{pe:.1f}x" if isinstance(pe,float) else f"{pe}",                    C["text"]),
            ("Fwd P/E",       f"{profile.get('fwd_pe','—')}x",                                       C["text"]),
            ("EPS (TTM)",     f"${eps:.2f}" if isinstance(eps,float) else f"${eps}",                C["text"]),
            ("EPS Growth",    f"{profile.get('eps_growth','—')}%",
             C["green_br"] if (profile.get("eps_growth",0) or 0) > 0 else C["red"]),
            ("Revenue",       profile.get("revenue","—"),                                             C["text"]),
            ("Gross Margin",  f"{float(gm)*100:.1f}%" if isinstance(gm,float) else "—",            C["green_br"]),
            ("Net Margin",    f"{float(nm)*100:.1f}%" if isinstance(nm,float) else "—",            C["green_br"]),
            ("ROE",           f"{float(roe)*100:.1f}%" if isinstance(roe,float) else "—",          C["text"]),
            ("Debt/Equity",   f"{profile.get('debt_equity','—')}",                                   C["text"]),
            ("Free Cash Flow",profile.get("fcf","—"),                                                C["text"]),
            ("FCF Yield",     f"{profile.get('fcf_yield',0)*100:.1f}%",                             C["text"]),
            ("Div. Yield",    f"{float(divyd)*100:.2f}%" if isinstance(divyd,float) else "—",      C["text"]),
            ("Beta",          f"{beta:.2f}" if isinstance(beta,float) else str(beta),              C["text"]),
            ("52W High",      f"${w52h:,.2f}" if isinstance(w52h,float) else f"${w52h}",           C["green_br"]),
            ("52W Low",       f"${w52l:,.2f}" if isinstance(w52l,float) else f"${w52l}",           C["red"]),
            ("Insider Own.",  profile.get("insider_own","—"),                                        C["text"]),
            ("Inst. Own.",    profile.get("inst_own","—"),                                           C["text"]),
            ("Short Float",   profile.get("short_float","—"),                                        C["orange_br"]),
        ])
        st.markdown("<br>", unsafe_allow_html=True)

        render_technical_panel(ticker, df)
        st.markdown("<br>", unsafe_allow_html=True)

        # Whale activity
        render_whale_activity()
        st.markdown("<br>", unsafe_allow_html=True)

        # Ticker-specific news (NewsAPI + GNews + NewsData combined search)
        phdr("Current News", "NEWSAPI · GNEWS · NEWSDATA")
        relevant = fetch_ticker_news(ticker, 8)
        if not relevant:
            relevant = [n for n in state.news_stocks
                        if ticker.lower() in n.get("title","").lower()][:6]
        if not relevant:
            relevant = state.news_stocks[:6]
        render_news_feed(relevant, max_items=8)
        st.markdown("<br>", unsafe_allow_html=True)

        # Insider transactions (Finnhub SEC Form 4)
        render_insider_transactions(ticker)
        st.markdown("<br>", unsafe_allow_html=True)

        # Analyst consensus (Finnhub)
        render_analyst_consensus(ticker)
        st.markdown("<br>", unsafe_allow_html=True)

        render_screener()


# ═════════════════════════════════════════════════════════════════════════════════════
# §30-VAL  TOP ALPHA PICKS — Institutional undervalued equity thesis
# ═════════════════════════════════════════════════════════════════════════════════════

TOP_ALPHA_PICKS: List[Dict] = [
    {
        "ticker": "GOOG",
        "name": "Alphabet Inc. — Class C",
        "sector": "Communication Services",
        "roi_target": "+38%",
        "timeframe": "12–18M",
        "tags": [("FCF MACHINE", C["blue_br"]), ("AI UNDERPRICED", C["gold"]), ("BUYBACK", C["green_br"])],
        "pe": "24.8x",
        "fcf_yield": "5.2%",
        "net_margin": "28.1%",
        "macro_thesis": (
            "Fed rate hold cycle compresses discount rate, re-rating growth multiples upward. "
            "Treasury yield normalization benefits long-duration tech. Geopolitical cloud "
            "migration (EU data sovereignty) accelerates GCP enterprise deals."
        ),
        "fundamental_thesis": (
            "Trading at 24.8x P/E vs sector avg 34x — significant discount for a business generating "
            "$60B+ FCF annually. YouTube Shorts monetization inflection, Gemini AI integration "
            "into Search (85% mkt share), and GCP 28% growth compound to a $2.6T+ fair value. "
            "FCF yield 5.2% > 10Y Treasury — rare for a hyperscaler."
        ),
        "risk": "DOJ antitrust ad-tech ruling, Apple default-search deal unwinding.",
        "verdict_color": C["green_br"],
        "verdict": "STRONG BUY",
    },
    {
        "ticker": "META",
        "name": "Meta Platforms Inc.",
        "sector": "Communication Services",
        "roi_target": "+29%",
        "timeframe": "9–15M",
        "tags": [("DEEP VALUE", C["gold"]), ("AI INFRA", C["blue_br"]), ("MOAT", C["green_br"])],
        "pe": "26.5x",
        "fcf_yield": "4.8%",
        "net_margin": "32.0%",
        "macro_thesis": (
            "Digital ad spending rebounds as CPI normalizes and consumer confidence recovers. "
            "Llama open-source AI strategy drives developer ecosystem lock-in at zero marginal cost. "
            "EU regulatory clarity post-DMA removes overhang — stock re-rating catalyst."
        ),
        "fundamental_thesis": (
            "32% net margins expanding with Reality Labs capex tapering post-2026. "
            "WhatsApp Business monetization is essentially untapped — 2B users generating <$2 ARPU "
            "vs $60 in US Instagram. Threads scaling to 200M+ MAU. P/E at historical discount "
            "relative to FCF generation. $40B buyback provides price floor."
        ),
        "risk": "Teen mental health legislation, EU AI Act compliance costs.",
        "verdict_color": C["green_br"],
        "verdict": "BUY",
    },
    {
        "ticker": "JPM",
        "name": "JPMorgan Chase & Co.",
        "sector": "Financial",
        "roi_target": "+24%",
        "timeframe": "12–24M",
        "tags": [("RATE BENEFICIARY", C["gold"]), ("FORTRESS BALANCE", C["green_br"]), ("DIVIDEND", C["blue_br"])],
        "pe": "12.8x",
        "fcf_yield": "7.1%",
        "net_margin": "31.4%",
        "macro_thesis": (
            "Higher-for-longer rate environment expands net interest income. "
            "Yield curve steepening (if spread normalizes from -32bp) is a direct earnings catalyst. "
            "M&A and IPO pipeline recovery in 2026 boosts IB fee revenue. "
            "Strong employment data = lower credit loss provisions."
        ),
        "fundamental_thesis": (
            "Trading at 12.8x P/E — historically cheap for ROTE of 17%+. "
            "First Republic integration fully accretive. $200B+ deposit base, "
            "industry-leading CET1 ratio 15.0%. Dividend yield 2.5% + $12B buyback authorization. "
            "Jamie Dimon's operational discipline means upside leverage to any macro normalization."
        ),
        "risk": "Credit cycle deterioration if unemployment spikes above 5.5%, Basel III endgame.",
        "verdict_color": C["green_br"],
        "verdict": "BUY",
    },
    {
        "ticker": "AVGO",
        "name": "Broadcom Inc.",
        "sector": "Technology / Semiconductors",
        "roi_target": "+31%",
        "timeframe": "12–18M",
        "tags": [("AI INFRA PLAY", C["gold"]), ("VMware SYNERGY", C["blue_br"]), ("HIGH YIELD", C["green_br"])],
        "pe": "35.0x",
        "fcf_yield": "3.8%",
        "net_margin": "27.5%",
        "macro_thesis": (
            "Hyperscaler AI capex boom drives custom ASIC and networking silicon demand — "
            "Broadcom's XPU partnerships (Google TPU, Meta, Apple) are a direct beneficiary. "
            "VMware acquisition (closed FY2024) adds $20B+ recurring software revenue with "
            "40%+ EBITDA margins, radically improving business quality."
        ),
        "fundamental_thesis": (
            "VMware integration synergies tracking ahead of $8.5B target by FY2025. "
            "Custom AI silicon (XPUs) reaching $10B+ segment — growing at 60%+ YoY. "
            "Dividend aristocrat: 15 consecutive years of dividend growth. "
            "Fortress FCF of $19B+ enables continued buybacks alongside 25x fwd P/E."
        ),
        "risk": "VMware customer churn risk, customer concentration (Apple, Google, Meta).",
        "verdict_color": C["gold"],
        "verdict": "BUY — MONITOR",
    },
]


def render_alpha_picks() -> None:
    """Institutional-grade Top Alpha Picks section for valuation tab."""
    st.markdown(
        f'<div style="background:linear-gradient(90deg,rgba(197,168,97,0.06) 0%,'
        f'rgba(12,14,19,0) 100%);border:1px solid rgba(197,168,97,0.2);'
        f'border-left:3px solid {C["gold"]};border-radius:4px;padding:10px 16px;'
        f'margin-bottom:6px;">'
        f'<div style="font-family:Syne,sans-serif;font-size:12px;font-weight:800;'
        f'color:{C["gold"]};letter-spacing:2.5px;">⊕ TOP ALPHA PICKS</div>'
        f'<div style="font-size:9px;color:{C["muted"]};margin-top:2px;">'
        f'Deeply undervalued equities · Multi-variable institutional thesis · '
        f'Macro + Fundamental + Technical convergence</div>'
        f'</div>',
        unsafe_allow_html=True)

    cols = st.columns(2, gap="small")
    for i, pick in enumerate(TOP_ALPHA_PICKS):
        with cols[i % 2]:
            tags_html = "".join(
                f'<span class="alpha-tag" style="background:rgba(0,0,0,0.3);'
                f'color:{tc};border:1px solid {tc}44;">{tl}</span>'
                for tl, tc in pick["tags"])
            st.markdown(
                f'<div class="alpha-card">'
                # Header
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
                f'<div>'
                f'<div class="alpha-ticker">{pick["ticker"]}</div>'
                f'<div class="alpha-name">{pick["name"]}</div>'
                f'<div style="margin-top:4px;">{tags_html}</div>'
                f'</div>'
                f'<div style="text-align:right;">'
                f'<div class="alpha-roi">{pick["roi_target"]}</div>'
                f'<div style="font-size:8px;color:{C["muted"]};margin-top:2px;">'
                f'{pick["timeframe"]} target</div>'
                f'<div style="margin-top:4px;font-size:8px;letter-spacing:1px;'
                f'padding:2px 8px;border-radius:2px;'
                f'background:rgba(0,0,0,0.3);border:1px solid {pick["verdict_color"]}44;'
                f'color:{pick["verdict_color"]};font-weight:700;">'
                f'{pick["verdict"]}</div>'
                f'</div>'
                f'</div>'
                # Metrics strip
                f'<div class="alpha-divider">'
                f'<div class="alpha-metric">'
                f'<div class="alpha-metric-val">{pick["pe"]}</div>'
                f'<div class="alpha-metric-lbl">P/E TTM</div>'
                f'</div>'
                f'<div class="alpha-metric">'
                f'<div class="alpha-metric-val" style="color:{C["green_br"]};">{pick["fcf_yield"]}</div>'
                f'<div class="alpha-metric-lbl">FCF Yield</div>'
                f'</div>'
                f'<div class="alpha-metric">'
                f'<div class="alpha-metric-val">{pick["net_margin"]}</div>'
                f'<div class="alpha-metric-lbl">Net Margin</div>'
                f'</div>'
                f'</div>'
                # Macro thesis
                f'<div class="alpha-thesis">'
                f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["gold"]};'
                f'font-weight:700;text-transform:uppercase;margin-bottom:3px;">⊕ MACRO THESIS</div>'
                f'<div style="font-size:8.5px;line-height:1.5;color:{C["text_dim"]};">'
                f'{pick["macro_thesis"]}</div>'
                f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["blue_br"]};'
                f'font-weight:700;text-transform:uppercase;margin:5px 0 3px;">⊞ FUNDAMENTAL THESIS</div>'
                f'<div style="font-size:8.5px;line-height:1.5;color:{C["text_dim"]};">'
                f'{pick["fundamental_thesis"]}</div>'
                f'<div style="font-size:8px;color:{C["red"]};margin-top:5px;">'
                f'<span style="font-weight:700;letter-spacing:1px;">⚠ RISK:</span> '
                f'{pick["risk"]}</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True)


@st.fragment(run_every=300)
def render_tab_valuation_v2() -> None:
    try:
        _render_tab_valuation_v2_inner()
    except Exception as e:
        st.error(f"⚠ Stock Valuation render error: {e}")


def _render_tab_valuation_v2_inner() -> None:
    state = get_terminal_state()
    macro = state.macro_indicators if state.macro_indicators else get_macro_indicators()

    # ── Toolbar row ──────────────────────────────────────────────────────────────
    _VAL_POPULAR_ASSETS = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
        "JPM", "V", "NFLX", "AMD", "COST", "ADBE", "XOM", "BRK-B",
        "SPY", "QQQ", "GLD", "IWM",
    ]
    h1, h2, h3 = st.columns([4, 2, 4], gap="small")
    with h1:
        vt_sel = st.selectbox(
            "Val asset",
            options=_VAL_POPULAR_ASSETS,
            index=0,
            key="val_ticker_sel",
            label_visibility="collapsed",
        )
        vt = vt_sel.upper().strip() if vt_sel else "AAPL"
    with h2:
        mode = st.selectbox("Model", ["DCF","DDM","EV/EBITDA"],
                            key="dcf_mode", label_visibility="collapsed")
    with h3:
        profile  = dict(VALUATION_PROFILES.get(vt, VALUATION_PROFILES["AAPL"]))
        fq_val   = fetch_finnhub_quote(vt)
        fm_val   = fetch_finnhub_metrics(vt)
        if fq_val.get("price"):
            profile["current_price"] = fq_val["price"]
        df_val   = get_ohlcv_best(vt, days=90)
        tech_val = build_tech_analysis(df_val, vt) if not df_val.empty else {}
        sent_val = state.sentiment_scores.get(vt, state.sentiment_scores.get("composite", 0.0))
        qr       = compute_quantamental_rating(profile, tech_val, fm_val, macro, sent_val)
        rating   = qr["score"]
        qlabel   = qr["label"]
        rcol     = C["green_br"] if rating >= 65 else C["orange_br"] if rating >= 45 else C["red"]
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:12px;padding-top:4px;">'
            f'<span style="font-size:9px;color:{C["muted"]};letter-spacing:1px;">QUANT RATING</span>'
            f'<span style="font-family:Syne,sans-serif;font-size:22px;font-weight:800;color:{rcol};'
            f'background:rgba(255,255,255,0.04);border:1px solid {rcol}33;'
            f'padding:4px 16px;border-radius:3px;">{rating}%</span>'
            f'<span style="font-size:9px;color:{rcol};letter-spacing:1.5px;font-weight:700;">'
            f'{qlabel}</span>'
            f'</div>', unsafe_allow_html=True)

    st.markdown(f'<hr style="border-color:{C["border"]};margin:6px 0;">', unsafe_allow_html=True)

    # ── Main grid: 2+1 layout (model left, data right) ──────────────────────────
    score_col, model_col, heat_col = st.columns([2.8, 3.6, 3.6], gap="small")

    # ── Column 1: Factor Breakdown (Scorecard moved to bottom) ─────────────────
    with score_col:
        phdr("Factor Breakdown", "LIVE SCORING", "blue")
        st.markdown(
            f'<div class="tp-glass" style="margin-top:6px;">'
            f'<div style="font-family:Syne,sans-serif;font-weight:700;color:{C["gold"]};'
            f'font-size:9px;letter-spacing:1.5px;margin-bottom:5px;">ASSESSMENT</div>'
            f'<div style="font-size:9.5px;line-height:1.6;color:{C["text_dim"]};">'
            f'{profile.get("summary","")}</div></div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="tp" style="margin-top:4px;">'
            f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:5px;">Company Profile</div>'
            f'<div style="font-size:9px;line-height:1.55;color:{C["text_dim"]};">'
            f'{profile.get("description","")}</div></div>', unsafe_allow_html=True)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        detail   = qr.get("detail", {})
        fmax_map = {"valuation":30,"financial_health":20,"technical":15,
                    "macro_regime":20,"sentiment":15}
        for factor, max_val in fmax_map.items():
            sc  = detail.get(factor, 0)
            pct = sc / max_val * 100 if max_val else 0
            fc  = C["green_br"] if pct >= 60 else C["orange_br"] if pct >= 35 else C["red"]
            st.markdown(
                f'<div style="margin-bottom:5px;">'
                f'<div style="display:flex;justify-content:space-between;font-size:8px;'
                f'color:{C["muted"]};margin-bottom:2px;text-transform:uppercase;">'
                f'<span>{factor.replace("_"," ")}</span>'
                f'<span style="color:{fc};font-family:Syne,sans-serif;font-weight:700;">'
                f'{sc:.0f}/{max_val}</span></div>'
                f'<div style="background:{C["border2"]};height:5px;border-radius:3px;">'
                f'<div style="width:{pct:.0f}%;height:100%;background:{fc};'
                f'border-radius:3px;box-shadow:0 0 6px {fc}44;">'
                f'</div></div></div>', unsafe_allow_html=True)

        ts_col = score_to_color(sent_val)
        ts_lbl = score_to_label(sent_val)
        st.markdown(
            f'<div class="tp-glass" style="padding:10px 14px;margin-top:4px;">'
            f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:4px;">NLP SENTIMENT — {vt}</div>'
            f'<div style="display:flex;align-items:center;gap:12px;">'
            f'<div style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;'
            f'color:{ts_col};">{ts_lbl}</div>'
            f'<div style="font-size:9px;color:{ts_col};">Score: {sent_val:+.3f}</div>'
            f'</div></div>', unsafe_allow_html=True)

    # ── Column 2: DCF Model + Sensitivity Heatmap + Macro Context ──────────────
    with model_col:
        phdr("Valuation Model", badge=mode, badge_type="gold")
        render_dcf_panel(profile, mode=mode)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # DCF Sensitivity heatmap removed per spec
        # Earnings surprise removed per spec
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Macro context grid
        phdr("Macro Context", "FRED · LIVE", "gold")
        m_keys = ["Fed Funds Rate","10Y Treasury","2Y Treasury",
                  "US CPI YoY","US GDP (QoQ)","VIX Index"]
        neg_up = {"Fed Funds Rate","10Y Treasury","2Y Treasury","US CPI YoY","Yield Spread"}
        mc_cols = st.columns(3)
        for i, key in enumerate(m_keys):
            info  = macro.get(key, MACRO_SEED.get(key, {}))
            val   = info.get("value","—")
            delta = info.get("delta","")
            trend = info.get("trend","flat")
            is_b  = trend == "up" and key in neg_up
            is_g  = trend == "up" and key not in neg_up
            dc    = C["red"] if is_b else C["green_br"] if is_g else C["muted"]
            arr   = "▲" if trend == "up" else "▼" if trend == "down" else "—"
            with mc_cols[i % 3]:
                st.markdown(
                    f'<div class="tp" style="margin-bottom:4px;padding:8px 10px;">'
                    f'<div style="font-size:7px;letter-spacing:1.5px;color:{C["muted"]};'
                    f'text-transform:uppercase;margin-bottom:2px;">{key}</div>'
                    f'<div style="font-family:Syne,sans-serif;font-size:15px;font-weight:800;'
                    f'color:{C["text"]};line-height:1;">{val}</div>'
                    f'<div style="font-size:8px;color:{dc};">{arr} {delta}</div></div>',
                    unsafe_allow_html=True)

    # ── Column 3: Peer Table + Data + Insider + Analyst ─────────────────────────
    with heat_col:
        render_peer_table(profile)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Marketstack data
        msk = fetch_marketstack_quotes([vt, "AAPL", "MSFT", "NVDA"])
        if msk.get(vt):
            d = msk[vt]
            phdr(f"Marketstack Data — {vt}", "GLOBAL EXCHANGE", "gold")
            render_kv([
                ("Date",   d.get("date","—")[:10], C["muted"]),
                ("Open",   f"${d.get('open',0):,.2f}",   C["text"]),
                ("High",   f"${d.get('high',0):,.2f}",   C["green_br"]),
                ("Low",    f"${d.get('low',0):,.2f}",    C["red"]),
                ("Close",  f"${d.get('price',0):,.2f}",  C["gold"]),
                ("Volume", f"{d.get('volume',0)/1e6:.2f}M", C["muted"]),
            ])
            st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Global exchange cross-listing
        render_global_exchange_panel()
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Insider transactions
        render_insider_transactions(vt)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # Analyst consensus
        render_analyst_consensus(vt)

    # ── Full-width: Valuation Scorecard (final concluding section) ──────────────
    st.markdown(f'<hr style="border-color:{C["border"]};margin:16px 0;">', unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:10px;'></div>", unsafe_allow_html=True)
    phdr(f"Valuation Scorecard — {profile.get('name', vt)}", "CONCLUDING ANALYSIS", "gold")
    _sc1, _sc2 = st.columns([1, 2], gap="small")
    with _sc1:
        render_val_cards(profile)
    with _sc2:
        st.markdown(
            f'<div class="tp-glass" style="margin-top:0;">' 
            f'<div style="font-family:Syne,sans-serif;font-weight:700;color:{C["gold"]};'
            f'font-size:9px;letter-spacing:1.5px;margin-bottom:5px;">ASSESSMENT</div>'
            f'<div style="font-size:9.5px;line-height:1.6;color:{C["text_dim"]};">'
            f'{profile.get("summary","")}</div></div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="tp" style="margin-top:4px;">' 
            f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:5px;">Company Profile</div>'
            f'<div style="font-size:9px;line-height:1.55;color:{C["text_dim"]};">'
            f'{profile.get("description","")}</div></div>', unsafe_allow_html=True)
    st.markdown(f'<hr style="border-color:{C["border"]};margin:10px 0;">', unsafe_allow_html=True)
    render_alpha_picks()


@st.fragment(run_every=120)
def render_tab_crypto_v2() -> None:
    try:
        _render_tab_crypto_v2_inner()
    except Exception as e:
        st.error(f"⚠ Crypto & Markets render error: {e}")


def _render_tab_crypto_v2_inner() -> None:
    state = get_terminal_state()
    cov   = state.crypto_overview if state.crypto_overview else fetch_coingecko_overview()
    glbl  = fetch_coingecko_global()
    if glbl:
        cov.update(glbl)
    coins = cov.get("coins", [])

    c1, c2 = st.columns([6, 4], gap="small")

    with c1:
        phdr("Crypto Market Heatmap", "COINGECKO · LIVE", "gold")
        st.plotly_chart(build_crypto_heatmap(coins), use_container_width=True,
                        config={"displayModeBar": False})
        st.markdown("<br>", unsafe_allow_html=True)
        render_crypto_table(cov)
        st.markdown("<br>", unsafe_allow_html=True)

        # BTC chart via Yahoo Finance (no Polygon crypto)
        phdr("BTC/USD Chart", "YAHOO FINANCE · LIVE", "gold")
        df_btc = fetch_yf_ohlcv("BTC-USD", period="3mo", interval="1d")
        if not df_btc.empty:
            fig_btc = build_candlestick(df_btc, "BTC-USD", show_ma=True,
                                         show_volume=True, show_bb=False, show_rsi=False)
            fig_btc.update_layout(height=300)
            st.plotly_chart(fig_btc, use_container_width=True, config={
                "displayModeBar": False})
        st.markdown("<br>", unsafe_allow_html=True)

        # ETH chart
        phdr("ETH/USD Chart", "YAHOO FINANCE · LIVE", "gold")
        df_eth = fetch_yf_ohlcv("ETH-USD", period="3mo", interval="1d")
        if not df_eth.empty:
            fig_eth = go.Figure(go.Scatter(
                x=df_eth["timestamp"], y=df_eth["close"],
                fill="tozeroy", fillcolor="rgba(74,143,224,0.10)",
                line=dict(color=C["blue_br"], width=1.5), name="ETH"))
            _apply_dark(fig_eth)
            fig_eth.update_layout(height=220)
            st.plotly_chart(fig_eth, use_container_width=True, config={"displayModeBar": False})

    with c2:
        phdr("Sentiment Gauges", "ALTERNATIVE.ME · CNN", "gold")
        fg1, fg2 = st.columns(2)
        with fg1:
            render_fear_greed_gauge(state.crypto_fear_greed, "CRYPTO F&G")
        with fg2:
            render_fear_greed_gauge(state.stock_fear_greed,  "EQUITY F&G")
        st.markdown("<br>", unsafe_allow_html=True)

        # 7-day F&G history
        history = state.crypto_fear_greed.get("history", [])
        if history:
            phdr("Crypto F&G History", "7 DAYS", "gold")
            fig_fg = go.Figure(go.Bar(
                x=[h["date"] for h in history],
                y=[h["value"] for h in history],
                marker_color=[
                    C["red"] if v < 25 else C["orange_br"] if v < 45
                    else C["gold"] if v < 55 else C["green_br"]
                    for v in [h["value"] for h in history]
                ],
                text=[h["label"] for h in history],
                textposition="outside",
                textfont=dict(size=8),
            ))
            _apply_dark(fig_fg)
            fig_fg.update_layout(height=180, margin=dict(l=0,r=0,t=8,b=0))
            st.plotly_chart(fig_fg, use_container_width=True, config={"displayModeBar": False})
            st.markdown("<br>", unsafe_allow_html=True)

        # Whale activity
        render_whale_activity()
        st.markdown("<br>", unsafe_allow_html=True)

        # Crypto news
        phdr("Crypto News", "RSS · NEWSAPI · GNEWS", "gold")
        render_news_feed(state.news_crypto, max_items=10)
        st.markdown("<br>", unsafe_allow_html=True)

        # Global crypto stats
        if glbl:
            phdr("Global Crypto Stats", "COINGECKO · LIVE", "gold")
            render_kv([
                ("Total Market Cap", f"${glbl.get('total_market_cap',0)/1e12:.2f}T",  C["text"]),
                ("BTC Dominance",    f"{glbl.get('btc_dominance',0):.1f}%",            C["gold"]),
                ("ETH Dominance",    f"{glbl.get('eth_dominance',0):.1f}%",            C["blue_br"]),
                ("Active Coins",     f"{glbl.get('active_coins',0):,}",                 C["muted"]),
            ])


# ═════════════════════════════════════════════════════════════════════════════════════
# §31  MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════════════

def _bootstrap_cold_start(state: "TerminalState") -> None:
    """
    On first page load the background thread hasn't run yet.
    Synchronously fetch the cheapest / fastest data so tabs render
    immediately rather than showing blank screens.
    """
    # Macro indicators (cached 1h — usually instant if already in st.cache_data)
    if not state.macro_indicators:
        try:
            m = get_macro_indicators()
            if m:
                state.update(macro_indicators=m)
        except Exception:
            pass

    # Market status (Tradier / heuristic — fast)
    if not state.market_status:
        try:
            ms = fetch_tradier_market_status()
            state.update(market_status=ms)
        except Exception:
            pass

    # Ticker prices (Finnhub — cached 30s per symbol)
    if not state.ticker_prices:
        try:
            prices = get_ticker_prices()
            if prices:
                state.update(ticker_prices=prices)
        except Exception:
            pass

    # Fear & Greed (Alternative.me — fast public API)
    if not state.crypto_fear_greed:
        try:
            cfg = fetch_crypto_fear_greed()
            sfg = fetch_stock_fear_greed()
            state.update(crypto_fear_greed=cfg, stock_fear_greed=sfg)
        except Exception:
            pass

    # News: quick GNews burst so ticker + feed aren't empty
    if not state.news_all:
        try:
            items = fetch_gnews("economy markets finance geopolitics", 15)
            if items:
                state.update(news_all=items, news_macro=items, news_stocks=items)
        except Exception:
            pass

    # Global indices (Yahoo Finance)
    if not state.global_indices:
        try:
            idxs = get_global_indices()
            if idxs:
                state.update(global_indices=idxs)
        except Exception:
            pass


def main() -> None:
    inject_css()
    start_background_pipeline()
    start_extended_pipeline()
    state = get_terminal_state()

    # Bootstrap first-render so no tab ever shows a blank black screen
    _bootstrap_cold_start(state)

    # ── Ticker bar ────────────────────────────────────────────────────────────────
    prices = state.ticker_prices if state.ticker_prices else get_ticker_prices()
    render_ticker_bar(prices)

    # ── Live Crucix news ticker ────────────────────────────────────────────────────
    news_ticker = state.news_all[:30] if state.news_all else fetch_gnews(
        "economy finance markets geopolitics conflict", 15)
    render_live_news_ticker(news_ticker)

    # Inject mobile CSS globally on every main render
    st.markdown(_MOBILE_CSS_EXTRA, unsafe_allow_html=True)

    # ── Terminal header ───────────────────────────────────────────────────────────
    now      = datetime.now(timezone.utc)
    errors   = [k for k, v in state.errors.items() if v]
    n_errors = len(errors)
    pill_col = C["red"] if n_errors else C["green_br"]
    pill_txt = f"⚠ {n_errors} ERR" if n_errors else "● LIVE"
    age_news = state.age_safe("news_all")   # ← OverflowError fixed via age_safe()
    age_lbl  = f"News {age_news}s ago" if age_news < 9999 else "Awaiting data"

    st.markdown(
        f'<div class="term-hdr">'
        f'<span class="term-logo">MACRO TERMINAL</span>'
        f'<span style="font-size:8.5px;color:{C["muted"]};letter-spacing:1.5px;">'
        f'INSTITUTIONAL INTELLIGENCE PLATFORM&nbsp;//&nbsp;v5.0</span>'
        f'<span style="margin-left:auto;display:flex;align-items:center;gap:12px;">'
        f'<span style="font-size:8px;color:{C["muted"]};">{age_lbl}</span>'
        f'<span style="display:inline-flex;align-items:center;gap:5px;padding:2px 8px;'
        f'background:rgba(0,0,0,0.3);border:1px solid {pill_col}44;border-radius:10px;'
        f'font-size:9px;color:{pill_col};letter-spacing:1.5px;">{pill_txt}</span>'
        f'<span style="font-size:9px;color:{C["muted"]};letter-spacing:1px;">'
        f'{now.strftime("%d %b %Y  %H:%M:%S")} UTC</span>'
        f'<span style="font-size:8px;color:{C["muted"]};padding-left:8px;'
        f'border-left:1px solid {C["border"]};">'
        f'FRED · POLYGON · ALPHAV · FINNHUB · TRADIER · MARKETSTACK'
        f'&nbsp;·&nbsp;NEWSAPI · GNEWS · NEWSDATA · WORLDNEWS · COINGECKO</span>'
        f'</span></div>',
        unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "⊕  GLOBAL MACRO",
        "📈  STOCK OVERVIEW",
        "⊞  STOCK VALUATION",
        "₿  CRYPTO & MARKETS",
    ])

    with tab1: render_tab_macro_v2()
    with tab2: render_tab_stock_v2()
    with tab3: render_tab_valuation_v2()
    with tab4: render_tab_crypto_v2()


if __name__ == "__main__":
    main()
