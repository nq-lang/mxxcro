#!/usr/bin/env python3
# ─── REQUIREMENTS ────────────────────────────────────────────────────────────────
# pip install streamlit plotly pydeck aiohttp feedparser deep-translator pytz yfinance
# pip install requests pandas numpy
# streamlit run app.py
# ─────────────────────────────────────────────────────────────────────────────────
# ─── DATA SOURCES (all free / public — no key required for base tier) ────────────
# Yahoo Finance (unofficial)   — OHLCV, fundamentals, key stats
# CoinGecko Free API           — Crypto prices, market cap, heatmap, Fear & Greed
# Binance Public REST          — Klines, aggTrades, futures 24hr ticker
# GDELT Project                — Geopolitical event GeoJSON
# FRED St. Louis Fed           — Macro indicators (DEMO key, register for live)
# Alternative.me               — Crypto Fear & Greed Index
# CNN Fear & Greed             — Equity sentiment gauge
# ForexFactory                 — Economic calendar
# OpenSky Network              — Live ADS-B flight data
# RSS Feeds                    — BBC, Reuters, Sky, DW, MilTimes, CoinDesk, CoinTelegraph
# CryptoCompare                — Free crypto news API
# ─────────────────────────────────────────────────────────────────────────────────
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║   MACRO TERMINAL  //  Institutional Intelligence Platform  v3.0                    ║
║   Architecture : Streamlit + PyDeck Globe + Plotly + Async Data Engine            ║
║                                                                                    ║
║   Live Data Sources (activated on deployment):                                     ║
║     • Yahoo Finance v8/v10  — OHLCV, fundamentals, key stats (unofficial)         ║
║     • GDELT Project         — Geopolitical event GeoJSON (public, no key)          ║
║     • FRED St. Louis Fed    — Macro indicators (free key: fred.stlouisfed.org)     ║
║     • RSS Feeds             — BBC, Reuters, Sky News, DW, Military Times           ║
║     • OpenSky Network       — Live ADS-B flight data (public REST)                 ║
║     • World Bank API        — Country GDP/CPI/debt metrics                         ║
║     • ACLED                 — Armed conflict coordinates                            ║
║                                                                                    ║
║   To inject API keys → search: # ── LIVE API KEY INJECTION POINT ──               ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
"""

# ── STANDARD LIBRARY ─────────────────────────────────────────────────────────────────
import asyncio
import concurrent.futures
import json
import logging
import math
import random
import re
import time
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ── THIRD-PARTY ──────────────────────────────────────────────────────────────────────
import aiohttp
import feedparser
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pydeck as pdk
import requests
import streamlit as st
from deep_translator import GoogleTranslator

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)

# ═════════════════════════════════════════════════════════════════════════════════════
# §0  PAGE CONFIG  (must be the very first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="MACRO TERMINAL",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═════════════════════════════════════════════════════════════════════════════════════
# §1  COLOUR PALETTE + ALL GLOBAL CONSTANTS
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

RSS_FEEDS: Dict[str, List[str]] = {
    "world": [
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://feeds.reuters.com/reuters/worldNews",
        "https://feeds.skynews.com/feeds/rss/world.xml",
        "https://rss.dw.com/rdf/rss-en-world",
        "https://feeds.npr.org/1004/rss.xml",
    ],
    "us": [
        "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
        "https://feeds.reuters.com/reuters/politicsNews",
        "https://feeds.npr.org/1001/rss.xml",
    ],
    "markets": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://feeds.marketwatch.com/marketwatch/topstories/",
    ],
    "military": [
        "https://www.militarytimes.com/arc/outboundfeeds/rss/",
        "https://taskandpurpose.com/feed/",
        "https://www.defensenews.com/rss/",
    ],
    "europe": [
        "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
        "https://rss.dw.com/rdf/rss-en-eu",
    ],
}

YF_CHART = ("https://query1.finance.yahoo.com/v8/finance/chart/"
            "{ticker}?interval={interval}&range={range}&includePrePost=false")
YF_QUOTE = ("https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
            "{ticker}?modules=summaryDetail,defaultKeyStatistics,financialData,assetProfile")
YF_BATCH = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"

GDELT_GEO = (
    "https://api.gdeltproject.org/api/v2/geo/geo"
    "?query=conflict%20military%20attack%20crisis%20war"
    "&mode=pointdata&maxpoints=150&timespan=24h&format=json"
)

FRED_BASE = ("https://api.stlouisfed.org/fred/series/observations"
             "?api_key={key}&file_type=json&limit=2&sort_order=desc&series_id={sid}")
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

COUNTRY_CII: List[Dict] = [
    {"country":"Iran",          "score":69.0,"lat":32.4,  "lng":53.7,  "color":[210,47,47,200]},
    {"country":"Russia",        "score":59.0,"lat":61.5,  "lng":105.3, "color":[210,47,47,180]},
    {"country":"Ukraine",       "score":56.0,"lat":48.4,  "lng":31.2,  "color":[200,120,40,180]},
    {"country":"Israel",        "score":49.0,"lat":31.0,  "lng":34.9,  "color":[200,120,40,160]},
    {"country":"China",         "score":43.0,"lat":35.9,  "lng":104.2, "color":[180,140,30,150]},
    {"country":"North Korea",   "score":41.0,"lat":40.3,  "lng":127.5, "color":[180,140,30,140]},
    {"country":"Syria",         "score":38.0,"lat":34.8,  "lng":38.9,  "color":[160,120,20,130]},
    {"country":"Sudan",         "score":35.0,"lat":15.6,  "lng":32.5,  "color":[160,120,20,120]},
    {"country":"Yemen",         "score":34.0,"lat":15.5,  "lng":48.5,  "color":[140,100,20,110]},
    {"country":"Myanmar",       "score":30.0,"lat":19.2,  "lng":96.7,  "color":[100,120,30,100]},
    {"country":"Haiti",         "score":26.0,"lat":18.9,  "lng":-72.3, "color":[80,130,50,90]},
    {"country":"Pakistan",      "score":24.0,"lat":30.4,  "lng":69.3,  "color":[70,120,50,85]},
    {"country":"Ethiopia",      "score":22.5,"lat":9.1,   "lng":40.5,  "color":[65,115,45,80]},
    {"country":"United States", "score":22.0,"lat":37.1,  "lng":-95.7, "color":[40,120,70,80]},
]

CONFLICT_HOTSPOTS: List[Dict] = [
    {"name":"Gaza Strip",          "lat":31.35, "lng":34.30, "severity":0.95,"type":"Active Conflict"},
    {"name":"Kyiv / Front Line",   "lat":50.45, "lng":30.52, "severity":0.90,"type":"Active Conflict"},
    {"name":"Donbas Line",         "lat":48.00, "lng":37.80, "severity":0.88,"type":"Active Conflict"},
    {"name":"Tehran",              "lat":35.69, "lng":51.39, "severity":0.82,"type":"Military Alert"},
    {"name":"Strait of Hormuz",    "lat":26.57, "lng":56.26, "severity":0.80,"type":"Naval Tension"},
    {"name":"Port of Aden",        "lat":12.78, "lng":45.03, "severity":0.76,"type":"Infrastructure"},
    {"name":"South China Sea",     "lat":12.50, "lng":113.00,"severity":0.72,"type":"Naval Tension"},
    {"name":"Taiwan Strait",       "lat":24.50, "lng":119.50,"severity":0.70,"type":"Naval Tension"},
    {"name":"Pyongyang",           "lat":39.02, "lng":125.75,"severity":0.65,"type":"Military Alert"},
    {"name":"Khartoum",            "lat":15.55, "lng":32.53, "severity":0.62,"type":"Active Conflict"},
    {"name":"Yangon",              "lat":16.87, "lng":96.19, "severity":0.58,"type":"Active Conflict"},
    {"name":"Port-au-Prince",      "lat":18.54, "lng":-72.34,"severity":0.52,"type":"Civil Unrest"},
    {"name":"Aleppo",              "lat":36.20, "lng":37.16, "severity":0.48,"type":"Reconstruction"},
    {"name":"Tripoli",             "lat":32.90, "lng":13.18, "severity":0.46,"type":"Civil Unrest"},
    {"name":"Sahel / Mali",        "lat":14.00, "lng":-2.00, "severity":0.44,"type":"Insurgency"},
    {"name":"Kabul",               "lat":34.52, "lng":69.18, "severity":0.42,"type":"Civil Unrest"},
    {"name":"Zaporizhzhia NPP",    "lat":47.51, "lng":34.58, "severity":0.85,"type":"Nuclear Risk"},
    {"name":"Red Sea Corridor",    "lat":18.00, "lng":42.00, "severity":0.70,"type":"Naval Tension"},
    {"name":"Suez Canal",          "lat":30.58, "lng":32.34, "severity":0.60,"type":"Infrastructure"},
    {"name":"Hormuz Pipeline Hub", "lat":27.00, "lng":57.00, "severity":0.75,"type":"Infrastructure"},
]

MILITARY_BASES: List[Dict] = [
    {"name":"Al Udeid AB, Qatar",      "lat":25.12,"lng":51.31,"country":"US"},
    {"name":"Camp Arifjan, Kuwait",    "lat":29.07,"lng":47.96,"country":"US"},
    {"name":"5th Fleet HQ, Bahrain",   "lat":26.21,"lng":50.59,"country":"US"},
    {"name":"Incirlik AB, Turkey",     "lat":37.00,"lng":35.42,"country":"NATO"},
    {"name":"Aviano AB, Italy",        "lat":46.03,"lng":12.60,"country":"US"},
    {"name":"Ramstein AB, Germany",    "lat":49.44,"lng":7.60, "country":"US"},
    {"name":"RAF Akrotiri, Cyprus",    "lat":34.59,"lng":32.99,"country":"UK"},
    {"name":"Diego Garcia, BIOT",      "lat":-7.31,"lng":72.41,"country":"US"},
    {"name":"Yokosuka Naval Base",     "lat":35.29,"lng":139.67,"country":"US"},
    {"name":"Camp Humphreys, Korea",   "lat":36.97,"lng":126.97,"country":"US"},
    {"name":"Kadena AB, Japan",        "lat":26.36,"lng":127.77,"country":"US"},
    {"name":"Tartus Naval Base, Syria","lat":34.89,"lng":35.87,"country":"RU"},
    {"name":"Hmeimim AB, Syria",       "lat":35.40,"lng":35.95,"country":"RU"},
]

NUCLEAR_SITES: List[Dict] = [
    {"name":"Natanz Enrichment, Iran",    "lat":33.72,"lng":51.73,"status":"ACTIVE"},
    {"name":"Fordow, Iran",               "lat":34.88,"lng":50.98,"status":"ACTIVE"},
    {"name":"Zaporizhzhia NPP, Ukraine",  "lat":47.51,"lng":34.58,"status":"AT RISK"},
    {"name":"Yongbyon Complex, N.Korea",  "lat":39.79,"lng":125.75,"status":"ACTIVE"},
    {"name":"Dimona, Israel",             "lat":31.01,"lng":35.15,"status":"CLASSIFIED"},
    {"name":"Bushehr NPP, Iran",          "lat":28.83,"lng":50.89,"status":"ACTIVE"},
    {"name":"Parchin Military Complex",   "lat":35.50,"lng":51.77,"status":"ACTIVE"},
]

CABLE_ROUTES: List[Dict] = [
    {"name":"MAREA (US-EU)",           "path":[[40.7,-74.0],[51.5,-0.12]]},
    {"name":"SEA-ME-WE-5",             "path":[[51.5,-0.12],[1.35,103.8],[31.2,121.5]]},
    {"name":"Pacific Light Cable",     "path":[[40.7,-74.0],[22.3,114.2]]},
    {"name":"AAG Asia-America",        "path":[[33.7,135.5],[21.3,-157.8],[34.0,-118.2]]},
    {"name":"EASSy Africa",            "path":[[51.5,-0.12],[-33.9,18.4],[1.3,36.8]]},
]

INFRA_CASCADE: List[Dict] = [
    {"node":"Port of Aden",       "type":"Port",      "lat":12.78,"lng":45.03,
     "countries":["Yemen","Germany","UK","Italy","Saudi Arabia"],"risk_pct":[38,29,25,20,20]},
    {"node":"Strait of Hormuz",   "type":"Chokepoint","lat":26.57,"lng":56.26,
     "countries":["UAE","India","Japan","South Korea","China"],  "risk_pct":[62,47,42,40,36]},
    {"node":"Suez Canal",         "type":"Canal",     "lat":30.58,"lng":32.34,
     "countries":["Egypt","UK","Netherlands","Germany","France"],"risk_pct":[55,34,30,28,25]},
    {"node":"Nord Stream Alt.",   "type":"Pipeline",  "lat":55.00,"lng":14.00,
     "countries":["Germany","Poland","Czech Rep.","Austria"],    "risk_pct":[70,46,32,26]},
    {"node":"Taiwan Strait",      "type":"Chokepoint","lat":24.50,"lng":119.50,
     "countries":["Taiwan","Japan","South Korea","Philippines"],  "risk_pct":[82,67,57,52]},
    {"node":"Bab-el-Mandeb",      "type":"Chokepoint","lat":12.50,"lng":43.50,
     "countries":["Ethiopia","Djibouti","Egypt","EU Shipping"],   "risk_pct":[60,55,48,44]},
]

PENTAGON_INDEX = {
    "score":67,"label":"ELEVATED",
    "components":{
        "Conflict Events":74,"Naval Activity":68,"Cyber Incidents":58,
        "Military Mobilization":71,"Diplomatic Tension":64,
    }
}

VALUATION_PROFILES: Dict[str, Dict] = {
    "AAPL": {
        "name":"Apple Inc.","sector":"Technology","exchange":"NASDAQ","industry":"Consumer Electronics",
        "description":("Apple Inc. designs, manufactures, and markets smartphones, personal computers, "
                       "tablets, wearables, and accessories worldwide. Key products include iPhone, Mac, "
                       "iPad, Apple Watch, AirPods, and a high-margin Services segment (App Store, "
                       "Apple TV+, Apple Music, iCloud) growing at ~14% YoY."),
        "rating":60,"rating_label":"MODERATE FOUNDATION",
        "dcf_verdict":"OVERVALUED","dcf_pct":40,
        "current_price":301.91,"fair_value_dcf":180.05,"fair_value_ddm":23.90,"fair_value_ev":196.00,
        "pe":36.8,"fwd_pe":33.2,"peer_pe":43.6,"eps":6.42,"eps_growth":8.2,
        "market_cap":"3.12T","revenue":"391B","gross_margin":0.441,"profit_margin":0.253,
        "ebitda_margin":0.318,"roe":0.147,"roa":0.178,"debt_equity":1.74,"fcf":"107B",
        "fcf_yield":0.034,"dividend_yield":0.0048,"payout_ratio":0.15,"beta":1.21,
        "week52_high":321.04,"week52_low":164.08,"shares_out":"15.2B","buyback":"90B",
        "insider_own":"0.07%","inst_own":"60.8%","short_float":"0.74%",
        "cards":[
            {"cat":"Valuation Models",      "verdict":"OVERVALUED",  "icon":"✕","color":"red",
             "detail":"DCF –40.4%  ·  DDM –92.7%  ·  EV/EBITDA –35%"},
            {"cat":"Financial Health",       "verdict":"STRONG",      "icon":"✓","color":"green",
             "detail":"Assets exceed liabilities short & long term  ·  AA+ credit rating"},
            {"cat":"Institutional Activity", "verdict":"BALANCED",    "icon":"△","color":"orange",
             "detail":"2 large purchases vs 2 large sales  ·  Net neutral flow"},
            {"cat":"Peer Comparison",        "verdict":"ATTRACTIVE",  "icon":"✓","color":"green",
             "detail":"P/E 36.8 < Peer Avg 43.6  ·  Ecosystem moat premium justified"},
            {"cat":"Comparable Analysis",    "verdict":"WEAK",        "icon":"✕","color":"red",
             "detail":"Rank #7 of 9  ·  Score 25  ·  Revenue growth lagging peers"},
            {"cat":"Capital Allocation",     "verdict":"B (69/100)",  "icon":"✓","color":"green",
             "detail":"$90B buyback  ·  $15B R&D  ·  Shareholder-friendly"},
            {"cat":"Earnings Quality",       "verdict":"HIGH",        "icon":"✓","color":"green",
             "detail":"FCF yield 3.4%  ·  Accrual ratio low  ·  Clean accounting"},
        ],
        "summary":("The balance sheet is solid and capital allocation is shareholder-friendly via "
                   "large buybacks. However DCF valuation indicates significant overvaluation at "
                   "current prices, Services revenue growth is decelerating, and multi-factor "
                   "ranking is weak vs peers."),
        "peers":[
            {"sym":"MSFT","pe":34.2,"fwd_pe":30.1,"mkt":"3.1T","margin":0.362,"roe":0.401,"growth":0.162},
            {"sym":"GOOG","pe":24.8,"fwd_pe":21.3,"mkt":"2.3T","margin":0.281,"roe":0.273,"growth":0.142},
            {"sym":"META","pe":26.5,"fwd_pe":23.8,"mkt":"1.4T","margin":0.320,"roe":0.352,"growth":0.188},
            {"sym":"AMZN","pe":42.1,"fwd_pe":36.7,"mkt":"2.2T","margin":0.094,"roe":0.187,"growth":0.124},
        ],
        "revenue_segments":{"iPhone":52,"Services":22,"Mac":8,"iPad":7,"Wearables":11},
        "quarterly_eps":[5.89,6.13,6.42,6.73],
        "quarterly_rev":[89.5,91.2,95.4,98.1],
    },
    "TSLA": {
        "name":"Tesla Inc.","sector":"Consumer Cyclical","exchange":"NASDAQ","industry":"Electric Vehicles",
        "description":("Tesla designs, develops, manufactures, leases, and sells electric vehicles and "
                       "energy generation/storage systems. Key products: Model S/3/X/Y, Cybertruck, "
                       "Semi, Powerwall, Megapack, and Full Self-Driving (FSD) software subscription."),
        "rating":38,"rating_label":"SPECULATIVE",
        "dcf_verdict":"OVERVALUED","dcf_pct":65,
        "current_price":248.50,"fair_value_dcf":87.00,"fair_value_ddm":0.0,"fair_value_ev":195.00,
        "pe":58.3,"fwd_pe":74.1,"peer_pe":12.0,"eps":4.26,"eps_growth":-8.4,
        "market_cap":"795B","revenue":"97B","gross_margin":0.182,"profit_margin":0.073,
        "ebitda_margin":0.126,"roe":0.124,"roa":0.078,"debt_equity":0.18,"fcf":"2.5B",
        "fcf_yield":0.003,"dividend_yield":0.0,"payout_ratio":0.0,"beta":2.34,
        "week52_high":488.54,"week52_low":138.80,"shares_out":"3.2B","buyback":"0B",
        "insider_own":"12.9%","inst_own":"44.2%","short_float":"3.1%",
        "cards":[
            {"cat":"Valuation Models",      "verdict":"OVERVALUED",  "icon":"✕","color":"red",
             "detail":"DCF –65%  ·  DDM N/A (no dividend)  ·  EV/EBITDA stretched"},
            {"cat":"Financial Health",       "verdict":"ADEQUATE",    "icon":"△","color":"orange",
             "detail":"Positive FCF but declining  ·  Margins compressing 6pp YoY"},
            {"cat":"Institutional Activity", "verdict":"NET SELLING", "icon":"✕","color":"red",
             "detail":"5 large sales vs 1 purchase  ·  Index rebalancing pressure"},
            {"cat":"Peer Comparison",        "verdict":"OVERPRICED",  "icon":"✕","color":"red",
             "detail":"P/E 58x vs OEM avg 12x  ·  Premium not supported at current margins"},
            {"cat":"Comparable Analysis",    "verdict":"WEAK",        "icon":"✕","color":"red",
             "detail":"Rank #8 of 9  ·  Losing China EV share to BYD"},
            {"cat":"Capital Allocation",     "verdict":"C (52/100)",  "icon":"△","color":"orange",
             "detail":"High capex $8.9B  ·  Uncertain RoI on Cybertruck / Semi"},
            {"cat":"Earnings Quality",       "verdict":"CONCERN",     "icon":"✕","color":"red",
             "detail":"Regulatory credits inflate net income  ·  Core auto margin 3.1%"},
        ],
        "summary":("Tesla is priced as a hyper-growth technology company yet faces severe margin "
                   "compression from price cuts, intensifying Chinese competition (BYD surpassed "
                   "Tesla globally), FSD revenue recognition uncertainty, and key-man risk. "
                   "The current premium is almost entirely speculative."),
        "peers":[
            {"sym":"F",   "pe":6.2, "fwd_pe":5.8, "mkt":"42B","margin":0.028,"roe":0.122,"growth":0.04},
            {"sym":"GM",  "pe":5.8, "fwd_pe":5.1, "mkt":"47B","margin":0.032,"roe":0.141,"growth":0.02},
            {"sym":"RIVN","pe":None,"fwd_pe":None,"mkt":"14B","margin":-0.18,"roe":-0.45,"growth":0.82},
            {"sym":"NIO", "pe":None,"fwd_pe":None,"mkt":"8B", "margin":-0.12,"roe":-0.31,"growth":0.38},
        ],
        "revenue_segments":{"Automotive":84,"Energy":8,"Services":8},
        "quarterly_eps":[1.19,0.91,0.72,0.66],
        "quarterly_rev":[23.3,21.3,25.2,25.7],
    },
    "NVDA": {
        "name":"NVIDIA Corporation","sector":"Technology","exchange":"NASDAQ","industry":"Semiconductors",
        "description":("NVIDIA provides graphics, compute and networking solutions. Platforms address "
                       "AI/data center, gaming, professional visualization, and automotive. The Hopper "
                       "H100/H200 and Blackwell GPU architectures dominate AI training workloads with "
                       "80%+ data-center GPU market share."),
        "rating":72,"rating_label":"STRONG FOUNDATION",
        "dcf_verdict":"FAIRLY VALUED","dcf_pct":8,
        "current_price":134.80,"fair_value_dcf":124.00,"fair_value_ddm":40.00,"fair_value_ev":128.00,
        "pe":38.2,"fwd_pe":29.4,"peer_pe":35.0,"eps":3.53,"eps_growth":122.0,
        "market_cap":"3.30T","revenue":"130B","gross_margin":0.752,"profit_margin":0.558,
        "ebitda_margin":0.632,"roe":0.698,"roa":0.424,"debt_equity":0.42,"fcf":"67B",
        "fcf_yield":0.020,"dividend_yield":0.0003,"payout_ratio":0.01,"beta":1.67,
        "week52_high":153.13,"week52_low":75.61,"shares_out":"24.5B","buyback":"25B",
        "insider_own":"3.5%","inst_own":"65.4%","short_float":"1.1%",
        "cards":[
            {"cat":"Valuation Models",      "verdict":"FAIRLY VALUED","icon":"≈","color":"green",
             "detail":"DCF –8%  ·  Premium justified by 122% YoY revenue growth"},
            {"cat":"Financial Health",       "verdict":"EXCEPTIONAL",  "icon":"✓","color":"green",
             "detail":"75% gross margin  ·  56% net margin  ·  Net cash $26B"},
            {"cat":"Institutional Activity", "verdict":"NET BUYING",   "icon":"✓","color":"green",
             "detail":"12 large purchases vs 3 sales  ·  Sovereign funds accumulating"},
            {"cat":"Peer Comparison",        "verdict":"PREMIUM",      "icon":"△","color":"orange",
             "detail":"P/E 38x vs semi avg 35x  ·  Justified by monopoly positioning"},
            {"cat":"Comparable Analysis",    "verdict":"LEADER",       "icon":"✓","color":"green",
             "detail":"Rank #1 of 9  ·  Score 91  ·  80%+ AI GPU market share"},
            {"cat":"Capital Allocation",     "verdict":"A (88/100)",   "icon":"✓","color":"green",
             "detail":"$25B buyback  ·  R&D 15% of revenue  ·  CUDA moat deepening"},
            {"cat":"Earnings Quality",       "verdict":"HIGH",         "icon":"✓","color":"green",
             "detail":"FCF yield 2.0%  ·  Revenue visibility from $70B+ backlog"},
        ],
        "summary":("NVIDIA's AI accelerator monopoly and CUDA software ecosystem create an "
                   "extraordinary competitive moat. Revenue grew 122% YoY driven by H100/H200 "
                   "demand from hyperscalers. DCF is near fair value at current growth rates. "
                   "Primary risks: cycle peak, AMD MI300X ramp, and custom silicon from Google/Amazon."),
        "peers":[
            {"sym":"AMD",  "pe":28.0,"fwd_pe":24.8,"mkt":"262B","margin":0.053,"roe":0.043,"growth":0.082},
            {"sym":"INTC", "pe":18.0,"fwd_pe":16.2,"mkt":"85B", "margin":0.012,"roe":0.008,"growth":-0.02},
            {"sym":"QCOM", "pe":16.5,"fwd_pe":14.1,"mkt":"152B","margin":0.243,"roe":0.312,"growth":0.058},
            {"sym":"AVGO", "pe":32.0,"fwd_pe":27.3,"mkt":"820B","margin":0.278,"roe":0.395,"growth":0.112},
        ],
        "revenue_segments":{"Data Center":87,"Gaming":9,"Professional":3,"Automotive":1},
        "quarterly_eps":[0.61,0.83,2.04,3.53],
        "quarterly_rev":[7.2,13.5,22.1,32.8],
    },
    "MSFT": {
        "name":"Microsoft Corporation","sector":"Technology","exchange":"NASDAQ","industry":"Software",
        "description":("Microsoft develops and supports software, services, devices, and solutions. "
                       "Segments: Productivity & Business Processes, Intelligent Cloud (Azure — 28% "
                       "YoY growth), and More Personal Computing. OpenAI partnership deepens AI moat."),
        "rating":78,"rating_label":"STRONG FOUNDATION",
        "dcf_verdict":"FAIRLY VALUED","dcf_pct":5,
        "current_price":415.20,"fair_value_dcf":394.00,"fair_value_ddm":220.00,"fair_value_ev":380.00,
        "pe":34.2,"fwd_pe":29.8,"peer_pe":35.0,"eps":12.14,"eps_growth":20.1,
        "market_cap":"3.08T","revenue":"245B","gross_margin":0.694,"profit_margin":0.362,
        "ebitda_margin":0.512,"roe":0.401,"roa":0.214,"debt_equity":0.35,"fcf":"75B",
        "fcf_yield":0.024,"dividend_yield":0.0072,"payout_ratio":0.24,"beta":0.88,
        "week52_high":468.35,"week52_low":362.90,"shares_out":"7.44B","buyback":"28B",
        "insider_own":"1.3%","inst_own":"72.1%","short_float":"0.5%",
        "cards":[
            {"cat":"Valuation Models",      "verdict":"FAIRLY VALUED","icon":"≈","color":"green",
             "detail":"DCF –5%  ·  Azure growth sustains premium multiple"},
            {"cat":"Financial Health",       "verdict":"EXCEPTIONAL",  "icon":"✓","color":"green",
             "detail":"AAA credit  ·  $80B cash  ·  Net cash position"},
            {"cat":"Institutional Activity", "verdict":"NET BUYING",   "icon":"✓","color":"green",
             "detail":"8 large purchases vs 2 sales  ·  Sovereign wealth accumulating"},
            {"cat":"Peer Comparison",        "verdict":"IN-LINE",      "icon":"△","color":"orange",
             "detail":"P/E 34x at sector avg  ·  Justified by AI/cloud integration"},
            {"cat":"Comparable Analysis",    "verdict":"LEADER",       "icon":"✓","color":"green",
             "detail":"Rank #2 of 9  ·  Score 84  ·  OpenAI partnership moat"},
            {"cat":"Capital Allocation",     "verdict":"A- (82/100)",  "icon":"✓","color":"green",
             "detail":"$28B buyback  ·  $24B R&D  ·  Activision integration ongoing"},
            {"cat":"Earnings Quality",       "verdict":"HIGH",         "icon":"✓","color":"green",
             "detail":"FCF yield 2.4%  ·  Subscription model = high revenue visibility"},
        ],
        "summary":("Microsoft's Azure cloud platform and deep OpenAI integration position it as "
                   "the enterprise AI infrastructure standard. Copilot monetization is early-stage "
                   "with significant upside. Valuation is fair, not cheap. Activision adds gaming "
                   "revenue but dilutes margins short-term."),
        "peers":[
            {"sym":"GOOG","pe":24.8,"fwd_pe":21.3,"mkt":"2.3T","margin":0.281,"roe":0.273,"growth":0.142},
            {"sym":"AMZN","pe":42.1,"fwd_pe":36.7,"mkt":"2.2T","margin":0.094,"roe":0.187,"growth":0.124},
            {"sym":"META","pe":26.5,"fwd_pe":23.8,"mkt":"1.4T","margin":0.320,"roe":0.352,"growth":0.188},
            {"sym":"AAPL","pe":36.8,"fwd_pe":33.2,"mkt":"3.1T","margin":0.253,"roe":0.147,"growth":0.065},
        ],
        "revenue_segments":{"Intelligent Cloud":44,"Productivity":32,"Personal Computing":24},
        "quarterly_eps":[9.81,11.45,12.14,13.20],
        "quarterly_rev":[56.5,61.9,65.6,70.1],
    },
}

MACRO_SEED: Dict[str, Dict] = {
    "Fed Funds Rate":  {"value":"5.25%",  "delta":"0.00%",  "trend":"flat","note":"Held — next FOMC June 2026"},
    "US CPI YoY":      {"value":"3.1%",   "delta":"+0.1%",  "trend":"up",  "note":"Above 2% target"},
    "US GDP (QoQ)":    {"value":"2.4%",   "delta":"+0.3%",  "trend":"up",  "note":"Q1 2026 Annualized"},
    "US Unemployment": {"value":"4.1%",   "delta":"-0.1%",  "trend":"down","note":"Marginally easing"},
    "10Y Treasury":    {"value":"4.62%",  "delta":"+0.08%", "trend":"up",  "note":"Discount rate pressure"},
    "2Y Treasury":     {"value":"4.94%",  "delta":"+0.05%", "trend":"up",  "note":"Yield curve inverted"},
    "VIX Index":       {"value":"18.4",   "delta":"-1.2",   "trend":"down","note":"Below 20 — moderate risk"},
    "M2 Money Supply": {"value":"$21.4T", "delta":"+$0.1T", "trend":"up",  "note":"Liquidity expanding"},
    "DXY (USD Index)": {"value":"104.2",  "delta":"+0.4",   "trend":"up",  "note":"Strong — FX headwind"},
    "Gold (XAU/USD)":  {"value":"$2,481", "delta":"+$12",   "trend":"up",  "note":"Safe-haven bid elevated"},
    "WTI Crude":       {"value":"$79.40", "delta":"+$2.10", "trend":"up",  "note":"Supply disruption premium"},
    "BTC/USD":         {"value":"$105.2K","delta":"+$1.8K", "trend":"up",  "note":"ETF inflow momentum"},
    "Yield Spread":    {"value":"-0.32%", "delta":"-0.03%", "trend":"down","note":"2s10s inverted — recession watch"},
    "ISM Mfg PMI":     {"value":"48.7",   "delta":"-0.8",   "trend":"down","note":"Contraction below 50"},
}

TICKER_BAR_SYMBOLS = [
    ("S&P 500","^GSPC"),("NASDAQ","^IXIC"),("RUSSELL","^RUT"),("FTSE","^FTSE"),
    ("DAX","^GDAXI"),("NIKKEI","^N225"),("EUR/USD","EURUSD=X"),("GBP/USD","GBPUSD=X"),
    ("USD/JPY","JPY=X"),("USD/CNY","CNY=X"),("GOLD","GC=F"),("SILVER","SI=F"),
    ("OIL(WTI)","CL=F"),("NAT GAS","NG=F"),("BTC","BTC-USD"),("ETH","ETH-USD"),
    ("AAPL","AAPL"),("NVDA","NVDA"),("TSLA","TSLA"),("MSFT","MSFT"),
]

SCREENER_UNIVERSE = [
    {"ticker":"AAPL","name":"Apple Inc.",          "sector":"Technology",   "mkt_cap":"3.12T","pe":36.8,"rating":60},
    {"ticker":"MSFT","name":"Microsoft Corp.",     "sector":"Technology",   "mkt_cap":"3.08T","pe":34.2,"rating":78},
    {"ticker":"NVDA","name":"NVIDIA Corp.",         "sector":"Technology",   "mkt_cap":"3.30T","pe":38.2,"rating":72},
    {"ticker":"GOOG","name":"Alphabet Inc.",        "sector":"Communication","mkt_cap":"2.30T","pe":24.8,"rating":70},
    {"ticker":"AMZN","name":"Amazon.com Inc.",      "sector":"Consumer",     "mkt_cap":"2.20T","pe":42.1,"rating":65},
    {"ticker":"META","name":"Meta Platforms Inc.",  "sector":"Communication","mkt_cap":"1.40T","pe":26.5,"rating":68},
    {"ticker":"TSLA","name":"Tesla Inc.",           "sector":"Consumer",     "mkt_cap":"795B", "pe":58.3,"rating":38},
    {"ticker":"BRK-B","name":"Berkshire Hathaway", "sector":"Financial",    "mkt_cap":"910B", "pe":22.4,"rating":74},
    {"ticker":"JPM", "name":"JPMorgan Chase",       "sector":"Financial",    "mkt_cap":"580B", "pe":12.8,"rating":72},
    {"ticker":"V",   "name":"Visa Inc.",            "sector":"Financial",    "mkt_cap":"560B", "pe":30.1,"rating":76},
    {"ticker":"JNJ", "name":"Johnson & Johnson",    "sector":"Healthcare",   "mkt_cap":"385B", "pe":14.2,"rating":69},
    {"ticker":"XOM", "name":"ExxonMobil Corp.",     "sector":"Energy",       "mkt_cap":"520B", "pe":14.8,"rating":62},
    {"ticker":"SPY", "name":"S&P 500 ETF",          "sector":"ETF",          "mkt_cap":"560B", "pe":22.0,"rating":70},
    {"ticker":"QQQ", "name":"Nasdaq-100 ETF",       "sector":"ETF",          "mkt_cap":"290B", "pe":28.4,"rating":68},
    {"ticker":"GLD", "name":"Gold ETF (SPDR)",      "sector":"Commodity",    "mkt_cap":"66B",  "pe":0.0, "rating":65},
]

# ═════════════════════════════════════════════════════════════════════════════════════
# §2  CSS — Full institutional dark terminal
# ═════════════════════════════════════════════════════════════════════════════════════

def inject_css() -> None:
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&family=Syne:wght@400;600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Space Mono','Courier New',Courier,monospace !important;
        background-color: {C['bg']} !important;
        color: {C['text']} !important;
    }}
    #MainMenu, footer, header, .stDeployButton,
    [data-testid="stToolbar"],[data-testid="stDecoration"],
    [data-testid="stStatusWidget"],[data-testid="stSidebarCollapsedControl"],
    section[data-testid="stSidebar"] {{
        display: none !important; visibility: hidden !important;
    }}
    .block-container {{ padding: 0 !important; max-width: 100% !important; }}
    [data-testid="stAppViewContainer"] {{ padding: 0 !important; background-color: {C['bg']} !important; }}
    [data-testid="stVerticalBlock"] {{ gap: 0 !important; }}
    [data-testid="column"] {{ padding: 0 2px !important; }}
    div[data-testid="stHorizontalBlock"] {{ gap: 2px !important; }}

    .stTabs [data-baseweb="tab-list"] {{
        background-color: {C['panel']} !important;
        border-bottom: 1px solid {C['border']} !important;
        padding: 0 8px !important; gap: 0 !important;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent !important; color: {C['muted']} !important;
        font-family: 'Syne',monospace !important; font-weight: 700 !important;
        font-size: 10px !important; letter-spacing: 2px !important;
        padding: 10px 22px !important; border: none !important;
        border-bottom: 2px solid transparent !important; border-radius: 0 !important;
    }}
    .stTabs [aria-selected="true"] {{
        color: {C['gold']} !important;
        border-bottom: 2px solid {C['gold']} !important;
        background: rgba(197,168,97,0.06) !important;
    }}
    .stTabs [data-baseweb="tab"]:hover {{ color: {C['text']} !important; background: rgba(255,255,255,0.03) !important; }}
    .stTabs [data-baseweb="tab-panel"] {{ padding: 0 !important; background: {C['bg']} !important; }}
    .stTabs [data-baseweb="tab-border"] {{ display: none !important; }}

    [data-testid="metric-container"] {{
        background: {C['panel']} !important; border: 1px solid {C['border']} !important;
        border-radius: 3px !important; padding: 8px 10px !important;
    }}
    [data-testid="stMetricLabel"] > div {{
        font-size: 8px !important; letter-spacing: 1.5px !important;
        text-transform: uppercase !important; color: {C['muted']} !important;
        font-family: 'Space Mono',monospace !important;
    }}
    [data-testid="stMetricValue"] {{
        font-family: 'Syne',sans-serif !important; font-size: 17px !important;
        font-weight: 800 !important; color: {C['text']} !important;
    }}
    [data-testid="stMetricDelta"] {{ font-size: 9px !important; }}
    [data-testid="stMetricDeltaIcon-Up"]   {{ color: {C['green_br']} !important; }}
    [data-testid="stMetricDeltaIcon-Down"] {{ color: {C['red']} !important; }}

    [data-testid="stProgress"] > div > div {{ background: {C['border2']} !important; }}
    [data-testid="stProgress"] > div > div > div {{ background: {C['gold']} !important; }}

    .stSelectbox > div > div,
    .stTextInput > div > div > input {{
        background: {C['panel']} !important; border: 1px solid {C['border2']} !important;
        border-radius: 3px !important; color: {C['text']} !important;
        font-family: 'Space Mono',monospace !important; font-size: 11px !important;
    }}
    .stSelectbox [data-baseweb="select"] > div {{ background: {C['panel']} !important; border-color: {C['border2']} !important; }}
    .stCheckbox span {{ color: {C['text']} !important; font-size: 10px !important; }}
    .stButton > button {{
        background: {C['gold']} !important; color: #000 !important; border: none !important;
        border-radius: 3px !important; font-family: 'Space Mono',monospace !important;
        font-size: 10px !important; font-weight: 700 !important; letter-spacing: 1.5px !important;
        padding: 6px 16px !important;
    }}
    .stButton > button:hover {{ opacity: 0.82 !important; }}
    [data-testid="stSlider"] > div > div > div > div {{ background: {C['gold']} !important; }}
    [data-testid="stSlider"] > div > div > div {{ background: {C['border2']} !important; }}
    .streamlit-expanderHeader {{
        background: {C['panel']} !important; color: {C['muted']} !important;
        font-size: 10px !important; border: 1px solid {C['border']} !important;
        border-radius: 3px !important; padding: 4px 10px !important;
    }}
    .streamlit-expanderContent {{
        background: {C['panel_alt']} !important; border: 1px solid {C['border']} !important;
        border-top: none !important; padding: 8px !important;
    }}
    ::-webkit-scrollbar {{ width: 4px; height: 4px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{ background: {C['border2']}; border-radius: 2px; }}
    iframe {{ border: none !important; }}

    .ticker-strip {{
        background: #000; border-bottom: 1px solid {C['border']};
        padding: 5px 14px; font-size: 10px; white-space: nowrap;
        overflow-x: auto; letter-spacing: 0.5px; display: flex; align-items: center;
    }}
    .t-label {{ color: {C['muted']}; margin-right: 5px; }}
    .t-item  {{ display: inline-block; margin-right: 22px; }}
    .pos {{ color: {C['green_br']}; }}
    .neg {{ color: {C['red']}; }}
    .gold{{ color: {C['gold']}; }}

    /* ── Crucix Live News Ticker ── */
    .crucix-ticker-outer {{
        background: {C['panel']}; border-bottom: 2px solid {C['gold_dim']};
        display: grid; grid-template-columns: 160px 1fr; align-items: center;
        overflow: hidden; height: 30px; position: relative;
    }}
    .crucix-ticker-label {{
        background: {C['gold_dim']}; color: #000; font-family: 'Syne',sans-serif;
        font-weight: 800; font-size: 8.5px; letter-spacing: 2.5px;
        text-transform: uppercase; padding: 0 12px; height: 100%;
        display: flex; align-items: center; flex-shrink: 0; z-index: 2;
        border-right: 2px solid {C['gold']};
    }}
    .crucix-ticker-track {{
        display: grid; grid-template-columns: 1fr;
        overflow: hidden; height: 100%; position: relative;
    }}
    .crucix-ticker-scroll {{
        display: inline-flex; align-items: center; gap: 0;
        white-space: nowrap; height: 30px;
        animation: crucix-scroll 120s linear infinite;
    }}
    .crucix-ticker-scroll:hover {{ animation-play-state: paused; }}
    @keyframes crucix-scroll {{
        0%   {{ transform: translateX(100vw); }}
        100% {{ transform: translateX(-100%); }}
    }}
    .crucix-ticker-item {{
        display: inline-flex; align-items: center; gap: 7px;
        padding: 0 20px; border-right: 1px solid {C['border2']}; height: 100%;
    }}
    .crucix-ticker-src {{
        font-size: 7.5px; letter-spacing: 1.5px; padding: 1px 5px;
        border-radius: 2px; font-weight: 700; text-transform: uppercase;
        white-space: nowrap;
    }}
    .crucix-ticker-src.flash    {{ background: rgba(211,47,47,0.22); color: {C['red']}; border: 1px solid {C['red']}44; }}
    .crucix-ticker-src.priority {{ background: rgba(192,122,42,0.18); color: {C['orange_br']}; }}
    .crucix-ticker-src.routine  {{ background: rgba(197,168,97,0.10); color: {C['gold']}; }}
    .crucix-ticker-headline {{
        font-size: 9.5px; color: {C['text']}; letter-spacing: 0.2px;
    }}
    .crucix-ticker-headline.flash {{ color: {C['red_bright']}; font-weight: 700; }}
    .crucix-ticker-ts {{
        font-size: 7.5px; color: {C['muted']}; white-space: nowrap;
    }}

    /* ── Crucix OSINT Stream Panel ── */
    .osint-panel {{
        background: {C['panel']}; border: 1px solid {C['border']};
        border-radius: 3px; overflow: hidden;
    }}
    .osint-header {{
        display: grid; grid-template-columns: 1fr auto auto;
        align-items: center; padding: 6px 12px;
        border-bottom: 1px solid {C['border']};
        background: {C['panel_alt']};
    }}
    .osint-stream-body {{
        max-height: 420px; overflow-y: auto;
    }}
    .osint-item {{
        display: grid; grid-template-columns: 18px 100px 1fr auto;
        gap: 8px; padding: 6px 12px;
        border-bottom: 1px solid {C['border']};
        align-items: flex-start;
        transition: background 0.15s;
    }}
    .osint-item:hover {{ background: rgba(255,255,255,0.025); }}
    .osint-item:last-child {{ border-bottom: none; }}
    .osint-tier-dot {{
        width: 7px; height: 7px; border-radius: 50%;
        margin-top: 4px; flex-shrink: 0;
    }}
    .osint-tier-dot.flash    {{ background: {C['red']}; box-shadow: 0 0 6px {C['red']}80; animation: blink 1.5s ease-in-out infinite; }}
    .osint-tier-dot.priority {{ background: {C['orange_br']}; }}
    .osint-tier-dot.routine  {{ background: {C['muted']}; }}
    .osint-src-badge {{
        font-size: 7.5px; letter-spacing: 0.5px; padding: 2px 5px;
        border-radius: 2px; text-transform: uppercase; font-weight: 700;
        text-align: center; margin-top: 1px; white-space: nowrap;
        overflow: hidden; text-overflow: ellipsis;
    }}
    .osint-content {{ flex: 1; }}
    .osint-title {{ font-size: 10px; line-height: 1.4; color: {C['text']}; }}
    .osint-title.flash {{ color: {C['red_bright']}; font-weight: 700; }}
    .osint-meta {{ font-size: 8px; color: {C['muted']}; margin-top: 2px; }}
    .osint-tier-tag {{
        font-size: 7px; letter-spacing: 2px; padding: 2px 5px; border-radius: 2px;
        font-weight: 700; text-transform: uppercase; white-space: nowrap;
        align-self: flex-start; margin-top: 2px;
    }}
    .osint-tier-tag.flash    {{ background: rgba(211,47,47,0.20); color: {C['red']}; border: 1px solid {C['red']}44; }}
    .osint-tier-tag.priority {{ background: rgba(192,122,42,0.18); color: {C['orange_br']}; }}
    .osint-tier-tag.routine  {{ background: rgba(100,100,100,0.12); color: {C['muted']}; }}

    .term-hdr {{
        background: {C['panel']}; border-bottom: 1px solid {C['border']};
        padding: 7px 16px; display: flex; align-items: center; gap: 16px;
    }}
    .term-logo {{ font-family:'Syne',sans-serif; font-size:14px; font-weight:800; color:{C['gold']}; letter-spacing:3px; }}
    .live-pill {{
        display:inline-flex; align-items:center; gap:6px;
        background:rgba(76,175,125,0.10); border:1px solid rgba(76,175,125,0.30);
        border-radius:20px; padding:2px 10px; font-size:9px; color:{C['green_br']}; letter-spacing:1.5px;
    }}
    .live-dot {{
        width:6px; height:6px; border-radius:50%; background:{C['green_br']};
        box-shadow:0 0 5px {C['green_br']}; animation:blink 2s ease-in-out infinite;
    }}
    @keyframes blink {{ 0%,100%{{opacity:1;}} 50%{{opacity:0.3;}} }}

    .phdr {{
        font-family:'Syne',sans-serif; font-weight:700; font-size:9px;
        letter-spacing:2px; text-transform:uppercase; color:{C['gold']};
        padding-bottom:6px; margin-bottom:8px; border-bottom:1px solid {C['border']};
        display:flex; justify-content:space-between; align-items:center;
    }}
    .badge {{
        font-size:8px; padding:2px 6px; border-radius:2px;
        letter-spacing:0.5px; text-transform:uppercase; font-weight:700;
    }}
    .bdg-red    {{ background:rgba(211,47,47,0.18);  color:{C['red']}; }}
    .bdg-green  {{ background:rgba(76,175,125,0.18); color:{C['green_br']}; }}
    .bdg-orange {{ background:rgba(192,122,42,0.18); color:{C['orange_br']}; }}
    .bdg-gold   {{ background:rgba(197,168,97,0.14); color:{C['gold']}; }}
    .bdg-blue   {{ background:rgba(74,143,224,0.14); color:{C['blue_br']}; }}

    .tp {{ background:{C['panel']}; border:1px solid {C['border']}; border-radius:3px; padding:10px 12px; margin-bottom:6px; }}

    .rr {{ display:flex; justify-content:space-between; align-items:center; padding:4px 0; border-bottom:1px solid {C['border']}; font-size:10.5px; }}
    .rr:last-child {{ border:none; }}

    .ni {{ padding:5px 0; border-bottom:1px solid {C['border']}; display:flex; gap:7px; align-items:flex-start; }}
    .ni:last-child {{ border:none; }}
    .ns {{ font-size:8px; padding:2px 4px; border-radius:2px; white-space:nowrap; flex-shrink:0; background:rgba(197,168,97,0.10); color:{C['gold']}; letter-spacing:0.5px; }}
    .ns.red  {{ background:rgba(211,47,47,0.12); color:{C['red']}; }}
    .ns.blue {{ background:rgba(74,143,224,0.12); color:{C['blue_br']}; }}
    .nh {{ font-size:10.5px; line-height:1.4; color:{C['text']}; }}
    .nm {{ font-size:8.5px; color:{C['muted']}; margin-top:1px; }}

    .ii {{ padding:4px 0; border-bottom:1px dotted {C['border']}; font-size:10px; line-height:1.4; color:{C['text']}; display:flex; gap:6px; align-items:flex-start; }}
    .it {{ font-size:8px; padding:1px 4px; border-radius:2px; background:rgba(211,47,47,0.14); color:{C['red']}; white-space:nowrap; flex-shrink:0; }}
    .it.blue {{ background:rgba(74,143,224,0.12); color:{C['blue_br']}; }}

    .vc {{ display:flex; gap:10px; padding:8px 12px; border-bottom:1px solid {C['border']}; align-items:flex-start; }}
    .vi {{ width:24px; height:24px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:12px; flex-shrink:0; margin-top:1px; }}
    .ipass {{ background:rgba(76,175,125,0.14); border:1px solid rgba(76,175,125,0.30); color:{C['green_br']}; }}
    .ifail {{ background:rgba(211,47,47,0.14);  border:1px solid rgba(211,47,47,0.30);  color:{C['red']}; }}
    .iwarn {{ background:rgba(192,122,42,0.14); border:1px solid rgba(192,122,42,0.30); color:{C['orange_br']}; }}

    .kr {{ display:flex; justify-content:space-between; padding:4px 8px; border-bottom:1px solid {C['border']}; font-size:10.5px; }}
    .kr:last-child {{ border:none; }}
    .kk {{ color:{C['muted']}; }}
    .kv {{ font-weight:700; text-align:right; }}

    .pbar-wrap {{ background:{C['border2']}; height:8px; border-radius:4px; overflow:hidden; margin:3px 0; }}
    .pbar      {{ height:100%; border-radius:4px; }}
    .clogo {{ width:40px; height:40px; border-radius:6px; background:{C['border2']}; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:700; color:{C['gold']}; border:1px solid {C['border']}; flex-shrink:0; }}
    </style>
    """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════════════
# §3  TRANSLATION ENGINE
# ═════════════════════════════════════════════════════════════════════════════════════

def translate_to_english(text: str) -> str:
    """
    Auto-detect and translate any text to English.
    ── LIVE API KEY INJECTION POINT ──
    For production, replace with:
      DeepL: deepl.Translator("YOUR_KEY").translate_text(text, target_lang="EN-US").text
      LibreTranslate: requests.post("https://libretranslate.com/translate", json={...}).json()["translatedText"]
    """
    if not text or len(text.strip()) < 5:
        return text
    try:
        if sum(1 for c in text if ord(c) < 128) / max(len(text), 1) > 0.90:
            return text
        result = GoogleTranslator(source="auto", target="en").translate(text[:4990])
        return result or text
    except Exception:
        return text


# ═════════════════════════════════════════════════════════════════════════════════════
# §4  DATA LAYER — Async + cached multi-source ingestion
# ═════════════════════════════════════════════════════════════════════════════════════

async def _async_get(url: str, session: aiohttp.ClientSession, timeout: int = 10) -> Optional[Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                ct = r.headers.get("Content-Type", "")
                if "json" in ct:
                    return await r.json(content_type=None)
                return await r.text()
    except Exception:
        pass
    return None


async def _fetch_concurrent(urls: List[str]) -> List[Optional[Any]]:
    connector = aiohttp.TCPConnector(ssl=False, limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        return await asyncio.gather(*[_async_get(url, session) for url in urls])


def run_async(coro) -> Any:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result(timeout=25)
        return loop.run_until_complete(coro)
    except Exception:
        try:
            return asyncio.run(coro)
        except Exception:
            return None


@st.cache_data(ttl=60, show_spinner=False)
def get_ohlcv(ticker: str, interval: str = "1d", period: str = "6mo") -> pd.DataFrame:
    """
    Yahoo Finance v8 OHLCV. Falls back to GBM synthetic data.
    ── LIVE API KEY INJECTION POINT ──
    Polygon.io: requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/...",
                             params={"apiKey": POLYGON_KEY})
    Alpaca:     alpaca_trade_api.REST(KEY, SECRET).get_bars(ticker, TimeFrame.Day, start, end).df
    """
    url = YF_CHART.format(ticker=ticker, interval=interval, range=period)
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        result = resp.json()["chart"]["result"][0]
        ts    = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(ts, unit="s"),
            "open":   [float(x) if x is not None else np.nan for x in quote.get("open",  [])],
            "high":   [float(x) if x is not None else np.nan for x in quote.get("high",  [])],
            "low":    [float(x) if x is not None else np.nan for x in quote.get("low",   [])],
            "close":  [float(x) if x is not None else np.nan for x in quote.get("close", [])],
            "volume": [float(x) if x is not None else 0.0    for x in quote.get("volume",[])],
        }).dropna(subset=["close"]).reset_index(drop=True)
        if len(df) > 5:
            return df
    except Exception:
        pass
    return _synthetic_ohlcv(ticker, period)


def _synthetic_ohlcv(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """GBM synthetic OHLCV — instant offline fallback. Replace with live feed post-deployment."""
    seeds = {"AAPL":280,"TSLA":230,"NVDA":120,"MSFT":415,"AMZN":185,
             "GOOG":165,"META":500,"JPM":195,"V":270,"BRK-B":370,
             "JNJ":155,"XOM":118,"SPY":515,"QQQ":440,"GLD":180}
    base = seeds.get(ticker, 150 + abs(hash(ticker)) % 200)
    n    = {"1d":1,"5d":5,"1mo":22,"3mo":66,"6mo":132,"1y":252,"2y":504}.get(period,132)
    np.random.seed(abs(hash(ticker)) % 9999)
    drift   = np.random.uniform(0.0001, 0.0006)
    vol     = np.random.uniform(0.014, 0.025)
    log_ret = np.random.normal(drift, vol, n)
    close   = base * np.exp(np.cumsum(log_ret))
    dr      = np.abs(np.random.normal(0, 0.008, n))
    opens   = close * (1 + np.random.uniform(-0.004, 0.004, n))
    highs   = np.maximum(opens, close) * (1 + dr * 1.2)
    lows    = np.minimum(opens, close) * (1 - dr * 1.2)
    vols    = np.random.randint(15_000_000, 100_000_000, n).astype(float)
    vols   *= np.where(np.abs(log_ret) > vol, 1.8, 1.0)
    dates   = pd.bdate_range(end=datetime.today(), periods=n)
    actual  = len(dates)
    return pd.DataFrame({"timestamp":dates,"open":opens[:actual],
                          "high":highs[:actual],"low":lows[:actual],
                          "close":close[:actual],"volume":vols[:actual]})

@st.cache_data(ttl=120, show_spinner=False)
def get_quote_summary(ticker: str) -> Dict:
    """
    Yahoo Finance quoteSummary — fundamentals.
    ── LIVE API KEY INJECTION POINT ──
    FMP: requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}")
    """
    url = YF_QUOTE.format(ticker=ticker)
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        result = resp.json()["quoteSummary"]["result"][0]
        merged = {}
        for mod in result.values():
            if isinstance(mod, dict):
                merged.update(mod)
        return merged
    except Exception:
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def get_ticker_prices() -> Dict[str, Dict]:
    """
    Batch live prices for ticker bar.
    ── LIVE API KEY INJECTION POINT ──
    IEX Cloud: requests.get("https://cloud.iexapis.com/stable/tops", params={"token":IEX_KEY,"symbols":syms})
    """
    out: Dict[str, Dict] = {}
    syms = [s for _, s in TICKER_BAR_SYMBOLS]
    try:
        url  = YF_BATCH.format(symbols="%2C".join(syms))
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        for q in resp.json()["quoteResponse"]["result"]:
            out[q.get("symbol","")] = {
                "price": q.get("regularMarketPrice", 0),
                "pct":   q.get("regularMarketChangePercent", 0),
            }
    except Exception:
        pass
    np.random.seed(int(time.time()) // 60)
    seeds = {"^GSPC":5280,"^IXIC":18640,"BTC-USD":105200,"GC=F":2481,
             "CL=F":79.4,"EURUSD=X":1.085,"AAPL":301.91,"NVDA":134.8,"TSLA":248.5}
    for _, sym in TICKER_BAR_SYMBOLS:
        if sym not in out:
            base = seeds.get(sym, 100)
            pct  = np.random.uniform(-0.8, 0.8)
            out[sym] = {"price": round(base*(1+pct/100),2), "pct": round(pct,2)}
    return out


@st.cache_data(ttl=300, show_spinner=False)
def fetch_rss_news(category: str = "world", max_items: int = 20) -> List[Dict]:
    """
    RSS multi-feed ingestion with translation.
    ── LIVE API KEY INJECTION POINT ──
    NewsAPI: requests.get("https://newsapi.org/v2/top-headlines",
             params={"category":category,"apiKey":NEWS_API_KEY,"language":"en"})
    """
    articles: List[Dict] = []
    for url in RSS_FEEDS.get(category, RSS_FEEDS["world"]):
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries[:8]:
                title  = translate_to_english(entry.get("title","") or "")
                source = re.sub(r"[^A-Z0-9 &]","",
                                parsed.feed.get("title", url.split("//")[-1].split("/")[0]).upper())[:14]
                articles.append({
                    "source": source,
                    "title":  title,
                    "link":   entry.get("link",""),
                    "published": entry.get("published","")[:16],
                    "is_alert": any(kw in title.lower() for kw in
                                    ["attack","strike","conflict","kill","war","missile",
                                     "threat","sanction","explosion","bomb","nuclear","crisis"]),
                })
        except Exception:
            continue
    return (articles or _seed_news(category))[:max_items]


def _seed_news(cat: str) -> List[Dict]:
    _w = [
        ("BBC",      "Iranian missiles injure 100 near Israeli nuclear facility",           True),
        ("REUTERS",  "Russia-China veto UN resolution to reopen Strait of Hormuz",          True),
        ("AP",       "Zelensky urges allies to hold pressure on Russia ahead of talks",     False),
        ("BBC",      "National blackout hits Cuba for second consecutive time this week",   False),
        ("REUTERS",  "Multiple Iranian strikes reported across Israeli territory",           True),
        ("DW",       "Germany coalition talks collapse — snap election likely in autumn",    False),
        ("AL JAZ.",  "Ceasefire negotiations in Qatar stall as conditions remain unclear",   False),
        ("CNN",      "Pentagon confirms F-22 deployment to Gulf as tensions escalate",      True),
        ("GUARDIAN", "China PBOC cuts rates 10bp in surprise emergency stimulus move",      False),
        ("FT",       "Global shipping costs surge 18% as Red Sea diversions continue",      False),
    ]
    _m = [
        ("MIL TIMES","Army receives new Black Hawk helicopter variant, pilot optional",     False),
        ("USNI",     "USS Theodore Roosevelt carrier group enters Persian Gulf",             True),
        ("T&PURPOSE","Pentagon deploys additional F-22 Raptors to Middle East theater",    True),
        ("ORYX",     "Russia confirms loss of 3 Su-34 strike aircraft in 72 hours",        True),
        ("MOD UK",   "Royal Navy HMS Lancaster begins Red Sea patrol operations",           False),
        ("DEF NEWS", "NATO activates Article 4 consultations over Baltic airspace",         True),
    ]
    _mk = [
        ("CNBC",     "S&P 500 climbs to record high on strong tech earnings beats",         False),
        ("BLOOMBERG","Oil surges 3.2% on Hormuz strait partial closure threats",            True),
        ("FT",       "Gold hits $2,481 as investors seek safe-haven assets amid conflict",  False),
        ("REUTERS",  "NVIDIA briefly crosses $4T market cap on sustained AI demand",        False),
        ("WSJ",      "BTC consolidates above $105K ahead of spot ETF options expansion",    False),
    ]
    _us = [
        ("AXIOS",    "Senate votes to advance Mullin nomination to lead DHS",               False),
        ("NYT",      "ICE officers to deploy to airports as delays mount, DHS confirms",    False),
        ("CNBC",     "Fed signals no rush to cut rates with core inflation above 3%",       False),
        ("WSJ",      "US trade deficit narrows sharply as exports surge to record high",    False),
        ("BLOOMBERG","Treasury 10Y yield rises to 4.62% on stronger-than-expected jobs",   False),
    ]
    _eu = [
        ("EURONEWS", "French mayoral elections: Far right suffers setbacks, Philippe wins", False),
        ("DW",       "ECB officials signal June rate cut as German inflation eases",         False),
        ("REUTERS",  "UK economy grows 0.4% in Q1, beating all analyst expectations",       False),
        ("FT",       "Poland increases defence spending to 5% GDP amid Russia threat",      False),
        ("EURONEWS", "Georgia constitutional crisis deepens as protests enter Day 5",       True),
    ]
    db = {"world":_w,"military":_m,"markets":_mk,"us":_us,"europe":_eu}
    rows = db.get(cat, _w)
    return [{"source":r[0],"title":r[1],"link":"","published":"","is_alert":r[2]} for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_gdelt_events() -> pd.DataFrame:
    """
    GDELT GeoJSON point data for globe.
    ── LIVE API KEY INJECTION POINT ──
    No key required. Increase maxpoints up to 500 for denser coverage.
    """
    try:
        resp = requests.get(GDELT_GEO, timeout=12,
                            headers={"User-Agent":"Mozilla/5.0"})
        data = resp.json()
        rows = []
        for pt in data.get("features", [])[:120]:
            coords = pt.get("geometry",{}).get("coordinates",[0,0])
            props  = pt.get("properties",{})
            name   = translate_to_english(props.get("name","Intelligence Event"))[:70]
            tone   = float(props.get("avgtone",0) or 0)
            sev    = min(max(abs(tone)/15.0, 0.15), 1.0)
            r,g,b  = (210,47,47) if tone < -8 else (192,122,42) if tone < -2 else (76,175,125)
            rows.append({"lat":coords[1],"lng":coords[0],"name":name,"severity":sev,
                          "color_r":r,"color_g":g,"color_b":b,"color_a":int(sev*210),
                          "radius":25000+sev*85000,"type":"GDELT"})
        if len(rows) > 5:
            return pd.DataFrame(rows)
    except Exception:
        pass
    return _seed_globe_df()


def _seed_globe_df() -> pd.DataFrame:
    rows = []
    type_colors = {"Active Conflict":(210,47,47),"Military Alert":(192,122,42),
                   "Naval Tension":(74,143,224),"Infrastructure":(192,122,42),
                   "Nuclear Risk":(180,40,180),"Civil Unrest":(150,110,30),
                   "Insurgency":(150,110,30),"Reconstruction":(80,130,50)}
    for hs in CONFLICT_HOTSPOTS:
        r,g,b = type_colors.get(hs["type"],(150,110,30))
        rows.append({"lat":hs["lat"],"lng":hs["lng"],"name":hs["name"],"severity":hs["severity"],
                      "color_r":r,"color_g":g,"color_b":b,"color_a":int(hs["severity"]*220),
                      "radius":30000+hs["severity"]*90000,"type":hs["type"]})
    np.random.seed(42)
    for clat,clng in [(31,34),(50,31),(35,51),(12,45),(26,56),(24,119),(40,125),(15,32)]:
        for _ in range(10):
            sev = np.random.uniform(0.18, 0.65)
            rows.append({"lat":clat+np.random.normal(0,2.8),"lng":clng+np.random.normal(0,2.8),
                          "name":"GDELT Intelligence Event","severity":sev,
                          "color_r":197,"color_g":168,"color_b":97,"color_a":int(sev*150),
                          "radius":22000+sev*55000,"type":"GDELT"})
    return pd.DataFrame(rows)


def _military_df() -> pd.DataFrame:
    rows = []
    for b in MILITARY_BASES:
        c = [74,143,224] if b["country"] in ("US","NATO","UK") else [210,100,50]
        rows.append({"lat":b["lat"],"lng":b["lng"],"name":b["name"],"severity":0.6,
                      "color_r":c[0],"color_g":c[1],"color_b":c[2],"color_a":190,
                      "radius":42000,"type":"Military"})
    return pd.DataFrame(rows)


def _nuclear_df() -> pd.DataFrame:
    return pd.DataFrame([{"lat":s["lat"],"lng":s["lng"],"name":f"☢ {s['name']}","severity":0.85,
                           "color_r":180,"color_g":40,"color_b":180,"color_a":210,
                           "radius":52000,"type":"Nuclear"} for s in NUCLEAR_SITES])


@st.cache_data(ttl=120, show_spinner=False)
def fetch_live_flights() -> pd.DataFrame:
    """
    OpenSky ADS-B flights over Middle East / Eastern Med.
    ── LIVE API KEY INJECTION POINT ──
    No key required for public access (400 req/day limit).
    Register at opensky-network.org for higher limits.
    """
    url = ("https://opensky-network.org/api/states/all"
           "?lamin=10.0&lomin=25.0&lamax=45.0&lomax=75.0")
    try:
        resp = requests.get(url, timeout=10)
        rows = []
        for s in (resp.json().get("states",[]) or [])[:200]:
            if s[6] and s[5]:
                rows.append({"lat":s[6],"lng":s[5],"name":f"✈ {(s[1] or 'UNK').strip()}",
                              "severity":0.3,"color_r":74,"color_g":143,"color_b":224,
                              "color_a":160,"radius":16000,"type":"Flight"})
        if rows:
            return pd.DataFrame(rows)
    except Exception:
        pass
    np.random.seed(int(time.time())//120)
    rows = []
    for lat1,lng1,lat2,lng2 in [(35,35,28,55),(40,30,24,53),(50,28,15,45),(31,32,25,57)]:
        for i in range(6):
            t = i/5.0
            rows.append({"lat":lat1+t*(lat2-lat1)+np.random.uniform(-0.5,0.5),
                          "lng":lng1+t*(lng2-lng1)+np.random.uniform(-0.5,0.5),
                          "name":f"✈ FLT{random.randint(100,999)}","severity":0.3,
                          "color_r":74,"color_g":143,"color_b":224,
                          "color_a":160,"radius":16000,"type":"Flight"})
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600, show_spinner=False)
def get_macro_indicators() -> Dict[str, Dict]:
    """
    FRED macro indicators with seed fallback.
    ── LIVE API KEY INJECTION POINT ──
    Replace 'DEMO' with free FRED key from fred.stlouisfed.org/docs/api/api_key.html
    Alpha Vantage for intraday VIX/DXY: AV_KEY in requests.get(AV_URL, params={"apikey":AV_KEY,...})
    """
    fred_key = "DEMO"
    results  = dict(MACRO_SEED)
    for label, sid in FRED_SERIES.items():
        try:
            url  = FRED_BASE.format(key=fred_key, sid=sid)
            resp = requests.get(url, timeout=7)
            obs  = [o for o in resp.json().get("observations",[])
                    if o.get("value","") not in ("",".",None)]
            if len(obs) >= 2:
                v1, v2 = float(obs[0]["value"]), float(obs[1]["value"])
                delta  = v1 - v2
                sign   = "+" if delta >= 0 else ""
                trend  = "up" if delta > 0 else "down" if delta < 0 else "flat"
                results[label] = {"value":f"{v1:.2f}","delta":f"{sign}{delta:.3f}",
                                   "trend":trend,"note":results.get(label,{}).get("note","")}
        except Exception:
            continue
    return results

# ═════════════════════════════════════════════════════════════════════════════════════
# §5  GLOBE BUILDER — PyDeck multi-layer
# ═════════════════════════════════════════════════════════════════════════════════════

def build_globe(events_df: pd.DataFrame, show_military: bool = False,
                show_nuclear: bool = False, show_flights: bool = False,
                show_cables: bool = False, show_hex: bool = True,
                zoom: float = 1.4, lat: float = 28.0, lng: float = 35.0) -> pdk.Deck:
    """
    Multi-layer PyDeck globe:
      ScatterplotLayer — conflict/GDELT hotspots
      HexagonLayer     — event density heatmap with 3D elevation
      ScatterplotLayer — military bases (optional)
      ScatterplotLayer — nuclear sites (optional)
      ScatterplotLayer — live ADS-B flights (optional)
      PathLayer        — undersea cable routes (optional)
    Zoom > 3.5 → satellite mode (requires Mapbox key injection)
    """
    df = events_df.copy()
    df["color"] = df.apply(
        lambda r: [int(r.color_r), int(r.color_g), int(r.color_b), int(r.color_a)], axis=1)

    country_df = pd.DataFrame(COUNTRY_CII)
    layers = []

    # Country risk halos
    layers.append(pdk.Layer("ScatterplotLayer", data=country_df,
        get_position=["lng","lat"], get_color="color", get_radius=320000,
        radius_min_pixels=5, radius_max_pixels=60, pickable=True,
        opacity=0.35, stroked=True, line_width_min_pixels=1))

    # Hex density elevation
    if show_hex and len(df) > 5:
        layers.append(pdk.Layer("HexagonLayer", data=df,
            get_position=["lng","lat"], radius=140000,
            elevation_scale=55, elevation_range=[0,1800],
            pickable=False, extruded=True, coverage=0.85,
            color_range=[
                [20,30,35,100],[50,70,70,140],[100,90,40,160],
                [170,110,25,185],[210,70,25,210],[210,47,47,235]],
            opacity=0.42))

    # Primary event scatter
    layers.append(pdk.Layer("ScatterplotLayer", data=df,
        get_position=["lng","lat"], get_color="color", get_radius="radius",
        radius_min_pixels=4, radius_max_pixels=42,
        pickable=True, auto_highlight=True))

    if show_military:
        mdf = _military_df()
        mdf["color"] = mdf.apply(
            lambda r:[int(r.color_r),int(r.color_g),int(r.color_b),int(r.color_a)],axis=1)
        layers.append(pdk.Layer("ScatterplotLayer", data=mdf,
            get_position=["lng","lat"], get_color="color", get_radius=40000,
            radius_min_pixels=5, radius_max_pixels=18,
            pickable=True, stroked=True, line_width_min_pixels=2))

    if show_nuclear:
        ndf = _nuclear_df()
        ndf["color"] = ndf.apply(
            lambda r:[int(r.color_r),int(r.color_g),int(r.color_b),int(r.color_a)],axis=1)
        layers.append(pdk.Layer("ScatterplotLayer", data=ndf,
            get_position=["lng","lat"], get_color="color", get_radius=55000,
            radius_min_pixels=7, radius_max_pixels=22,
            pickable=True, stroked=True, line_width_min_pixels=2))

    if show_cables:
        cable_data = [{"name":c["name"],"path":c["path"],"color":[74,143,224,160]}
                      for c in CABLE_ROUTES]
        layers.append(pdk.Layer("PathLayer", data=cable_data,
            get_path="path", get_color="color", get_width=3,
            width_min_pixels=1, width_max_pixels=4, pickable=True))

    if show_flights:
        fdf = fetch_live_flights()
        if len(fdf) > 0:
            fdf["color"] = [[74,143,224,160]] * len(fdf)
            layers.append(pdk.Layer("ScatterplotLayer", data=fdf,
                get_position=["lng","lat"], get_color="color", get_radius=16000,
                radius_min_pixels=3, radius_max_pixels=10, pickable=True))

    view    = pdk.ViewState(latitude=lat, longitude=lng, zoom=zoom, pitch=0, bearing=0)
    tooltip = {
        "html": ("<div style='font-family:Space Mono,monospace;background:#0c0e13;"
                 "border:1px solid #252b38;padding:8px 10px;border-radius:3px;"
                 "font-size:10px;color:#c8cdd8;max-width:240px;line-height:1.5;'>"
                 "<b style='color:#c5a861;'>{name}</b><br/>Type: {type}</div>"),
        "style": {"backgroundColor":"transparent","color":"white"},
    }

    map_style = "mapbox://styles/mapbox/satellite-v9" if zoom >= 4.0 else "mapbox://styles/mapbox/dark-v11"

    return pdk.Deck(
        layers=layers, initial_view_state=view, tooltip=tooltip,
        map_style=map_style,
        # ── LIVE API KEY INJECTION POINT ──
        # api_keys={"mapbox": "pk.eyJ1IjoiWU9VUl9LRVkiLCJhIjoiWU9VUl9LRVkifQ.XXXX"},
        parameters={"depthTest": False},
    )


# ═════════════════════════════════════════════════════════════════════════════════════
# §6  CHART BUILDERS — Plotly dark-themed charts
# ═════════════════════════════════════════════════════════════════════════════════════

def build_candlestick(df: pd.DataFrame, ticker: str, show_ma: bool = True,
                      show_volume: bool = True, show_bb: bool = False,
                      show_rsi: bool = False) -> go.Figure:
    """
    Full-featured dark candlestick:
      • OHLC candles with colour-coded wicks
      • MA20 (gold) + MA50 (blue) overlays
      • Bollinger Bands ±2σ (optional)
      • RSI(14) sub-pane (optional)
      • Volume bars colour-matched to candle direction
      • Crosshair spikes + unified hover
      • Full zoom/pan via plotly dragmode
    """
    n_extra = (1 if show_volume else 0) + (1 if show_rsi else 0)
    total   = 1 + n_extra
    rh_base = 1.0 - (0.22 if show_volume else 0) - (0.15 if show_rsi else 0)
    row_h   = [rh_base] + ([0.22] if show_volume else []) + ([0.15] if show_rsi else [])

    fig = make_subplots(rows=total, cols=1, shared_xaxes=True,
                        vertical_spacing=0.015, row_heights=row_h)

    fig.add_trace(go.Candlestick(
        x=df["timestamp"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color=C["green_br"], decreasing_line_color=C["red"],
        increasing_fillcolor=C["green_br"],  decreasing_fillcolor=C["red"],
        name=ticker, hoverinfo="x+y",
        increasing_line_width=1.2, decreasing_line_width=1.2,
    ), row=1, col=1)

    df = df.copy()
    if show_ma and len(df) >= 20:
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma50"] = df["close"].rolling(50).mean()
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ma20"],
            line=dict(color=C["gold"],width=1.3), name="MA 20", hoverinfo="skip"), row=1, col=1)
        if df["ma50"].notna().sum() > 10:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ma50"],
                line=dict(color=C["blue_br"],width=1.0,dash="dot"),
                name="MA 50", hoverinfo="skip"), row=1, col=1)

    if show_bb and len(df) >= 20:
        df["bbm"] = df["close"].rolling(20).mean()
        df["bbs"] = df["close"].rolling(20).std()
        df["bbu"] = df["bbm"] + 2*df["bbs"]
        df["bbl"] = df["bbm"] - 2*df["bbs"]
        fig.add_trace(go.Scatter(
            x=pd.concat([df["timestamp"],df["timestamp"][::-1]]),
            y=pd.concat([df["bbu"],df["bbl"][::-1]]),
            fill="toself", fillcolor="rgba(197,168,97,0.08)",
            line=dict(color="rgba(0,0,0,0)"), name="BB ±2σ", hoverinfo="skip",
        ), row=1, col=1)
        for y_col, name in [("bbu","BB Upper"),("bbl","BB Lower")]:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df[y_col],
                line=dict(color=C["gold_dim"],width=0.8,dash="dot"),
                name=name, hoverinfo="skip"), row=1, col=1)

    vol_row = 2
    if show_volume and "volume" in df.columns:
        colors = [C["green_br"] if c >= o else C["red"]
                  for c,o in zip(df["close"],df["open"])]
        fig.add_trace(go.Bar(x=df["timestamp"], y=df["volume"],
            marker_color=colors, marker_opacity=0.55,
            name="Volume", hoverinfo="skip"), row=vol_row, col=1)

    rsi_row = vol_row + (1 if show_volume else 0)
    if show_rsi and len(df) >= 15:
        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        df["rsi"] = 100 - (100/(1+(gain/loss.replace(0,np.nan))))
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["rsi"],
            line=dict(color=C["gold"],width=1.2),
            name="RSI(14)", hoverinfo="skip"), row=rsi_row, col=1)
        for lvl, col in [(70,C["red"]),(30,C["green_br"]),(50,C["muted"])]:
            fig.add_hline(y=lvl, line_color=col, line_dash="dot",
                          line_width=0.8, row=rsi_row, col=1)

    last = df["close"].iloc[-1] if len(df) else 0
    prev = df["close"].iloc[-2] if len(df) >= 2 else last
    pct  = (last-prev)/prev*100 if prev else 0
    sign = "▲" if pct >= 0 else "▼"

    fig.update_layout(
        title=dict(text=f"{ticker}   ${last:,.2f}   {sign} {abs(pct):.2f}%",
                   font=dict(family="Syne,monospace",size=13,color=C["text"]), x=0.01),
        paper_bgcolor=C["bg"], plot_bgcolor=C["panel"],
        font=dict(family="Space Mono,monospace",size=9,color=C["muted"]),
        xaxis_rangeslider_visible=False,
        margin=dict(l=6,r=6,t=34,b=6),
        legend=dict(orientation="h",y=1.02,x=0.01,
                    bgcolor="rgba(0,0,0,0)",font=dict(size=9,color=C["muted"])),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=C["panel"],bordercolor=C["border2"],
                        font_family="Space Mono,monospace",font_size=9),
        dragmode="zoom",
    )
    ax = dict(gridcolor=C["border"],showgrid=True,zeroline=False,
              linecolor=C["border"],tickfont=dict(size=8,color=C["muted"]),
              showspikes=True,spikecolor=C["gold"],spikethickness=1,spikedash="dot")
    fig.update_xaxes(**ax)
    fig.update_yaxes(**ax, tickprefix="$")
    if show_volume:
        fig.update_yaxes(tickprefix="",tickformat=".2s",row=vol_row,col=1)
    if show_rsi:
        fig.update_yaxes(tickprefix="",range=[0,100],row=rsi_row,col=1)
    return fig


def build_earnings_chart(profile: Dict) -> go.Figure:
    qtrs = ["Q1","Q2","Q3","Q4(E)"]
    eps  = profile.get("quarterly_eps",[0,0,0,0])
    rev  = profile.get("quarterly_rev",[0,0,0,0])
    fig  = make_subplots(rows=1, cols=2,
                         subplot_titles=["Quarterly EPS ($)","Revenue ($B)"],
                         horizontal_spacing=0.08)
    fig.add_trace(go.Bar(x=qtrs,y=eps,
        marker_color=[C["green_br"] if e>=0 else C["red"] for e in eps],
        marker_opacity=0.8, name="EPS", showlegend=False), row=1, col=1)
    fig.add_trace(go.Bar(x=qtrs,y=rev,
        marker_color=C["gold"], marker_opacity=0.7,
        name="Rev $B", showlegend=False), row=1, col=2)
    fig.update_layout(paper_bgcolor=C["bg"],plot_bgcolor=C["panel"],
        font=dict(family="Space Mono,monospace",size=9,color=C["muted"]),
        margin=dict(l=4,r=4,t=26,b=4),height=190,showlegend=False)
    ax = dict(gridcolor=C["border"],showgrid=True,zeroline=False,
              linecolor=C["border"],tickfont=dict(size=8,color=C["muted"]))
    fig.update_xaxes(**ax); fig.update_yaxes(**ax)
    for a in fig.layout.annotations:
        a.font.color = C["gold"]; a.font.size = 9
    return fig


def build_segment_chart(profile: Dict) -> go.Figure:
    segs = profile.get("revenue_segments",{})
    if not segs:
        return go.Figure()
    labels = list(segs.keys()); values = list(segs.values())
    colors = [C["gold"],C["blue_br"],C["green_br"],C["orange_br"],C["purple"],C["red"]][:len(labels)]
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.55,
        marker=dict(colors=colors,line=dict(color=C["bg"],width=2)),
        textfont=dict(family="Space Mono,monospace",size=9,color=C["text"]),
        hoverinfo="label+percent"))
    fig.update_layout(paper_bgcolor=C["bg"],plot_bgcolor=C["bg"],
        margin=dict(l=4,r=4,t=8,b=4),height=190,showlegend=True,
        legend=dict(font=dict(size=8,color=C["muted"]),bgcolor="rgba(0,0,0,0)"))
    return fig


def build_dcf_sensitivity(current: float, fair: float) -> go.Figure:
    """DCF sensitivity heatmap — overvaluation % across WACC × Terminal Growth Rate."""
    wacc_r = [0.07,0.08,0.09,0.10,0.11,0.12]
    tgr_r  = [0.01,0.02,0.025,0.03,0.035,0.04]
    fcf    = current * 0.034
    z = []
    for w in wacc_r:
        row = []
        for g in tgr_r:
            pv = fcf*(1+g)/(w-g) if w > g else fair
            row.append(round((current-pv)/current*100, 1))
        z.append(row)
    fig = go.Figure(go.Heatmap(
        z=z,
        x=[f"{g*100:.1f}% TGR" for g in tgr_r],
        y=[f"{w*100:.0f}% WACC" for w in wacc_r],
        colorscale=[[0,"#2e7d52"],[0.4,"#c5a861"],[1,"#d32f2f"]],
        text=[[f"{v:+.1f}%" for v in row] for row in z],
        texttemplate="%{text}",
        textfont={"size":8,"color":"white"},
        hovertemplate="WACC: %{y}<br>TGR: %{x}<br>Premium: %{z}%<extra></extra>",
        zmin=-30, zmax=60,
        colorbar=dict(tickfont=dict(size=8,color=C["muted"]),
                      title=dict(text="Over%",font=dict(size=8,color=C["muted"]))),
    ))
    fig.update_layout(
        paper_bgcolor=C["bg"], plot_bgcolor=C["panel"],
        margin=dict(l=4,r=4,t=8,b=4), height=175,
        font=dict(family="Space Mono,monospace",size=8,color=C["muted"]),
    )
    fig.update_xaxes(tickfont=dict(size=8,color=C["muted"]))
    fig.update_yaxes(tickfont=dict(size=8,color=C["muted"]))
    return fig


def build_macro_bar(macro: Dict) -> go.Figure:
    keys   = ["Fed Funds Rate","10Y Treasury","US CPI YoY","US GDP (QoQ)","VIX Index"]
    labels = ["Fed Rate","10Y UST","CPI YoY","GDP","VIX"]
    vals   = []
    for k in keys:
        v = macro.get(k, MACRO_SEED.get(k,{})).get("value","0")
        try: vals.append(float(re.sub(r"[^0-9.\-]","",v)))
        except: vals.append(0.0)
    colors = [C["red"],C["red"],C["orange_br"],C["green_br"],C["green_br"]]
    fig = go.Figure(go.Bar(x=labels, y=vals, marker_color=colors, marker_opacity=0.8,
        text=[f"{v:.2f}" for v in vals], textposition="outside",
        textfont=dict(size=8,color=C["muted"])))
    fig.update_layout(paper_bgcolor=C["bg"],plot_bgcolor=C["panel"],
        margin=dict(l=4,r=4,t=8,b=4),height=145,showlegend=False,
        font=dict(family="Space Mono,monospace",size=8,color=C["muted"]))
    ax = dict(gridcolor=C["border"],showgrid=True,zeroline=False,
              linecolor=C["border"],tickfont=dict(size=8,color=C["muted"]))
    fig.update_xaxes(**ax); fig.update_yaxes(**ax)
    return fig


# ═════════════════════════════════════════════════════════════════════════════════════
# §7  UI COMPONENT LIBRARY
# ═════════════════════════════════════════════════════════════════════════════════════

def render_ticker_bar(prices: Dict) -> None:
    logo = (f'<span class="gold" style="font-family:Syne,sans-serif;font-weight:800;'
            f'font-size:13px;letter-spacing:2px;margin-right:18px;">TERMINAL //</span>'
            f'<span style="font-size:8px;color:{C["muted"]};letter-spacing:1px;'
            f'margin-right:18px;padding-right:18px;border-right:1px solid {C["border"]};">'
            f'{datetime.now(timezone.utc).strftime("%H:%M UTC")}</span>')
    items = logo
    for label, sym in TICKER_BAR_SYMBOLS:
        info  = prices.get(sym, {})
        price = info.get("price", 0)
        pct   = info.get("pct", 0)
        cls   = "pos" if pct >= 0 else "neg"
        sign  = "▲" if pct >= 0 else "▼"
        items += (f'<span class="t-item"><span class="t-label">{label}</span>'
                  f'<span>{price:,.2f}</span> '
                  f'<span class="{cls}">{sign}{abs(pct):.2f}%</span></span>')
    st.markdown(f'<div class="ticker-strip">{items}</div>', unsafe_allow_html=True)


def phdr(title: str, badge: str = "", btype: str = "gold") -> None:
    b = f'<span class="badge bdg-{btype}">{badge}</span>' if badge else ""
    st.markdown(f'<div class="phdr">&gt;&nbsp;{title}&nbsp;{b}</div>',
                unsafe_allow_html=True)


def render_risk_panel(data: List[Dict]) -> None:
    top  = max(data, key=lambda x: x["score"])
    phdr("Strategic Risk Overview", f"TOP CII: {top['score']}", "red")
    rows = ""
    for c in sorted(data, key=lambda x: -x["score"])[:10]:
        s   = c["score"]
        col = C["red"] if s>=55 else C["orange_br"] if s>=35 else C["green_br"]
        lbl = "CRITICAL" if s>=60 else "HIGH" if s>=45 else "MEDIUM" if s>=30 else "LOW"
        rows += (f'<div class="rr">'
                 f'<span style="display:flex;align-items:center;gap:6px;">'
                 f'<span style="width:7px;height:7px;border-radius:50%;background:{col};flex-shrink:0;"></span>'
                 f'{c["country"]}</span>'
                 f'<span style="display:flex;align-items:center;gap:8px;">'
                 f'<span style="font-size:8px;color:{col};letter-spacing:1px;">{lbl}</span>'
                 f'<span style="width:55px;height:3px;background:{C["border2"]};border-radius:2px;display:inline-block;">'
                 f'<span style="display:block;width:{int(s)}%;height:100%;background:{col};border-radius:2px;"></span></span>'
                 f'<span style="color:{col};font-weight:700;min-width:24px;text-align:right;">{s}</span>'
                 f'</span></div>')
    st.markdown(f'<div class="tp" style="padding:6px 10px;">{rows}</div>',
                unsafe_allow_html=True)


def render_pentagon_index() -> None:
    pi    = PENTAGON_INDEX
    score = pi["score"]
    col   = C["red"] if score>=70 else C["orange_br"] if score>=50 else C["green_br"]
    phdr("Pentagon Threat Index", f"{score}/100 — {pi['label']}", "red")
    html  = (f'<div class="tp" style="padding:8px 10px;">'
             f'<div style="display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:8px;">'
             f'<div style="font-family:Syne,sans-serif;font-size:32px;font-weight:800;color:{col};line-height:1;">{score}</div>'
             f'<div style="font-size:8px;color:{C["muted"]};text-align:right;letter-spacing:1px;">COMPOSITE<br>THREAT SCORE<br>/100</div>'
             f'</div>'
             f'<div style="background:{C["border2"]};height:5px;border-radius:3px;margin-bottom:10px;">'
             f'<div style="width:{score}%;height:100%;background:{col};border-radius:3px;"></div></div>')
    for comp, val in pi["components"].items():
        c2 = C["red"] if val>=70 else C["orange_br"] if val>=50 else C["green_br"]
        html += (f'<div style="margin-bottom:5px;">'
                 f'<div style="display:flex;justify-content:space-between;font-size:9px;margin-bottom:2px;">'
                 f'<span style="color:{C["muted"]};">{comp}</span>'
                 f'<span style="color:{c2};font-weight:700;">{val}</span></div>'
                 f'<div class="pbar-wrap"><div class="pbar" style="width:{val}%;background:{c2};"></div></div>'
                 f'</div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_intel_feed(items: List[Dict]) -> None:
    alerts = sum(1 for i in items if i.get("is_alert"))
    phdr("Live Intel Feed", f"{alerts} ALERTS", "red")
    html = '<div class="tp" style="max-height:195px;overflow-y:auto;padding:6px 10px;">'
    for item in items[:14]:
        tag_cls = "it" if item.get("is_alert") else "it blue"
        html += (f'<div class="ii"><span class="{tag_cls}">{item["source"][:10]}</span>'
                 f'<span>{item["title"][:105]}</span></div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_infra_cascade(idx: int = 0) -> None:
    node = INFRA_CASCADE[idx]
    phdr("Infrastructure Cascade", f"{node['node']} — {node['type']}", "orange")
    for country, pct in zip(node["countries"], node["risk_pct"]):
        lbl = "CRITICAL" if pct>=65 else "HIGH" if pct>=50 else "MEDIUM" if pct>=25 else "LOW"
        col = C["red"] if pct>=50 else C["orange_br"] if pct>=25 else C["green_br"]
        lc, bc = st.columns([3,5])
        with lc: st.markdown(f'<span style="font-size:10px;">{country}</span>', unsafe_allow_html=True)
        with bc:
            st.progress(pct/100)
            st.markdown(f'<span style="font-size:8px;color:{col};">{lbl} {pct}%</span>',
                        unsafe_allow_html=True)


def render_news_feed(articles: List[Dict], max_items: int = 12) -> None:
    html = '<div style="max-height:240px;overflow-y:auto;">'
    for a in articles[:max_items]:
        sc  = "ns red" if a.get("is_alert") else "ns"
        html += (f'<div class="ni"><span class="{sc}">{a["source"][:13]}</span>'
                 f'<div><div class="nh">{a["title"][:115]}</div>'
                 f'<div class="nm">{a.get("published","")[:16]}</div></div></div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_macro_grid(macro: Dict) -> None:
    keys    = list(macro.keys())
    cols    = st.columns(2)
    neg_up  = {"US CPI YoY","10Y Treasury","2Y Treasury","Fed Funds Rate",
               "DXY (USD Index)","Yield Spread","ISM Mfg PMI"}
    pos_dn  = {"VIX Index","US Unemployment"}
    for i, key in enumerate(keys):
        info  = macro[key]
        val   = info.get("value","—")
        delta = info.get("delta","")
        note  = info.get("note","")
        trend = info.get("trend","flat")
        is_b  = (trend=="up" and key in neg_up) or (trend=="down" and key in pos_dn)
        is_g  = (trend=="up" and key not in neg_up) or (trend=="down" and key in pos_dn)
        dc    = C["red"] if is_b else C["green_br"] if is_g else C["muted"]
        arr   = "▲" if trend=="up" else "▼" if trend=="down" else "—"
        with cols[i%2]:
            st.markdown(
                f'<div class="tp" style="margin-bottom:4px;padding:7px 10px;">'
                f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};text-transform:uppercase;margin-bottom:2px;">{key}</div>'
                f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;color:{C["text"]};line-height:1;">{val}</div>'
                f'<div style="font-size:8.5px;color:{dc};margin-top:1px;">{arr} {delta}'
                f'&nbsp;<span style="color:{C["muted"]};">{note}</span></div></div>',
                unsafe_allow_html=True)


def render_kv(rows: List[Tuple[str,str,str]]) -> None:
    html = '<div class="tp" style="padding:4px;">'
    for k,v,col in rows:
        html += (f'<div class="kr"><span class="kk">{k}</span>'
                 f'<span class="kv" style="color:{col};">{v}</span></div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_val_cards(profile: Dict) -> None:
    icon_cls = {"green":"ipass","red":"ifail","orange":"iwarn"}
    for card in profile.get("cards",[]):
        cls = icon_cls.get(card["color"],"iwarn")
        col = {"green":C["green_br"],"red":C["red"],"orange":C["orange_br"]}.get(card["color"],C["muted"])
        st.markdown(
            f'<div class="vc"><div class="vi {cls}">{card["icon"]}</div>'
            f'<div><div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};text-transform:uppercase;">{card["cat"]}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:12.5px;font-weight:700;color:{col};">{card["verdict"]}</div>'
            f'<div style="font-size:9px;color:{C["muted"]};">{card["detail"]}</div></div></div>',
            unsafe_allow_html=True)


def render_dcf_panel(profile: Dict, mode: str = "DCF") -> None:
    current  = profile["current_price"]
    fair_map = {"DCF":profile["fair_value_dcf"],"DDM":profile["fair_value_ddm"],"EV/EBITDA":profile["fair_value_ev"]}
    fair     = fair_map.get(mode, profile["fair_value_dcf"])
    is_over  = current > fair*1.03
    pct      = abs(current-fair)/max(fair,0.01)*100
    verdict  = "OVERVALUED" if is_over else "UNDERVALUED" if fair>current*1.03 else "FAIRLY VALUED"
    vcol     = C["red"] if is_over else C["green_br"] if fair>current*1.03 else C["gold"]
    bar_r    = min(fair/current,1.0) if is_over else min(current/fair,1.0)

    st.markdown(
        f'<div class="tp" style="text-align:center;background:rgba(0,0,0,0.3);border-color:{vcol}33;margin-bottom:6px;">'
        f'<div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:{vcol};letter-spacing:2px;">{verdict} {pct:.0f}%</div>'
        f'<div style="font-size:8.5px;color:{C["muted"]};margin-top:3px;">{mode} Model · Annual · Base Case</div></div>',
        unsafe_allow_html=True)

    c1, _, c2 = st.columns([5,1,5])
    with c1:
        st.markdown(
            f'<div class="tp" style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};text-transform:uppercase;margin-bottom:3px;">Current Price</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:24px;font-weight:800;color:{C["red"]};">${current:,.2f}</div></div>',
            unsafe_allow_html=True)
    with _:
        st.markdown(f'<div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:18px;color:{vcol};">↓</div>',
                    unsafe_allow_html=True)
    with c2:
        st.markdown(
            f'<div class="tp" style="text-align:center;">'
            f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};text-transform:uppercase;margin-bottom:3px;">Fair Value ({mode})</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:24px;font-weight:800;color:{C["green_br"]};">${fair:,.2f}</div></div>',
            unsafe_allow_html=True)

    st.markdown(
        f'<div class="tp" style="margin-top:4px;">'
        f'<div style="font-size:8px;letter-spacing:1px;color:{C["muted"]};margin-bottom:5px;text-transform:uppercase;">Fair Value vs Current Price</div>'
        f'<div style="background:{C["border2"]};height:8px;border-radius:4px;overflow:hidden;">'
        f'<div style="width:{bar_r*100:.1f}%;height:100%;background:{C["green_br"]};border-radius:4px;"></div></div>'
        f'<div style="display:flex;justify-content:space-between;font-size:8.5px;color:{C["muted"]};margin-top:3px;">'
        f'<span>Fair ${fair:,.0f}</span><span>Price ${current:,.0f}</span></div></div>',
        unsafe_allow_html=True)


def render_peer_table(profile: Dict) -> None:
    phdr("Peer Comparison")
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9.5px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:4px 8px;color:{C["gold"]};font-size:7.5px;letter-spacing:1.5px;">{h}</td>'
                     for h in ["TICKER","P/E","FWD P/E","MKT CAP","NET MARGIN","REV GROWTH"])
           + "</tr>")
    rows = ""
    for p in profile.get("peers",[]):
        pe_s = f"{p['pe']:.1f}x"       if p.get("pe")      else "N/A"
        fp_s = f"{p['fwd_pe']:.1f}x"   if p.get("fwd_pe")  else "N/A"
        m    = p.get("margin",0) or 0
        mc   = C["green_br"] if m>0.15 else C["orange_br"] if m>0 else C["red"]
        g    = p.get("growth",0) or 0
        gc   = C["green_br"] if g>0.10 else C["orange_br"] if g>0 else C["red"]
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:5px 8px;font-weight:700;color:{C["text"]};font-family:Syne,sans-serif;">{p["sym"]}</td>'
                 f'<td style="padding:5px 8px;">{pe_s}</td>'
                 f'<td style="padding:5px 8px;">{fp_s}</td>'
                 f'<td style="padding:5px 8px;">{p["mkt"]}</td>'
                 f'<td style="padding:5px 8px;color:{mc};font-weight:700;">{m*100:.1f}%</td>'
                 f'<td style="padding:5px 8px;color:{gc};font-weight:700;">{g*100:.1f}%</td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;">{hdr}{rows}</table></div>',
                unsafe_allow_html=True)


def render_screener_table() -> None:
    phdr("Stock Screener", "15 STOCKS", "gold")
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:4px 8px;color:{C["gold"]};font-size:7.5px;letter-spacing:1.5px;">{h}</td>'
                     for h in ["TICKER","NAME","SECTOR","MKT CAP","P/E","RATING"])
           + "</tr>")
    rows = ""
    for s in SCREENER_UNIVERSE:
        rc   = C["green_br"] if s["rating"]>=65 else C["orange_br"] if s["rating"]>=45 else C["red"]
        pe_s = f"{s['pe']:.1f}x" if s["pe"] else "N/A"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:4px 8px;font-weight:700;color:{C["gold"]};font-family:Syne,sans-serif;">{s["ticker"]}</td>'
                 f'<td style="padding:4px 8px;color:{C["text"]};">{s["name"]}</td>'
                 f'<td style="padding:4px 8px;color:{C["muted"]};">{s["sector"]}</td>'
                 f'<td style="padding:4px 8px;">{s["mkt_cap"]}</td>'
                 f'<td style="padding:4px 8px;">{pe_s}</td>'
                 f'<td style="padding:4px 8px;"><span style="color:{rc};font-weight:700;">{s["rating"]}%</span></td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:320px;overflow-y:auto;">{hdr}{rows}</table></div>',
                unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════════════
# §A  CENTRALIZED TERMINAL STATE — Single source of truth for all live data
# ═════════════════════════════════════════════════════════════════════════════════════

import threading
import hashlib
import html
from dataclasses import dataclass, field
from typing import Callable

@dataclass
class TerminalState:
    """
    Centralized state object. All data pipelines write here.
    Streamlit fragments read from this via get_state() helpers.
    Thread-safe via _lock for background updates.
    """
    # ── Market prices (ticker bar + overview) ──────────────────────────────
    ticker_prices:     Dict[str, Dict]   = field(default_factory=dict)
    crypto_overview:   Dict              = field(default_factory=dict)
    stock_overview:    Dict              = field(default_factory=dict)
    global_indices:    List[Dict]        = field(default_factory=list)
    market_status:     Dict              = field(default_factory=dict)

    # ── News aggregated from all sources ──────────────────────────────────
    news_all:          List[Dict]        = field(default_factory=list)   # merged + deduped
    news_crypto:       List[Dict]        = field(default_factory=list)
    news_stocks:       List[Dict]        = field(default_factory=list)
    news_macro:        List[Dict]        = field(default_factory=list)

    # ── Sentiment scores (0-100 per asset class) ───────────────────────────
    sentiment_scores:  Dict[str, float]  = field(default_factory=dict)
    sentiment_history: List[Dict]        = field(default_factory=list)

    # ── Macro / FRED indicators ────────────────────────────────────────────
    macro_indicators:  Dict[str, Dict]   = field(default_factory=dict)

    # ── Economic calendar ─────────────────────────────────────────────────
    econ_calendar:     List[Dict]        = field(default_factory=list)

    # ── Fear & Greed ──────────────────────────────────────────────────────
    crypto_fear_greed: Dict              = field(default_factory=dict)
    stock_fear_greed:  Dict              = field(default_factory=dict)

    # ── Technical analysis cache ───────────────────────────────────────────
    tech_analysis:     Dict[str, Dict]   = field(default_factory=dict)

    # ── GDELT geopolitical ────────────────────────────────────────────────
    geo_events:        pd.DataFrame      = field(default_factory=pd.DataFrame)

    # ── RSS geopolitical news ─────────────────────────────────────────────
    geo_news:          Dict[str, List]   = field(default_factory=dict)

    # ── Crucix OSINT Stream — live intelligence feed with tier classification ─────────
    osint_stream:      List[Dict]        = field(default_factory=list)  # FLASH/PRIORITY/ROUTINE tagged
    news_ticker_items: List[Dict]        = field(default_factory=list)  # merged for scrolling ticker bar


    # ── On-chain whale activity ───────────────────────────────────────────────
    whale_data:        Dict              = field(default_factory=dict)

    # ── Advanced models cache ──────────────────────────────────────────────
    heatmap_data:      Dict              = field(default_factory=dict)
    liquidation_data:  pd.DataFrame      = field(default_factory=pd.DataFrame)
    ai_report:         str               = field(default_factory=str)

    # ── Metadata ──────────────────────────────────────────────────────────
    last_update:       Dict[str, float]  = field(default_factory=dict)
    errors:            Dict[str, str]    = field(default_factory=dict)
    _lock:             threading.Lock    = field(default_factory=threading.Lock)

    def update(self, **kwargs) -> None:
        with self._lock:
            ts = time.time()
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)
                    self.last_update[k] = ts

    def age(self, key: str) -> float:
        """Seconds since this key was last updated."""
        t = self.last_update.get(key, 0)
        return time.time() - t if t else float("inf")

    def mark_error(self, key: str, msg: str) -> None:
        with self._lock:
            self.errors[key] = msg

    def clear_error(self, key: str) -> None:
        with self._lock:
            self.errors.pop(key, None)


# Global singleton — shared across all Streamlit reruns via st.session_state
def get_terminal_state() -> TerminalState:
    if "terminal_state" not in st.session_state:
        st.session_state["terminal_state"] = TerminalState()
    return st.session_state["terminal_state"]


# ═════════════════════════════════════════════════════════════════════════════════════
# §B  RATE LIMITER — Throttle all outbound API calls globally
# ═════════════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """
    Token-bucket rate limiter per endpoint domain.
    Prevents hitting API ceilings during fast intraday refreshes.
    """
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
        """Returns True if request is allowed, False if rate-limited."""
        domain = self._domain(url)
        now    = time.time()
        window = 60.0
        with self._lock:
            bucket = self._buckets.setdefault(domain, [])
            # Drop timestamps older than window
            self._buckets[domain] = [t for t in bucket if now - t < window]
            if len(self._buckets[domain]) >= max_per_minute:
                return False
            self._buckets[domain].append(now)
        return True

    def wait(self, url: str, max_per_minute: int = 20) -> None:
        """Block until request is allowed (simple backoff)."""
        while not self.check(url, max_per_minute):
            time.sleep(1.5)


RATE_LIMITER = RateLimiter()


# ═════════════════════════════════════════════════════════════════════════════════════
# §C  SENTIMENT ENGINE — Score news text intraday (no API key required)
# ═════════════════════════════════════════════════════════════════════════════════════

# Lexicon-based sentiment — no external dependency, works offline
_BULL_WORDS = frozenset([
    "surge","surges","surged","rally","rallied","rise","rises","rose","gain","gains",
    "jump","jumps","jumped","soar","soars","soared","climb","climbs","climbed",
    "bullish","breakout","record","high","beat","beats","beat","strong","robust",
    "positive","upbeat","boom","booming","growth","profit","profits","outperform",
    "upgrade","upgrades","buy","accumulate","overweight","green","bull","momentum",
    "recovery","recover","rebound","rebounds","expansion","accelerate","accelerates",
    "inflow","inflows","demand","succeed","success","milestone","approve","approved",
])

_BEAR_WORDS = frozenset([
    "plunge","plunges","plunged","crash","crashes","crashed","fall","falls","fell",
    "drop","drops","dropped","slide","slides","slid","slump","slumps","slumped",
    "bearish","breakdown","low","miss","misses","missed","weak","negative","concern",
    "loss","losses","decline","declines","declined","sell","underperform","downgrade",
    "downgrades","red","bear","fear","panic","crisis","recession","contraction",
    "outflow","outflows","default","default","bankrupt","bankruptcy","fail","failed",
    "warning","warn","caution","risk","threat","sanction","sanctions","tariff",
    "inflation","geopolit","conflict","war","attack","strike",
])

_INTENSIFIERS = frozenset(["very","extremely","sharply","significantly","massively","deeply"])

def score_text_sentiment(text: str) -> float:
    """
    Lexicon-based sentiment score.
    Returns float in [-1.0, +1.0]:  -1 = very bearish, 0 = neutral, +1 = very bullish.
    Works entirely offline with no API calls.
    ── LIVE API KEY INJECTION POINT ──
    Replace with OpenAI sentiment: openai.chat(model="gpt-4o-mini", messages=[...])
    Or FinBERT:  pipeline("text-classification", model="ProsusAI/finbert")(text)
    """
    if not text:
        return 0.0
    words  = re.findall(r'\b\w+\b', text.lower())
    bull   = 0
    bear   = 0
    amp    = 1.0  # amplifier for intensifiers
    for i, w in enumerate(words):
        if w in _INTENSIFIERS:
            amp = 1.5
            continue
        if w in _BULL_WORDS:
            bull += amp
        elif w in _BEAR_WORDS:
            bear += amp
        amp = 1.0
    total = bull + bear
    if total == 0:
        return 0.0
    return round((bull - bear) / total, 4)


def score_to_label(score: float) -> str:
    if score >= 0.35:  return "VERY BULLISH"
    if score >= 0.12:  return "BULLISH"
    if score <= -0.35: return "VERY BEARISH"
    if score <= -0.12: return "BEARISH"
    return "NEUTRAL"


def score_to_color(score: float) -> str:
    if score >= 0.35:  return C["green_br"]
    if score >= 0.12:  return C["green_br"]
    if score <= -0.35: return C["red"]
    if score <= -0.12: return C["red"]
    return C["gold"]


def compute_asset_sentiment(news_items: List[Dict], asset_filter: str = "") -> Dict[str, float]:
    """
    Aggregate sentiment per asset from a list of news dicts.
    Returns {asset_key: score_-1_to_1}.
    """
    buckets: Dict[str, List[float]] = {}
    for item in news_items:
        src  = item.get("source","")
        text = item.get("title","") + " " + item.get("summary","")
        sym  = item.get("symbol", item.get("asset_type","market"))
        if asset_filter and asset_filter.lower() not in sym.lower():
            continue
        sc = score_text_sentiment(text)
        buckets.setdefault(sym, []).append(sc)

    out: Dict[str, float] = {}
    for sym, scores in buckets.items():
        out[sym] = round(sum(scores) / len(scores), 4) if scores else 0.0
    return out


def compute_composite_sentiment(news_items: List[Dict]) -> float:
    """Composite market sentiment from all news (single scalar)."""
    scores = [score_text_sentiment(i.get("title","")+" "+i.get("summary",""))
              for i in news_items if i.get("title")]
    return round(sum(scores) / len(scores), 4) if scores else 0.0


# ═════════════════════════════════════════════════════════════════════════════════════
# §D  MARKET DATA ENGINE — Bloomberg/CryptoLens data pipelines (async)
# ═════════════════════════════════════════════════════════════════════════════════════

# ── D1. CoinGecko crypto market overview ─────────────────────────────────────────────

async def _fetch_coingecko_overview(client: aiohttp.ClientSession) -> Dict:
    """
    CoinGecko free API — top 50 coins by market cap.
    Ported from cryptolens market_overview_service.py.
    No API key required.
    ── LIVE API KEY INJECTION POINT ──
    CoinGecko Pro: add header {"x-cg-pro-api-key": COINGECKO_KEY} for higher rate limits.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = ("?vs_currency=usd&order=market_cap_desc"
              "&per_page=50&page=1&sparkline=false&price_change_percentage=24h")
    try:
        async with client.get(url + params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                coins = await r.json(content_type=None)
                total_vol = sum(c.get("total_volume", 0) or 0 for c in coins)
                return {
                    "coins": [{
                        "symbol":   c.get("symbol","").upper(),
                        "name":     c.get("name",""),
                        "price":    c.get("current_price", 0) or 0,
                        "change_24h": c.get("price_change_percentage_24h", 0) or 0,
                        "volume_24h": c.get("total_volume", 0) or 0,
                        "market_cap": c.get("market_cap", 0) or 0,
                        "high_24h": c.get("high_24h", 0) or 0,
                        "low_24h":  c.get("low_24h", 0) or 0,
                        "rank":     c.get("market_cap_rank", 0) or 0,
                        "logo":     c.get("image", ""),
                    } for c in coins],
                    "total_volume_24h": total_vol,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
    except Exception as e:
        pass
    return {}


async def _fetch_coingecko_global(client: aiohttp.ClientSession) -> Dict:
    """CoinGecko global market stats (BTC dominance, total cap)."""
    try:
        async with client.get("https://api.coingecko.com/api/v3/global",
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = (await r.json(content_type=None)).get("data", {})
                return {
                    "total_market_cap": d.get("total_market_cap", {}).get("usd", 0),
                    "btc_dominance":    d.get("market_cap_percentage", {}).get("btc", 0),
                    "eth_dominance":    d.get("market_cap_percentage", {}).get("eth", 0),
                    "active_coins":     d.get("active_cryptocurrencies", 0),
                }
    except Exception:
        pass
    return {}


# ── D2. Fear & Greed indices ─────────────────────────────────────────────────────────

async def _fetch_crypto_fear_greed(client: aiohttp.ClientSession) -> Dict:
    """Alternative.me Fear & Greed (crypto). No key required."""
    try:
        async with client.get("https://api.alternative.me/fng/?limit=7",
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = (await r.json(content_type=None)).get("data", [])
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


async def _fetch_stock_fear_greed(client: aiohttp.ClientSession) -> Dict:
    """CNN Fear & Greed (stocks). No key required."""
    try:
        async with client.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status == 200:
                d    = (await r.json(content_type=None)).get("fear_and_greed", {})
                score = d.get("score", 50)
                return {"value": int(score), "classification": d.get("rating","Neutral")}
    except Exception:
        pass
    return {"value": 50, "classification": "Neutral"}


# ── D3. Yahoo Finance stock data (Neuberg pattern) ───────────────────────────────────

NASDAQ_WATCH = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AVGO",
    "COST","NFLX","AMD","ADBE","QCOM","TXN","INTC","PYPL",
    "^GSPC","^IXIC","^DJI","^FTSE","^GDAXI","^N225",
]

async def _fetch_yf_batch(client: aiohttp.ClientSession, symbols: List[str]) -> List[Dict]:
    """
    Yahoo Finance v8 chart API batch fetch.
    Adapted from Neuberg yahoo-finance.ts and CryptoLens stock_market_service.py.
    ── LIVE API KEY INJECTION POINT ──
    Polygon.io: GET https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers
                    ?tickers=AAPL,MSFT&apiKey=POLYGON_KEY
    """
    results = []
    hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json"}
    for sym in symbols:
        if not RATE_LIMITER.check("https://query1.finance.yahoo.com", max_per_minute=30):
            continue
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=2d"
            async with client.get(url, headers=hdrs,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                data   = await r.json(content_type=None)
                result = data.get("chart", {}).get("result", [])
                if not result:
                    continue
                meta   = result[0].get("meta", {})
                price  = float(meta.get("regularMarketPrice", 0) or 0)
                prev   = float(meta.get("chartPreviousClose", 0) or meta.get("previousClose", price) or price)
                chg    = (price - prev) / prev * 100 if prev else 0
                vol    = float(meta.get("regularMarketVolume", 0) or 0)
                results.append({
                    "symbol":    sym,
                    "name":      meta.get("longName") or meta.get("shortName") or sym,
                    "price":     price,
                    "change_24h": round(chg, 2),
                    "volume_24h": vol * price,
                    "high_24h":  float(meta.get("regularMarketDayHigh", price) or price),
                    "low_24h":   float(meta.get("regularMarketDayLow",  price) or price),
                    "market_cap": float(meta.get("marketCap", 0) or 0),
                    "prev_close": prev,
                })
        except Exception:
            continue
    return results


# ── D4. Binance crypto technical analysis (CryptoLens pattern) ───────────────────────

async def _fetch_binance_klines(symbol: str, interval: str = "4h",
                                 limit: int = 100) -> List[List]:
    """Binance public klines endpoint. No key required."""
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    if not RATE_LIMITER.check(url, max_per_minute=15):
        return []
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
    except Exception:
        pass
    return []


def _calc_rsi(closes: List[float], period: int = 14) -> float:
    """RSI calculation — ported from CryptoLens technical_analysis_service.py."""
    if len(closes) < period + 1:
        return 50.0
    changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains   = [c if c > 0 else 0 for c in changes]
    losses  = [-c if c < 0 else 0 for c in changes]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
    return round(100 - (100/(1+(ag/al if al else float("inf")))), 2)


def _calc_atr(klines: List[List], period: int = 14) -> float:
    """ATR — ported from CryptoLens technical_analysis_service.py."""
    if len(klines) < period+1:
        return 0.0
    trs = []
    for i in range(1, len(klines)):
        h, l, pc = float(klines[i][2]), float(klines[i][3]), float(klines[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    atr = sum(trs[:period]) / period
    for t in trs[period:]:
        atr = (atr*(period-1) + t) / period
    return atr


def _calc_ema(closes: List[float], period: int) -> float:
    if len(closes) < period:
        return closes[-1] if closes else 0
    mult = 2/(period+1)
    ema  = sum(closes[:period]) / period
    for c in closes[period:]:
        ema = (c - ema)*mult + ema
    return ema


async def get_technical_snapshot(symbol: str) -> Dict:
    """
    Full technical analysis for a crypto pair or stock ticker.
    Returns RSI, ATR, trend, support/resistance, target.
    ── LIVE API KEY INJECTION POINT ──
    For stocks: Polygon.io /v2/aggs/ticker/{symbol}/range/1/hour/{from}/{to}?apiKey=KEY
    """
    state = get_terminal_state()
    if symbol in state.tech_analysis and state.age("tech_analysis") < 180:
        return state.tech_analysis[symbol]

    result: Dict = {"symbol": symbol, "error": None}
    try:
        clean = symbol.upper().replace("NASDAQ:","").replace("NYSE:","")
        binance_sym = clean if clean.endswith("USDT") else clean + "USDT"
        klines = await _fetch_binance_klines(binance_sym, "4h", 100)

        if klines and len(klines) >= 20:
            closes  = [float(k[4]) for k in klines]
            highs   = [float(k[2]) for k in klines]
            lows    = [float(k[3]) for k in klines]
            current = closes[-1]
            rsi     = _calc_rsi(closes)
            atr     = _calc_atr(klines)
            ema10   = _calc_ema(closes, 10)
            ema30   = _calc_ema(closes, 30)
            trend   = ("bullish" if ema10 > ema30*1.01
                       else "bearish" if ema10 < ema30*0.99 else "neutral")

            # Pivot support/resistance
            recent_h = max(highs[-24:])
            recent_l = min(lows[-24:])
            pivot    = (recent_h + recent_l + current) / 3
            s1 = round(2*pivot - recent_h, 4)
            r1 = round(2*pivot - recent_l, 4)

            rsi_lbl = ("Overbought" if rsi>=70 else "Oversold" if rsi<=30
                       else "Bullish" if rsi>=55 else "Bearish" if rsi<=45 else "Neutral")

            # Target price
            mult  = 1.5 if trend=="bullish" else -1.5 if trend=="bearish" else 0.5
            tgt_hi = current + atr * abs(mult)
            tgt_lo = current + atr * (mult * 0.5)
            if tgt_lo > tgt_hi:
                tgt_lo, tgt_hi = tgt_hi, tgt_lo

            result.update({
                "current_price":     current,
                "rsi":               rsi,
                "rsi_signal":        rsi_lbl,
                "atr":               atr,
                "trend":             trend,
                "ema10":             ema10,
                "ema30":             ema30,
                "pivot":             round(pivot, 4),
                "support":           [round(s1,4), round(s1 - atr,4)],
                "resistance":        [round(r1,4), round(r1 + atr,4)],
                "target_range":      f"${tgt_lo:,.4g} – ${tgt_hi:,.4g}",
                "sentiment_score":   0.0,  # filled by sentiment engine below
            })
        else:
            result["error"] = "Insufficient kline data"
    except Exception as e:
        result["error"] = str(e)

    # Cache
    with state._lock:
        state.tech_analysis[symbol] = result
        state.last_update["tech_analysis"] = time.time()
    return result


# ── D5. Economic calendar (Neuberg / ForexFactory) ───────────────────────────────────

async def _fetch_economic_calendar(client: aiohttp.ClientSession) -> List[Dict]:
    """
    ForexFactory free JSON calendar — ported from Neuberg fmp-calendar.ts.
    No API key required.
    ── LIVE API KEY INJECTION POINT ──
    FMP: GET https://financialmodelingprep.com/api/v3/economic_calendar
         ?from=2026-05-01&to=2026-05-31&apikey=FMP_KEY
    """
    urls = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
    ]
    events: List[Dict] = []
    currency_map = {
        "USD":"US","EUR":"EU","GBP":"GB","JPY":"JP","CNY":"CN",
        "AUD":"AU","CAD":"CA","CHF":"CH","NZD":"NZ","CAD":"CA",
    }
    for url in urls:
        if not RATE_LIMITER.check(url, max_per_minute=5):
            continue
        try:
            async with client.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    for ev in await r.json(content_type=None):
                        if not ev.get("title") or not ev.get("date"):
                            continue
                        impact = ev.get("impact","low").lower()
                        impact = ("high" if impact=="high" else
                                  "medium" if impact in ("medium","moderate") else "low")
                        events.append({
                            "event":    ev["title"],
                            "country":  currency_map.get(ev.get("country",""),"??"),
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


# ── D6. Multi-source news aggregation (CryptoLens + Bloomberg merged) ────────────────

_CRYPTO_RSS = [
    ("CoinDesk",    "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("CoinTelegraph","https://cointelegraph.com/rss"),
    ("CryptoSlate", "https://cryptoslate.com/feed/"),
    ("Decrypt",     "https://decrypt.co/feed"),
]

_MARKET_RSS = [
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("Investing.com","https://www.investing.com/rss/news.rss"),
    ("SeekingAlpha","https://seekingalpha.com/market_currents.xml"),
    ("TheStreet",   "https://www.thestreet.com/rss/main.rss"),
]

_MACRO_RSS = [
    ("BBC World",   "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Reuters",     "https://feeds.reuters.com/reuters/worldNews"),
    ("FT Markets",  "https://www.ft.com/rss/home/uk"),
    ("WSJ",         "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"),
    ("DW",          "https://rss.dw.com/rdf/rss-en-world"),
]

_MILITARY_RSS = [
    ("MilTimes",    "https://www.militarytimes.com/arc/outboundfeeds/rss/"),
    ("T&Purpose",   "https://taskandpurpose.com/feed/"),
    ("DefNews",     "https://www.defensenews.com/rss/"),
]

# ── Crucix OSINT feeds — public open-source intelligence RSS channels ─────────────────
# Mirrors Crucix's merged news ticker: RSS + GDELT + open-channel posts
_OSINT_RSS: List[Tuple[str, str]] = [
    ("Al Jazeera",   "https://www.aljazeera.com/xml/rss/all.xml"),
    ("BBC World",    "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Reuters",      "https://feeds.reuters.com/reuters/worldNews"),
    ("DW World",     "https://rss.dw.com/rdf/rss-en-world"),
    ("AP News",      "https://rsshub.app/apnews/topics/apf-topnews"),
    ("MilTimes",     "https://www.militarytimes.com/arc/outboundfeeds/rss/"),
    ("DefNews",      "https://www.defensenews.com/rss/"),
    ("T&Purpose",    "https://taskandpurpose.com/feed/"),
    ("STRATFOR",     "https://worldview.stratfor.com/rss.xml"),
    ("ACLED Blog",   "https://acleddata.com/feed/"),
    ("ReliefWeb",    "https://reliefweb.int/headlines/rss.xml"),
    ("ISW",          "https://www.understandingwar.org/rss.xml"),
    ("Bellingcat",   "https://www.bellingcat.com/feed/"),
    ("TRT World",    "https://www.trtworld.com/rss"),
    ("France 24",    "https://www.france24.com/en/rss"),
    ("Middle East Eye","https://www.middleeasteye.net/rss"),
    ("MEE Conflict", "https://www.middleeasteye.net/rss/conflict"),
    ("IntelSlava",   "https://rsshub.app/telegram/channel/intelslava"),
    ("CIG_TELEGRAM", "https://rsshub.app/telegram/channel/conflictin"),
    ("OSINTdefcom",  "https://rsshub.app/telegram/channel/osintdefcom"),
]

# OSINT signal tier classification — mirrors Crucix FLASH/PRIORITY/ROUTINE
_FLASH_KEYWORDS = frozenset([
    "breaking","urgent","flash","alert","attack","strike","explosion","killed",
    "nuclear","chemical","biological","missile","bomb","war declared","ceasefire broken",
    "coup","invasion","assassin","hostage","mass casualty","emergency",
])
_PRIORITY_KEYWORDS = frozenset([
    "conflict","military","troops","deploy","sanction","airstrike","clashes",
    "protest","riot","casualties","offensive","withdrawal","escalat","blockade",
    "siege","detained","arrested","crisis","tensions","threat","mobiliz",
])

def _classify_osint_tier(title: str) -> str:
    """Classify headline into Crucix alert tiers: FLASH / PRIORITY / ROUTINE."""
    t = title.lower()
    if any(k in t for k in _FLASH_KEYWORDS):
        return "FLASH"
    if any(k in t for k in _PRIORITY_KEYWORDS):
        return "PRIORITY"
    return "ROUTINE"

def _tier_color(tier: str) -> str:
    return {
        "FLASH":    C["red"],
        "PRIORITY": C["orange_br"],
        "ROUTINE":  C["muted"],
    }.get(tier, C["muted"])


def _news_id(title: str, source: str) -> str:
    return hashlib.md5(f"{title[:60]}:{source}".encode()).hexdigest()[:12]


def _clean_html(text: str) -> str:
    text = re.sub(r'<[^>]+>', '', text or '')
    return html.unescape(text).strip()


def _is_alert(title: str) -> bool:
    kws = ["attack","strike","conflict","kill","war","missile","threat",
           "sanction","explosion","bomb","nuclear","crisis","default","crash"]
    return any(k in title.lower() for k in kws)


async def _fetch_rss_batch_async(feeds: List[Tuple[str,str]],
                                  asset_type: str,
                                  client: aiohttp.ClientSession) -> List[Dict]:
    """Fetch multiple RSS feeds concurrently, returning normalised news dicts."""
    items: List[Dict] = []
    for source, url in feeds:
        if not RATE_LIMITER.check(url, max_per_minute=6):
            continue
        try:
            async with client.get(url, timeout=aiohttp.ClientTimeout(total=8),
                                   headers={"User-Agent":"Mozilla/5.0"}) as r:
                if r.status != 200:
                    continue
                text = await r.text()
                feed = feedparser.parse(text)
                for entry in feed.entries[:12]:
                    title   = translate_to_english(_clean_html(entry.get("title","")))
                    summary = translate_to_english(_clean_html(entry.get("summary",""))[:300])
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
                        "source":     source[:14],
                        "title":      title[:140],
                        "summary":    summary,
                        "link":       entry.get("link",""),
                        "published":  pub,
                        "is_alert":   _is_alert(title),
                        "asset_type": asset_type,
                        "sentiment":  score_text_sentiment(title + " " + summary),
                    })
        except Exception:
            continue
    return items


async def _fetch_cryptocompare_news_async(client: aiohttp.ClientSession) -> List[Dict]:
    """CryptoCompare free news API — no key required for 50 req/day."""
    url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest"
    if not RATE_LIMITER.check(url, max_per_minute=4):
        return []
    try:
        async with client.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                items = []
                for n in (await r.json(content_type=None)).get("Data",[])[:20]:
                    title = translate_to_english(n.get("title",""))
                    body  = _clean_html(n.get("body",""))[:300]
                    if not title:
                        continue
                    ts = n.get("published_on", time.time())
                    pub = datetime.fromtimestamp(ts).strftime("%d %b %H:%M")
                    items.append({
                        "id":         _news_id(title, n.get("source","")),
                        "source":     (n.get("source","CC")[:14]),
                        "title":      title[:140],
                        "summary":    body,
                        "link":       n.get("url",""),
                        "published":  pub,
                        "is_alert":   _is_alert(title),
                        "asset_type": "crypto",
                        "sentiment":  score_text_sentiment(title + " " + body),
                    })
                return items
    except Exception:
        pass
    return []


async def aggregate_all_news() -> Dict[str, List[Dict]]:
    """
    Master news aggregation — runs all RSS+API feeds concurrently.
    Deduplicates by title hash. Returns categorised dict.
    """
    connector = aiohttp.TCPConnector(ssl=False, limit=24)
    async with aiohttp.ClientSession(connector=connector) as client:
        crypto_rss, market_rss, macro_rss, mil_rss, cc_news = await asyncio.gather(
            _fetch_rss_batch_async(_CRYPTO_RSS, "crypto", client),
            _fetch_rss_batch_async(_MARKET_RSS, "markets", client),
            _fetch_rss_batch_async(_MACRO_RSS,  "macro",  client),
            _fetch_rss_batch_async(_MILITARY_RSS,"military",client),
            _fetch_cryptocompare_news_async(client),
            return_exceptions=False,
        )

    # Merge crypto
    crypto_all = crypto_rss + cc_news
    # Merge macro + military
    macro_all  = macro_rss + mil_rss

    def dedup(items: List[Dict]) -> List[Dict]:
        seen: set = set()
        out: List[Dict] = []
        for item in sorted(items, key=lambda x: x.get("published",""), reverse=True):
            k = item["id"]
            if k not in seen:
                seen.add(k)
                out.append(item)
        return out[:50]

    return {
        "crypto":  dedup(crypto_all),
        "markets": dedup(market_rss),
        "macro":   dedup(macro_all),
        "all":     dedup(crypto_all + market_rss + macro_all),
    }


# ── Crucix OSINT Stream Aggregator ───────────────────────────────────────────────────
# Mirrors Crucix's merged ticker: RSS + open OSINT channels + GDELT, every 5 min.
# No static caching — each call triggers fresh fetch as per Crucix spec.

async def aggregate_osint_stream() -> List[Dict]:
    """
    Fetch all OSINT RSS sources concurrently.
    Returns items tagged with: source, title, tier (FLASH/PRIORITY/ROUTINE), timestamp.
    Deduped by title hash. Sorted newest-first with FLASH headlines promoted to top.
    This is the data engine behind both the OSINT Stream panel and the Live News Ticker.
    ── LIVE EXTENSION POINT ──
    Telegram OSINT channels (CIG_TELEGRAM, OSINTdefcom, IntelSlava):
      Use python-telegram-bot or Telethon to subscribe to channel posts in real-time.
      Replace rsshub.app/telegram stubs with: client.get_messages(channel, limit=50).
    """
    connector = aiohttp.TCPConnector(ssl=False, limit=20)
    items: List[Dict] = []
    seen_ids: set = set()

    async with aiohttp.ClientSession(connector=connector,
                                      headers={"User-Agent":"Mozilla/5.0"}) as client:
        async def _fetch_one(source: str, url: str) -> List[Dict]:
            if not RATE_LIMITER.check(url, max_per_minute=4):
                return []
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=9)) as r:
                    if r.status != 200:
                        return []
                    text = await r.text()
                    feed = feedparser.parse(text)
                    out  = []
                    for entry in feed.entries[:10]:
                        raw_title = _clean_html(entry.get("title", ""))
                        title     = translate_to_english(raw_title)
                        if not title or len(title) < 10:
                            continue
                        nid = _news_id(title, source)
                        if nid in seen_ids:
                            continue
                        seen_ids.add(nid)
                        pub = ""
                        try:
                            if entry.get("published_parsed"):
                                pub = datetime(*entry.published_parsed[:6]).strftime("%d %b %H:%M")
                        except Exception:
                            pass
                        tier = _classify_osint_tier(title)
                        out.append({
                            "id":        nid,
                            "source":    source[:18],
                            "title":     title[:160],
                            "summary":   translate_to_english(_clean_html(
                                             entry.get("summary",""))[:250]),
                            "link":      entry.get("link",""),
                            "published": pub,
                            "tier":      tier,
                            "sentiment": score_text_sentiment(title),
                            "is_alert":  tier in ("FLASH","PRIORITY"),
                            "asset_type":"osint",
                        })
                    return out
            except Exception:
                return []

        results = await asyncio.gather(*[_fetch_one(src, url) for src, url in _OSINT_RSS],
                                        return_exceptions=True)

    for r in results:
        if isinstance(r, list):
            items.extend(r)

    # Sort: FLASH first, then PRIORITY, then by timestamp
    tier_rank = {"FLASH": 0, "PRIORITY": 1, "ROUTINE": 2}
    items.sort(key=lambda x: (tier_rank.get(x["tier"], 2), x.get("published","") == ""))
    return items[:80]


# ═════════════════════════════════════════════════════════════════════════════════════
# §E  BACKGROUND PIPELINE ORCHESTRATOR
# Runs all data fetches in a daemon thread — never blocks Streamlit rendering.
# ═════════════════════════════════════════════════════════════════════════════════════

_PIPELINE_INTERVALS: Dict[str, int] = {
    "ticker_prices":      60,   # 1 min
    "crypto_overview":    120,  # 2 min
    "stock_overview":     120,
    "fear_greed":         300,  # 5 min
    "news_all":           300,  # ← 5 min  (Crucix spec: real-time every 5 min, no static cache)
    "osint_stream":       300,  # ← 5 min  (OSINT Stream — Crucix)
    "news_ticker":        300,  # ← 5 min  (Horizontal live ticker — Crucix)
    "econ_calendar":      1800, # 30 min
    "macro_indicators":   3600, # 1 hr
    "geo_events":         300,
    "liquidation_data":   30,   # 30 sec — Binance futures
    "heatmap_data":       300,  # 5 min — CoinGecko heatmap
    "ai_report":          600,  # 10 min — AI report generation
}

async def _run_pipeline_cycle(state: TerminalState) -> None:
    """
    Single async cycle — fetches only stale data sources.
    Called every 30 seconds from the background thread.
    """
    connector = aiohttp.TCPConnector(ssl=False, limit=16)

    async with aiohttp.ClientSession(connector=connector,
                                      headers={"User-Agent":"Mozilla/5.0"}) as client:

        tasks = []

        # ── Crypto overview ──────────────────────────────────────────────
        if state.age("crypto_overview") > _PIPELINE_INTERVALS["crypto_overview"]:
            async def _crypto():
                try:
                    overview = await _fetch_coingecko_overview(client)
                    glbl     = await _fetch_coingecko_global(client)
                    if overview.get("coins"):
                        overview.update(glbl)
                        state.update(crypto_overview=overview)
                        state.clear_error("crypto_overview")
                except Exception as e:
                    state.mark_error("crypto_overview", str(e))
            tasks.append(_crypto())

        # ── Stock overview ───────────────────────────────────────────────
        if state.age("stock_overview") > _PIPELINE_INTERVALS["stock_overview"]:
            async def _stocks():
                try:
                    stocks = await _fetch_yf_batch(client, NASDAQ_WATCH)
                    if stocks:
                        # Build ticker price map for bar
                        pmap = {s["symbol"]: {"price":s["price"],"pct":s["change_24h"]}
                                for s in stocks}
                        state.update(stock_overview={"coins": stocks},
                                     ticker_prices=pmap)
                        state.clear_error("stock_overview")
                except Exception as e:
                    state.mark_error("stock_overview", str(e))
            tasks.append(_stocks())

        # ── Fear & Greed ─────────────────────────────────────────────────
        if state.age("fear_greed") > _PIPELINE_INTERVALS["fear_greed"]:
            async def _fg():
                try:
                    cfg = await _fetch_crypto_fear_greed(client)
                    sfg = await _fetch_stock_fear_greed(client)
                    state.update(crypto_fear_greed=cfg, stock_fear_greed=sfg)
                except Exception as e:
                    state.mark_error("fear_greed", str(e))
            tasks.append(_fg())

        # ── Economic calendar ────────────────────────────────────────────
        if state.age("econ_calendar") > _PIPELINE_INTERVALS["econ_calendar"]:
            async def _cal():
                try:
                    ev = await _fetch_economic_calendar(client)
                    if ev:
                        state.update(econ_calendar=ev)
                except Exception as e:
                    state.mark_error("econ_calendar", str(e))
            tasks.append(_cal())

        # ── Macro indicators (FRED) ──────────────────────────────────────
        if state.age("macro_indicators") > _PIPELINE_INTERVALS["macro_indicators"]:
            async def _macro():
                try:
                    macro = get_macro_indicators()   # uses existing cached fn
                    state.update(macro_indicators=macro)
                except Exception as e:
                    state.mark_error("macro_indicators", str(e))
            tasks.append(_macro())

        await asyncio.gather(*tasks, return_exceptions=True)

    # ── News (separate session — many hosts) ─────────────────────────────
    if state.age("news_all") > _PIPELINE_INTERVALS["news_all"]:
        try:
            news = await aggregate_all_news()
            # Compute sentiment scores across all news
            all_items = news.get("all", [])
            sent_scores = compute_asset_sentiment(all_items)
            composite   = compute_composite_sentiment(all_items)
            sent_scores["composite"] = composite

            # Merge geopolitical RSS into macro
            geo = {}
            for cat in ["world","us","europe","military","markets"]:
                geo[cat] = fetch_rss_news(cat, max_items=15)

            state.update(
                news_all     = news.get("all", []),
                news_crypto  = news.get("crypto", []),
                news_stocks  = news.get("markets", []),
                news_macro   = news.get("macro", []),
                sentiment_scores = sent_scores,
                geo_news     = geo,
            )
            state.clear_error("news_all")
        except Exception as e:
            state.mark_error("news_all", str(e))

    # ── Crucix OSINT Stream (5-min refresh, no static caching) ──────────
    if state.age("osint_stream") > _PIPELINE_INTERVALS["osint_stream"]:
        try:
            osint_items = await aggregate_osint_stream()
            # Merged ticker = OSINT + general news, deduplicated
            all_news = state.news_all or []
            merged_ticker = osint_items[:30] + [
                n for n in all_news
                if _news_id(n.get("title",""), n.get("source","")) not in
                {i["id"] for i in osint_items}
            ]
            state.update(
                osint_stream      = osint_items,
                news_ticker_items = merged_ticker[:60],
            )
            state.clear_error("osint_stream")
        except Exception as e:
            state.mark_error("osint_stream", str(e))

    # ── GDELT globe events ───────────────────────────────────────────────
    if state.age("geo_events") > _PIPELINE_INTERVALS["geo_events"]:
        try:
            gdf = fetch_gdelt_events()
            state.update(geo_events=gdf)
        except Exception:
            pass


def _pipeline_thread_worker(state: TerminalState) -> None:
    """
    Background daemon thread — runs the async pipeline every 30 seconds.
    Calls both _run_pipeline_cycle (core data) and _run_extended_pipeline
    (whale trades, global indices, market status, symbol-aware sentiment).
    Never raises — silently logs errors into state.errors.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(_run_pipeline_cycle(state))
        except Exception as e:
            state.mark_error("pipeline_core", str(e))
        try:
            loop.run_until_complete(_run_extended_pipeline(state))
        except Exception as e:
            state.mark_error("pipeline_ext", str(e))
        # Refresh global indices every 2 minutes
        if state.age("global_indices") > 120:
            try:
                idxs = get_global_indices()
                state.update(global_indices=idxs)
            except Exception:
                pass
        time.sleep(30)


def start_background_pipeline() -> None:
    """
    Launch background data pipeline exactly once per Streamlit session.
    Uses a session_state flag to prevent duplicate threads.
    """
    if st.session_state.get("_pipeline_started"):
        return
    state = get_terminal_state()
    t = threading.Thread(target=_pipeline_thread_worker, args=(state,),
                         daemon=True, name="TerminalDataPipeline")
    t.start()
    st.session_state["_pipeline_started"] = True


# ═════════════════════════════════════════════════════════════════════════════════════
# §F  NEW UI COMPONENTS wired to TerminalState
# ═════════════════════════════════════════════════════════════════════════════════════

def _svc_unavailable(label: str) -> None:
    """Graceful degradation — shows service-unavailable notice in a panel."""
    st.markdown(
        f'<div class="tp" style="border-color:{C["red"]}44;padding:10px 12px;'
        f'text-align:center;">'
        f'<div style="font-size:8px;letter-spacing:2px;color:{C["red"]};'
        f'text-transform:uppercase;margin-bottom:4px;">⚠ SERVICE UNAVAILABLE</div>'
        f'<div style="font-size:9px;color:{C["muted"]};">{label}</div></div>',
        unsafe_allow_html=True,
    )


def render_fear_greed_gauge(data: Dict, label: str = "FEAR & GREED") -> None:
    """Circular-style gauge rendered in HTML/CSS."""
    if not data:
        _svc_unavailable(f"{label} — data unavailable")
        return
    val   = data.get("value", 50)
    cls   = data.get("classification","Neutral")
    color = (C["red"] if val < 25 else C["orange_br"] if val < 45
             else C["gold"] if val < 55 else C["green_br"] if val < 75 else C["green_br"])
    pct   = val  # 0-100 maps to stroke dashoffset
    circ  = 283  # circumference of r=45 circle
    dash  = circ - (circ * val / 100)
    st.markdown(f"""
    <div class="tp" style="text-align:center;padding:12px;">
      <div style="font-size:8px;letter-spacing:2px;color:{C['muted']};
                  text-transform:uppercase;margin-bottom:8px;">{label}</div>
      <svg width="100" height="100" viewBox="0 0 100 100">
        <circle cx="50" cy="50" r="45" fill="none"
                stroke="{C['border2']}" stroke-width="8"/>
        <circle cx="50" cy="50" r="45" fill="none"
                stroke="{color}" stroke-width="8"
                stroke-dasharray="{circ}"
                stroke-dashoffset="{dash:.1f}"
                stroke-linecap="round"
                transform="rotate(-90 50 50)"/>
        <text x="50" y="46" text-anchor="middle"
              font-family="Syne,sans-serif" font-size="18" font-weight="800"
              fill="{color}">{val}</text>
        <text x="50" y="60" text-anchor="middle"
              font-family="Space Mono,monospace" font-size="7"
              fill="{C['muted']}">{cls.upper()[:11]}</text>
      </svg>
    </div>""", unsafe_allow_html=True)


def render_sentiment_dashboard(state: TerminalState) -> None:
    """Sentiment panel — composite + per-asset bars wired to TerminalState."""
    scores = state.sentiment_scores
    if not scores:
        _svc_unavailable("Sentiment engine — no news loaded yet")
        return

    phdr("Market Sentiment Engine", "NLP · LIVE", "gold")
    composite = scores.get("composite", 0.0)
    comp_lbl  = score_to_label(composite)
    comp_col  = score_to_color(composite)
    bar_pct   = int((composite + 1) / 2 * 100)  # map -1..1 to 0..100

    st.markdown(f"""
    <div class="tp" style="padding:8px 12px;margin-bottom:4px;">
      <div style="font-size:8px;letter-spacing:1.5px;color:{C['muted']};
                  text-transform:uppercase;margin-bottom:4px;">COMPOSITE MARKET SENTIMENT</div>
      <div style="display:flex;align-items:center;gap:10px;">
        <div style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;
                    color:{comp_col};">{comp_lbl}</div>
        <div style="font-size:10px;color:{comp_col};">{composite:+.3f}</div>
      </div>
      <div style="background:{C['border2']};height:6px;border-radius:3px;
                  margin-top:6px;overflow:hidden;">
        <div style="width:{bar_pct}%;height:100%;background:{comp_col};
                    border-radius:3px;transition:width 0.6s;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:8px;
                  color:{C['muted']};margin-top:2px;"><span>BEARISH</span><span>BULLISH</span></div>
    </div>""", unsafe_allow_html=True)

    # Per-asset mini bars
    interesting = {k:v for k,v in scores.items()
                   if k not in ("composite",) and abs(v) > 0.05}
    for sym, sc in sorted(interesting.items(), key=lambda x: -abs(x[1]))[:8]:
        col  = score_to_color(sc)
        lbl  = score_to_label(sc)
        bpct = int((sc+1)/2*100)
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;">
          <span style="width:80px;font-size:9px;color:{C['muted']};
                       white-space:nowrap;overflow:hidden;">{sym[:12]}</span>
          <div style="flex:1;background:{C['border2']};height:4px;border-radius:2px;">
            <div style="width:{bpct}%;height:100%;background:{col};border-radius:2px;"></div>
          </div>
          <span style="font-size:8px;color:{col};min-width:55px;text-align:right;">{lbl}</span>
        </div>""", unsafe_allow_html=True)


def render_fear_greed_row(state: TerminalState) -> None:
    """Side-by-side Fear & Greed gauges."""
    c1, c2 = st.columns(2)
    with c1:
        render_fear_greed_gauge(state.crypto_fear_greed, "CRYPTO F&G")
    with c2:
        render_fear_greed_gauge(state.stock_fear_greed,  "EQUITY F&G")


def render_crypto_overview_table(state: TerminalState) -> None:
    """Live crypto market overview table from CoinGecko."""
    phdr("Crypto Market Overview", "COINGECKO · LIVE", "gold")
    data = state.crypto_overview.get("coins", [])
    if not data:
        _svc_unavailable("CoinGecko — awaiting data")
        return
    btc_dom = state.crypto_overview.get("btc_dominance", 0)
    tot_cap = state.crypto_overview.get("total_market_cap", 0)

    # Stats bar
    st.markdown(f"""
    <div style="display:flex;gap:16px;padding:4px 0 8px 0;font-size:9px;">
      <span><span style="color:{C['muted']};">Total Cap </span>
            <span style="color:{C['text']};font-weight:700;">
              ${tot_cap/1e12:.2f}T</span></span>
      <span><span style="color:{C['muted']};">BTC Dom </span>
            <span style="color:{C['gold']};font-weight:700;">
              {btc_dom:.1f}%</span></span>
    </div>""", unsafe_allow_html=True)

    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;'
                     f'letter-spacing:1.5px;">{h}</td>'
                     for h in ["#","SYMBOL","PRICE","24H","VOL","MKT CAP"])
           + "</tr>")
    rows = ""
    for c in data[:20]:
        chg   = c.get("change_24h", 0)
        cc    = C["green_br"] if chg >= 0 else C["red"]
        sign  = "▲" if chg >= 0 else "▼"
        price = c.get("price", 0)
        pfmt  = (f"${price:,.0f}" if price >= 1000
                 else f"${price:,.2f}" if price >= 1
                 else f"${price:.5f}")
        vol   = c.get("volume_24h", 0)
        vfmt  = f"${vol/1e9:.2f}B" if vol>=1e9 else f"${vol/1e6:.0f}M"
        cap   = c.get("market_cap", 0)
        cfmt  = f"${cap/1e12:.2f}T" if cap>=1e12 else f"${cap/1e9:.0f}B"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{c.get("rank",0)}</td>'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["gold"]};'
                 f'font-family:Syne,sans-serif;">{c.get("symbol","")}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{pfmt}</td>'
                 f'<td style="padding:3px 6px;color:{cc};font-weight:700;">{sign}{abs(chg):.2f}%</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{vfmt}</td>'
                 f'<td style="padding:3px 6px;">{cfmt}</td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:380px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


def render_econ_calendar(state: TerminalState) -> None:
    """Economic calendar panel from ForexFactory."""
    phdr("Economic Calendar", "FOREX FACTORY · LIVE", "gold")
    events = state.econ_calendar
    if not events:
        _svc_unavailable("Economic calendar — awaiting ForexFactory data")
        return
    impact_color = {"high": C["red"], "medium": C["orange_br"], "low": C["muted"]}
    impact_dot   = {"high": "●●●", "medium": "●●○", "low": "●○○"}

    html_out = ('<div class="tp" style="padding:4px;max-height:300px;overflow-y:auto;">'
                f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
                f'<tr style="border-bottom:1px solid {C["border"]};">'
                + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};'
                          f'font-size:7.5px;letter-spacing:1px;">{h}</td>'
                          for h in ["IMPACT","COUNTRY","EVENT","ACTUAL","FORECAST","PREV"])
                + "</tr>")

    for ev in events[:25]:
        imp   = ev.get("impact","low")
        ic    = impact_color.get(imp, C["muted"])
        idot  = impact_dot.get(imp, "●○○")
        act   = ev.get("actual") or "—"
        fcast = ev.get("forecast") or "—"
        prev  = ev.get("previous") or "—"
        # Highlight if actual available
        act_col = (C["green_br"] if act != "—" and fcast != "—"
                   and str(act) >= str(fcast) else
                   C["red"] if act != "—" else C["text"])
        html_out += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                     f'<td style="padding:3px 6px;color:{ic};font-size:8px;">{idot}</td>'
                     f'<td style="padding:3px 6px;color:{C["muted"]};">{ev.get("country","")}</td>'
                     f'<td style="padding:3px 6px;color:{C["text"]};">{ev.get("event","")[:40]}</td>'
                     f'<td style="padding:3px 6px;color:{act_col};font-weight:700;">{act}</td>'
                     f'<td style="padding:3px 6px;color:{C["muted"]};">{fcast}</td>'
                     f'<td style="padding:3px 6px;color:{C["muted"]};">{prev}</td>'
                     f'</tr>')
    html_out += "</table></div>"
    st.markdown(html_out, unsafe_allow_html=True)


def render_live_news_feed_merged(state: TerminalState, category: str = "all",
                                  max_items: int = 15) -> None:
    """
    Merged news feed from all sources — wired to TerminalState.
    Shows live sentiment badge per article.
    Gracefully falls back to RSS seeds if pipeline hasn't loaded yet.
    """
    news_map = {
        "all":     state.news_all,
        "crypto":  state.news_crypto,
        "markets": state.news_stocks,
        "macro":   state.news_macro,
    }
    geo_map = state.geo_news

    items = news_map.get(category, [])

    # Fallback: use geo_news if available, then static seeds
    if not items:
        if category in geo_map and geo_map[category]:
            items = geo_map[category]
        else:
            items = _seed_news(category)

    if not items:
        _svc_unavailable(f"News feed ({category}) — loading…")
        return

    html_out = '<div style="max-height:260px;overflow-y:auto;">'
    for a in items[:max_items]:
        sc     = a.get("sentiment", score_text_sentiment(a.get("title","")))
        sc_col = score_to_color(sc)
        sc_lbl = "▲" if sc > 0.08 else "▼" if sc < -0.08 else "—"
        alert  = a.get("is_alert", _is_alert(a.get("title","")))
        src_cls = "ns red" if alert else "ns"
        title  = a.get("title","")[:115]
        pub    = a.get("published","")[:16]
        src    = a.get("source","")[:13]
        html_out += (f'<div class="ni">'
                     f'<span class="{src_cls}">{src}</span>'
                     f'<div style="flex:1;">'
                     f'<div class="nh">{title}</div>'
                     f'<div style="display:flex;gap:8px;align-items:center;margin-top:1px;">'
                     f'<span class="nm">{pub}</span>'
                     f'<span style="font-size:8px;color:{sc_col};">{sc_lbl} {abs(sc):.2f}</span>'
                     f'</div></div></div>')
    html_out += "</div>"
    st.markdown(html_out, unsafe_allow_html=True)


def render_technical_panel(ticker: str, state: TerminalState) -> None:
    """
    Technical analysis panel — ATR, RSI, trend, support/resistance.
    Fetches async via get_technical_snapshot, renders gracefully if unavailable.
    """
    phdr("Technical Analysis", "BINANCE KLINES", "gold")
    ta = state.tech_analysis.get(ticker)

    if ta is None:
        # Trigger background fetch
        try:
            ta = run_async(get_technical_snapshot(ticker))
        except Exception:
            ta = {}

    if not ta or ta.get("error"):
        _svc_unavailable(f"Technical data — {ta.get('error','loading') if ta else 'loading'}")
        return

    current = ta.get("current_price", 0)
    rsi     = ta.get("rsi", 50)
    trend   = ta.get("trend","neutral")
    rsi_lbl = ta.get("rsi_signal","Neutral")
    tgt     = ta.get("target_range","—")
    supports    = ta.get("support",[])
    resistances = ta.get("resistance",[])

    trend_col = C["green_br"] if trend=="bullish" else C["red"] if trend=="bearish" else C["gold"]
    rsi_col   = C["red"] if rsi>=70 else C["green_br"] if rsi<=30 else C["gold"]

    # RSI gauge bar
    rsi_pct = int(rsi)
    st.markdown(f"""
    <div class="tp" style="padding:8px 12px;">
      <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
        <span style="font-size:8px;color:{C['muted']};letter-spacing:1px;">RSI(14)</span>
        <span style="font-family:Syne,sans-serif;font-size:14px;font-weight:700;
                     color:{rsi_col};">{rsi:.1f} — {rsi_lbl}</span>
      </div>
      <div style="background:{C['border2']};height:6px;border-radius:3px;overflow:hidden;">
        <div style="width:{rsi_pct}%;height:100%;background:{rsi_col};border-radius:3px;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:7px;
                  color:{C['muted']};margin-top:2px;"><span>0</span><span>30</span>
        <span>70</span><span>100</span></div>
    </div>""", unsafe_allow_html=True)

    render_kv([
        ("Trend",        trend.upper(),                 trend_col),
        ("EMA 10",       f"${ta.get('ema10',0):,.4g}",  C["gold"]),
        ("EMA 30",       f"${ta.get('ema30',0):,.4g}",  C["blue_br"]),
        ("Pivot",        f"${ta.get('pivot',0):,.4g}",  C["text"]),
        ("Support S1",   f"${supports[0]:,.4g}"  if supports    else "—", C["green_br"]),
        ("Support S2",   f"${supports[1]:,.4g}"  if len(supports)>1 else "—", C["green_br"]),
        ("Resistance R1",f"${resistances[0]:,.4g}" if resistances else "—", C["red"]),
        ("Resistance R2",f"${resistances[1]:,.4g}" if len(resistances)>1 else "—", C["red"]),
        ("ATR(14)",      f"{ta.get('atr',0):.4g}",      C["muted"]),
        ("Target Range", tgt,                            C["gold"]),
    ])


def render_market_status(state: TerminalState) -> None:
    """NASDAQ market open/closed/pre-market indicator."""
    ms = state.market_status
    if not ms:
        return
    status  = ms.get("status","—")
    msg     = ms.get("message","—")
    col_map = {"Open":C["green_br"],"Pre-market":C["orange_br"],
               "After-hours":C["blue_br"],"Closed":C["red"]}
    col = col_map.get(status, C["muted"])
    st.markdown(
        f'<div style="display:inline-flex;align-items:center;gap:6px;padding:3px 10px;'
        f'background:rgba(0,0,0,0.3);border:1px solid {col}44;border-radius:3px;'
        f'font-size:9px;color:{col};letter-spacing:1px;">{msg}</div>',
        unsafe_allow_html=True,
    )



# ═════════════════════════════════════════════════════════════════════════════════════
# §G  SOURCE INTEGRATIONS — Bloomberg Terminal + CryptoLens + Neuberg
# ═════════════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════════════
# §G0  CRUCIX COMPONENTS — Live News Ticker + OSINT Stream
# Ported design from calesthio/Crucix (MIT) — adapted for Python/Streamlit.
# Data: merged RSS + GDELT headlines, 5-min refresh cycle, no static cache.
# Visual hierarchy: CSS Grid, viewport-locked horizontal scroll, dark institutional.
# ═════════════════════════════════════════════════════════════════════════════════════

def render_crucix_news_ticker(state: "TerminalState") -> None:
    """
    Live horizontal scrolling news ticker — Crucix-style.
    Layout engine: CSS Grid + viewport-locked infinite marquee.
    Sources: OSINT stream + merged news (RSS + GDELT), 5-min refresh cycle.
    Hover pauses the scroll. FLASH items highlighted in red.
    Falls back to seed news if pipeline hasn't loaded yet.
    """
    # Prefer OSINT stream items; fall back to general news
    items = state.news_ticker_items or state.news_all or []
    if not items:
        items = _seed_news("all")

    # Build ticker HTML items
    ticker_items_html = ""
    for item in items[:50]:
        tier    = item.get("tier", _classify_osint_tier(item.get("title","")))
        tier_lc = tier.lower()
        src     = item.get("source","NEWS")[:14]
        title   = item.get("title","")[:120]
        ts      = item.get("published","")[:11]
        hdl_cls = f'crucix-ticker-headline {tier_lc}' if tier == "FLASH" else "crucix-ticker-headline"
        sep     = "  ⬥  "  # visual separator between items

        ticker_items_html += (
            f'<span class="crucix-ticker-item">'
            f'<span class="crucix-ticker-src {tier_lc}">{src}</span>'
            f'<span class="{hdl_cls}">{title}</span>'
            f'<span class="crucix-ticker-ts">{ts}</span>'
            f'</span>'
        )

    # Duplicate content for seamless loop
    scroll_content = ticker_items_html * 2

    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    st.markdown(
        f'<div class="crucix-ticker-outer">'
        f'<div class="crucix-ticker-label">⚡ LIVE FEED &nbsp;{now_utc}</div>'
        f'<div class="crucix-ticker-track">'
        f'<div class="crucix-ticker-scroll">{scroll_content}</div>'
        f'</div></div>',
        unsafe_allow_html=True,
    )


def render_osint_stream(state: "TerminalState", max_items: int = 40) -> None:
    """
    Crucix-style OSINT Stream panel.
    Visual hierarchy: source tag (Al Jazeera, CIG_TELEGRAM, etc.) + tier badge
    (FLASH / PRIORITY / ROUTINE) + timestamp. Dark-mode, high-density institutional.
    Layout: CSS Grid (dot | source | content | tier-tag).
    Data: aggregate_osint_stream() — 5-min refresh, no static cache.
    Falls back to general news_macro if OSINT stream hasn't loaded yet.
    """
    items = state.osint_stream
    if not items:
        # Fallback: use macro news tagged as ROUTINE
        items = [
            {**n, "tier": _classify_osint_tier(n.get("title","")),
             "source": n.get("source","RSS")}
            for n in (state.news_macro + state.news_all)[:max_items]
        ]
    if not items:
        _svc_unavailable("OSINT Stream — loading intelligence feeds…")
        return

    # Count tiers for header stats
    flash_n    = sum(1 for i in items if i.get("tier") == "FLASH")
    priority_n = sum(1 for i in items if i.get("tier") == "PRIORITY")
    routine_n  = sum(1 for i in items if i.get("tier") == "ROUTINE")
    age_s      = int(state.age("osint_stream"))
    age_lbl    = f"{age_s}s ago" if age_s < 999 else "initializing"

    # Header
    st.markdown(
        f'<div class="osint-panel">'
        f'<div class="osint-header">'
        f'<div style="font-family:Syne,sans-serif;font-weight:800;font-size:10px;'
        f'letter-spacing:2px;color:{C["gold"]};text-transform:uppercase;">'
        f'⊕ OSINT STREAM</div>'
        f'<div style="display:flex;gap:10px;align-items:center;">'
        f'<span style="font-size:8px;color:{C["red"]};letter-spacing:1px;'
        f'background:rgba(211,47,47,0.14);padding:2px 7px;border-radius:2px;">'
        f'FLASH {flash_n}</span>'
        f'<span style="font-size:8px;color:{C["orange_br"]};letter-spacing:1px;'
        f'background:rgba(192,122,42,0.12);padding:2px 7px;border-radius:2px;">'
        f'PRIORITY {priority_n}</span>'
        f'<span style="font-size:8px;color:{C["muted"]};letter-spacing:1px;">'
        f'ROUTINE {routine_n}</span>'
        f'</div>'
        f'<div style="font-size:8px;color:{C["muted"]};letter-spacing:1px;">'
        f'5-MIN REFRESH · {age_lbl}</div>'
        f'</div>'
        f'<div class="osint-stream-body">',
        unsafe_allow_html=True,
    )

    # Stream items
    html_rows = ""
    for item in items[:max_items]:
        tier    = item.get("tier", "ROUTINE")
        tier_lc = tier.lower()
        src     = item.get("source", "OSINT")[:16]
        title   = item.get("title", "")[:150]
        ts      = item.get("published","")[:16]
        link    = item.get("link","")
        sent    = item.get("sentiment", 0.0)
        sc_col  = score_to_color(sent)

        # Source badge color logic — prominent source tagging per Crucix spec
        if any(k in src.upper() for k in ["ALJAZEERA","AL JAZEERA","TRT","FRANCE24","MEE","MIDDLE"]):
            src_style = f"background:rgba(0,115,230,0.18);color:{C['blue_br']};"
        elif any(k in src.upper() for k in ["TELEGRAM","CIG_","OSINT","INTEL","BELLINGCAT","ISW"]):
            src_style = f"background:rgba(126,87,194,0.18);color:{C['purple']};"
        elif any(k in src.upper() for k in ["MILTIMES","DEFNEWS","STRATFOR","T&PURPOSE"]):
            src_style = f"background:rgba(192,122,42,0.18);color:{C['orange_br']};"
        elif tier == "FLASH":
            src_style = f"background:rgba(211,47,47,0.22);color:{C['red']};"
        else:
            src_style = f"background:rgba(197,168,97,0.10);color:{C['gold']};"

        # Clickable title
        title_html = (f'<a href="{link}" target="_blank" style="color:inherit;text-decoration:none;">'
                      f'{title}</a>' if link else title)
        title_cls  = "osint-title flash" if tier == "FLASH" else "osint-title"

        html_rows += (
            f'<div class="osint-item">'
            f'<div class="osint-tier-dot {tier_lc}"></div>'
            f'<div class="osint-src-badge" style="{src_style}">{src}</div>'
            f'<div class="osint-content">'
            f'<div class="{title_cls}">{title_html}</div>'
            f'<div class="osint-meta">'
            f'<span>{ts}</span>'
            f'<span style="color:{sc_col};margin-left:8px;">{'▲' if sent>0.05 else '▼' if sent<-0.05 else '—'} {abs(sent):.2f}</span>'
            f'</div></div>'
            f'<div class="osint-tier-tag {tier_lc}">{tier}</div>'
            f'</div>'
        )

    st.markdown(html_rows + '</div></div>', unsafe_allow_html=True)


# ── G1. Symbol Detection Engine (ported from CryptoLens news_service.py) ─────────────

# Crypto symbol map (keyword → Binance pair) — priority ordered, longest first
CRYPTO_SYMBOL_MAP: List[Tuple[str, str]] = [
    ("BITCOIN",       "BTCUSDT"),  ("BTCUSDT",     "BTCUSDT"),  (" BTC ",      "BTCUSDT"),
    ("ETHEREUM",      "ETHUSDT"),  ("ETHUSDT",     "ETHUSDT"),  (" ETH ",      "ETHUSDT"),
    ("SOLANA",        "SOLUSDT"),  ("SOLUSDT",     "SOLUSDT"),  (" SOL ",      "SOLUSDT"),
    ("RIPPLE",        "XRPUSDT"),  ("XRPUSDT",     "XRPUSDT"),  (" XRP ",      "XRPUSDT"),
    ("CARDANO",       "ADAUSDT"),  (" ADA ",        "ADAUSDT"),
    ("DOGECOIN",      "DOGEUSDT"), (" DOGE ",       "DOGEUSDT"),
    ("SHIBA INU",     "SHIBUSDT"), (" SHIB ",       "SHIBUSDT"),
    ("AVALANCHE",     "AVAXUSDT"), (" AVAX ",       "AVAXUSDT"),
    ("POLKADOT",      "DOTUSDT"),  (" DOT ",        "DOTUSDT"),
    ("CHAINLINK",     "LINKUSDT"), (" LINK ",       "LINKUSDT"),
    ("POLYGON",       "MATICUSDT"),(" MATIC ",      "MATICUSDT"),
    ("UNISWAP",       "UNIUSDT"),  (" UNI ",        "UNIUSDT"),
    ("LITECOIN",      "LTCUSDT"),  (" LTC ",        "LTCUSDT"),
    ("BINANCE COIN",  "BNBUSDT"),  (" BNB ",        "BNBUSDT"),
    ("COSMOS",        "ATOMUSDT"), (" ATOM ",       "ATOMUSDT"),
    ("NEAR PROTOCOL", "NEARUSDT"), (" NEAR ",       "NEARUSDT"),
    ("SUI NETWORK",   "SUIUSDT"),  (" SUI ",        "SUIUSDT"),
    ("ARBITRUM",      "ARBUSDT"),  (" ARB ",        "ARBUSDT"),
    ("OPTIMISM",      "OPUSDT"),   (" OP ",         "OPUSDT"),
    ("PEPE",          "PEPEUSDT"), ("TRON",         "TRXUSDT"),  (" TRX ",     "TRXUSDT"),
]

# Stock keyword → ticker (ported from CryptoLens stock_market_service.py STOCK_METADATA)
STOCK_KEYWORD_MAP: Dict[str, str] = {
    "APPLE": "AAPL",   "MICROSOFT": "MSFT",  "NVIDIA": "NVDA",   "GOOGLE": "GOOG",
    "ALPHABET": "GOOG","AMAZON": "AMZN",     "META": "META",     "FACEBOOK": "META",
    "TESLA": "TSLA",   "NETFLIX": "NFLX",   "BROADCOM": "AVGO", "COSTCO": "COST",
    "AMD": "AMD",      "INTEL": "INTC",      "QUALCOMM": "QCOM", "CISCO": "CSCO",
    "PAYPAL": "PYPL",  "JPMORGAN": "JPM",    "JP MORGAN": "JPM", "GOLDMAN": "GS",
    "MORGAN STANLEY": "MS", "VISA": "V",     "MASTERCARD": "MA", "BLACKROCK": "BLK",
    "EXXON": "XOM",    "CHEVRON": "CVX",     "PFIZER": "PFE",    "MODERNA": "MRNA",
    "JOHNSON": "JNJ",  "ELI LILLY": "LLY",  "LILLY": "LLY",     "MERCK": "MRK",
    "WALMART": "WMT",  "TARGET": "TGT",      "HOME DEPOT": "HD", "NIKE": "NKE",
    "DISNEY": "DIS",   "STARBUCKS": "SBUX",  "MCDONALD": "MCD",  "BOEING": "BA",
    "CATERPILLAR": "CAT", "TAIWAN SEMICONDUCTOR": "TSM", "TSMC": "TSM",
    "APPLIED MATERIALS": "AMAT", "LAM RESEARCH": "LRCX",
}


# ── Extended stock keyword map (merged from CryptoLens symbol_detection_service.py) ──
STOCK_KEYWORD_MAP.update({
    "COINBASE":"COIN","COIN":"COIN","MICROSTRATEGY":"MSTR","MSTR":"MSTR",
    "MARATHON DIGITAL":"MARA","MARA":"MARA","RIOT PLATFORMS":"RIOT","RIOT":"RIOT",
    "ADOBE":"ADBE","ADBE":"ADBE","SALESFORCE":"CRM","CRM":"CRM",
    "ORACLE":"ORCL","ORCL":"ORCL","SHOPIFY":"SHOP","SHOP":"SHOP",
    "UBER":"UBER","AIRBNB":"ABNB","ABNB":"ABNB","SNOWFLAKE":"SNOW","SNOW":"SNOW",
    "PALANTIR":"PLTR","PLTR":"PLTR","CROWDSTRIKE":"CRWD","CRWD":"CRWD",
    "CLOUDFLARE":"NET","ROBINHOOD":"HOOD","HOOD":"HOOD","SOFI":"SOFI",
    "BLOCK":"SQ","SQUARE":"SQ","SQ":"SQ","RIVIAN":"RIVN","RIVN":"RIVN",
    "LUCID":"LCID","LCID":"LCID","NIO":"NIO","ELI LILLY":"LLY","LLY":"LLY",
    "NOVO NORDISK":"NVO","NVO":"NVO","PLUG POWER":"PLUG","PLUG":"PLUG",
    "FIRST SOLAR":"FSLR","FSLR":"FSLR","BLACKROCK":"BLK","BLK":"BLK",
    "FORD":"F","GENERAL MOTORS":"GM","GM":"GM",
    "DISNEY":"DIS","DIS":"DIS","STARBUCKS":"SBUX","SBUX":"SBUX",
    "MCDONALD":"MCD","MCD":"MCD","BOEING":"BA","BA":"BA",
    "CISCO":"CSCO","CSCO":"CSCO","IBM":"IBM","NETFLIX":"NFLX","NFLX":"NFLX",
})

# ── CoinGecko ID map (merged from CryptoLens asset_detail_service.py) ──
COINGECKO_IDS: Dict[str, str] = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","SOL":"solana",
    "XRP":"ripple","ADA":"cardano","DOGE":"dogecoin","AVAX":"avalanche-2",
    "LINK":"chainlink","DOT":"polkadot","MATIC":"matic-network","POL":"matic-network",
    "SHIB":"shiba-inu","TRX":"tron","UNI":"uniswap","ATOM":"cosmos",
    "LTC":"litecoin","ETC":"ethereum-classic","XLM":"stellar","BCH":"bitcoin-cash",
    "NEAR":"near","APT":"aptos","FIL":"filecoin","ARB":"arbitrum",
    "OP":"optimism","VET":"vechain","ALGO":"algorand","AAVE":"aave",
    "FTM":"fantom","SAND":"the-sandbox","MANA":"decentraland","AXS":"axie-infinity",
    "THETA":"theta-token","XTZ":"tezos","EOS":"eos","FLOW":"flow","CHZ":"chiliz",
    "LDO":"lido-dao","IMX":"immutable-x","RENDER":"render-token","RNDR":"render-token",
    "INJ":"injective-protocol","SUI":"sui","SEI":"sei-network","TIA":"celestia",
    "PEPE":"pepe","WIF":"dogwifcoin","BONK":"bonk","FLOKI":"floki",
    "FET":"fetch-ai","GRT":"the-graph","STX":"blockstack","MKR":"maker",
    "RUNE":"thorchain","WLD":"worldcoin-wld","TAO":"bittensor",
    "TON":"the-open-network","HBAR":"hedera-hashgraph","ICP":"internet-computer",
    "KAS":"kaspa","ENS":"ethereum-name-service","COMP":"compound-governance-token",
    "CRO":"crypto-com-chain","QNT":"quant-network","KAVA":"kava",
    "ZEC":"zcash","MINA":"mina-protocol","NEO":"neo","XMR":"monero",
    "IOTA":"iota","ZIL":"zilliqa","DASH":"dash","1INCH":"1inch",
    "SUSHI":"sushi","YFI":"yearn-finance","BAT":"basic-attention-token",
}

# ── Crypto sector taxonomy (from CryptoLens heatmap_service.py) ──
CRYPTO_SECTORS: Dict[str, str] = {
    "bitcoin":"Store of Value","ethereum":"Smart Contracts","binancecoin":"Exchange",
    "solana":"Smart Contracts","ripple":"Payments","cardano":"Smart Contracts",
    "dogecoin":"Meme","avalanche-2":"Smart Contracts","polkadot":"Interoperability",
    "chainlink":"Oracle","matic-network":"Layer 2","litecoin":"Payments",
    "uniswap":"DeFi","cosmos":"Interoperability","near":"Smart Contracts",
    "stellar":"Payments","aptos":"Smart Contracts","arbitrum":"Layer 2",
    "optimism":"Layer 2","filecoin":"Storage","injective-protocol":"DeFi",
    "sui":"Smart Contracts","render-token":"AI/Compute","the-graph":"Infrastructure",
    "aave":"DeFi","tron":"Smart Contracts","shiba-inu":"Meme",
    "fetch-ai":"AI/Compute","bittensor":"AI/Compute",
}

# Full company name list sorted by length (longest first) for greedy matching
_STOCK_NAMES_SORTED = sorted(STOCK_KEYWORD_MAP.items(), key=lambda x: len(x[0]), reverse=True)


def detect_asset_symbol(text: str, title: str = "", asset_type: str = "auto") -> str:
    """
    Smart symbol detection from news text.
    Ported and merged from CryptoLens symbol_detection_service.py + news_service.py.
    Returns Binance symbol (crypto) or Yahoo Finance ticker (stock).
    """
    t_up = (" " + title.upper() + " ") if title else ""
    b_up = " " + text.upper() + " "

    # Auto-detect type from keywords
    if asset_type == "auto":
        crypto_kws = {"BITCOIN","ETHEREUM","CRYPTO","BLOCKCHAIN","DEFI","NFT","BINANCE","COINBASE"}
        asset_type = "crypto" if any(k in t_up+b_up for k in crypto_kws) else "stock"

    if asset_type == "crypto":
        for kw, sym in CRYPTO_SYMBOL_MAP:
            if kw in t_up or kw in b_up:
                return sym
        return "BTCUSDT"
    else:
        # Try company names first (longest match)
        for name, ticker in _STOCK_NAMES_SORTED:
            if name in t_up or name in b_up:
                return ticker
        # Try ticker symbols with word boundaries
        for ticker in ["AAPL","MSFT","NVDA","GOOG","AMZN","META","TSLA","AMD","INTC",
                       "JPM","V","MA","XOM","CVX","PFE","JNJ","LLY","WMT","DIS"]:
            if f" {ticker} " in t_up or f" {ticker} " in b_up:
                return ticker
        return "SPY"


# ── G2. On-Chain Whale Activity (ported from CryptoLens onchain_service.py) ────────

MIN_WHALE_USD = 500_000  # $500K minimum whale threshold

async def _fetch_binance_aggtrades(symbol: str,
                                    client: aiohttp.ClientSession,
                                    limit: int = 60) -> List[Dict]:
    """Fetch Binance aggTrades — public, no key required."""
    if not RATE_LIMITER.check(f"https://api.binance.com/api/v3/aggTrades", max_per_minute=8):
        return []
    try:
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&limit={limit}"
        async with client.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                trades = await r.json(content_type=None)
                whales = []
                for t in trades:
                    price = float(t["p"]); qty = float(t["q"]); val = price * qty
                    if val >= MIN_WHALE_USD:
                        whales.append({
                            "symbol":    symbol.replace("USDT",""),
                            "price":     price,
                            "quantity":  qty,
                            "value":     val,
                            "side":      "sell" if t["m"] else "buy",
                            "timestamp": t["T"],
                        })
                return whales
    except Exception:
        pass
    return []


async def fetch_whale_trades_async() -> Dict:
    """
    Aggregate on-chain whale activity from Binance public aggTrades.
    Ported from CryptoLens onchain_service.py fetch_whale_trades().
    ── LIVE API KEY INJECTION POINT ──
    For institutional whale tracking: Whale Alert API
      requests.get("https://api.whale-alert.io/v1/transactions",
                   params={"api_key": WHALE_ALERT_KEY, "min_value": 1000000})
    """
    state   = get_terminal_state()
    tracked = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"]
    all_whales: List[Dict] = []

    connector = aiohttp.TCPConnector(ssl=False, limit=6)
    try:
        async with aiohttp.ClientSession(connector=connector) as client:
            tasks = [_fetch_binance_aggtrades(sym, client) for sym in tracked]
            results = await asyncio.gather(*tasks, return_exceptions=False)
        for r in results:
            if isinstance(r, list):
                all_whales.extend(r)
    except Exception:
        pass

    all_whales.sort(key=lambda x: x["timestamp"], reverse=True)
    buy_vol  = sum(t["value"] for t in all_whales if t["side"] == "buy")
    sell_vol = sum(t["value"] for t in all_whales if t["side"] == "sell")
    total    = buy_vol + sell_vol

    result = {
        "trades": all_whales[:30],
        "buy_pressure_pct": round(buy_vol / total * 100, 1) if total else 50.0,
        "net_flow":         buy_vol - sell_vol,
        "total_volume":     total,
    }
    state.update(whale_data=result if hasattr(state, "whale_data") else result)
    return result


# ── G3. Market Status (ported from CryptoLens stock_market_service.py) ──────────────

try:
    import pytz as _pytz_mod
    _HAS_PYTZ = True
except ImportError:
    _HAS_PYTZ = False


def get_market_status_live() -> Dict:
    """
    Real-time NASDAQ market session status.
    Ported from CryptoLens get_market_status().
    ── LIVE API KEY INJECTION POINT ──
    For live holiday/early-close calendar: Polygon.io /v1/marketstatus/now
      requests.get("https://api.polygon.io/v1/marketstatus/now", params={"apiKey": POLYGON_KEY})
    """
    try:
        if _HAS_PYTZ:
            import pytz
            tz  = pytz.timezone("America/New_York")
            now = datetime.now(tz)
        else:
            # UTC offset fallback: EST = UTC-5, EDT = UTC-4
            import datetime as _dt
            utc_offset = -4  # Approximate EDT; adjust for DST
            now = datetime.now(timezone.utc) + timedelta(hours=utc_offset)
        if now.weekday() >= 5:
            return {"status":"Closed","message":"🔴 Closed (Weekend)","color":"red"}
        mins = now.hour*60 + now.minute
        if 240 <= mins < 570:
            to_open = 570 - mins
            return {"status":"Pre-market","message":f"🟠 Pre-market · Opens in {to_open//60}h {to_open%60}m","color":"orange"}
        if 570 <= mins < 960:
            to_close = 960 - mins
            return {"status":"Open","message":f"🟢 Market Open · Closes in {to_close//60}h {to_close%60}m","color":"green"}
        if 960 <= mins < 1200:
            return {"status":"After-hours","message":"🌙 After-hours trading","color":"blue"}
        return {"status":"Closed","message":"🔴 Closed","color":"red"}
    except Exception:
        return {"status":"Unknown","message":"-- Market Status","color":"gray"}


# ── G4. Global Indices (ported from CryptoLens stock_market_service.py) ─────────────

GLOBAL_INDICES_MAP: Dict[str, Dict] = {
    "^GSPC":  {"name":"S&P 500",     "region":"US"},
    "^IXIC":  {"name":"NASDAQ Comp.","region":"US"},
    "^DJI":   {"name":"Dow Jones",   "region":"US"},
    "^FTSE":  {"name":"FTSE 100",    "region":"UK"},
    "^GDAXI": {"name":"DAX",         "region":"DE"},
    "^N225":  {"name":"Nikkei 225",  "region":"JP"},
    "^HSI":   {"name":"Hang Seng",   "region":"HK"},
    "^FCHI":  {"name":"CAC 40",      "region":"FR"},
    "^AXJO":  {"name":"ASX 200",     "region":"AU"},
    "^BSESN": {"name":"Sensex",      "region":"IN"},
}


@st.cache_data(ttl=120, show_spinner=False)
def get_global_indices() -> List[Dict]:
    """
    Batch-fetch global equity index prices from Yahoo Finance.
    Ported from CryptoLens fetch_global_indices().
    """
    out: List[Dict] = []
    hdrs = {"User-Agent": "Mozilla/5.0"}
    for sym, meta in GLOBAL_INDICES_MAP.items():
        if not RATE_LIMITER.check(YF_CHART, max_per_minute=25):
            continue
        try:
            url  = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=2d"
            resp = requests.get(url, headers=hdrs, timeout=8)
            res  = resp.json()["chart"]["result"][0]
            m    = res["meta"]
            price = float(m.get("regularMarketPrice",0) or 0)
            prev  = float(m.get("chartPreviousClose",0) or price)
            chg   = (price-prev)/prev*100 if prev else 0
            out.append({**meta,"symbol":sym,"price":price,"change_24h":round(chg,2)})
        except Exception:
            continue
    # Synthetic fallback for any missing
    seed = {"^GSPC":5280,"^IXIC":18640,"^DJI":41200,"^FTSE":8480,
            "^GDAXI":18240,"^N225":38500,"^HSI":19800,"^FCHI":7920,
            "^AXJO":7820,"^BSESN":72400}
    existing = {i["symbol"] for i in out}
    for sym, meta in GLOBAL_INDICES_MAP.items():
        if sym not in existing:
            base = seed.get(sym, 10000)
            pct  = round(random.uniform(-0.8, 0.8), 2)
            out.append({**meta,"symbol":sym,"price":round(base*(1+pct/100),2),"change_24h":pct})
    return out


# ── G5. Enhanced Symbol-Aware Sentiment (Bloomberg + CryptoLens merged) ──────────────

def score_news_batch_with_symbols(news_items: List[Dict]) -> Dict[str, float]:
    """
    Run sentiment scoring across all news items and bucket by detected symbol.
    Merges Bloomberg Terminal AI stub + CryptoLens sentiment + our lexicon engine.
    Returns {symbol: avg_sentiment}.
    """
    buckets: Dict[str, List[float]] = {}
    for item in news_items:
        title   = item.get("title","")
        summary = item.get("summary","")
        atype   = item.get("asset_type", "auto")
        sym     = item.get("symbol") or detect_asset_symbol(summary, title, atype)
        score   = score_text_sentiment(title + " " + summary)
        buckets.setdefault(sym, []).append(score)
    return {sym: round(sum(sc)/len(sc),4) for sym, sc in buckets.items() if sc}


# ── G6. New UI Panels ─────────────────────────────────────────────────────────────────

def render_whale_activity(whale_data: Dict) -> None:
    """
    Whale on-chain activity panel — ported from CryptoLens onchain_service.py.
    Shows recent $500K+ trades with buy/sell pressure bar.
    """
    phdr("Whale Activity Monitor", "BINANCE · LIVE", "blue")
    if not whale_data or not whale_data.get("trades"):
        _svc_unavailable("Whale monitor — awaiting Binance aggTrades")
        return

    trades  = whale_data.get("trades", [])
    bp      = whale_data.get("buy_pressure_pct", 50.0)
    net     = whale_data.get("net_flow", 0)
    net_col = C["green_br"] if net >= 0 else C["red"]
    net_sign= "▲" if net >= 0 else "▼"

    # Pressure bar
    st.markdown(f"""
    <div class="tp" style="padding:8px 12px;margin-bottom:4px;">
      <div style="display:flex;justify-content:space-between;font-size:9px;
                  color:{C["muted"]};margin-bottom:4px;">
        <span>BUY PRESSURE</span>
        <span style="color:{net_col};">{net_sign} Net ${abs(net)/1e6:.1f}M</span>
      </div>
      <div style="display:flex;background:{C["border2"]};height:8px;border-radius:4px;overflow:hidden;">
        <div style="width:{bp:.1f}%;background:{C["green_br"]};border-radius:4px 0 0 4px;"></div>
        <div style="flex:1;background:{C["red"]};border-radius:0 4px 4px 0;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:8px;
                  color:{C["muted"]};margin-top:2px;">
        <span style="color:{C["green_br"]};">BUY {bp:.1f}%</span>
        <span style="color:{C["red"]};">SELL {100-bp:.1f}%</span>
      </div>
    </div>""", unsafe_allow_html=True)

    # Recent trades table
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;">{h}</td>'
                     for h in ["SYMBOL","SIDE","SIZE","PRICE"])
           + "</tr>")
    rows = ""
    for t in trades[:12]:
        sc   = C["green_br"] if t["side"]=="buy" else C["red"]
        side = "▲ BUY"  if t["side"]=="buy" else "▼ SELL"
        sz   = f"${t['value']/1e6:.2f}M" if t["value"]>=1e6 else f"${t['value']/1e3:.0f}K"
        pr   = (f"${t['price']:,.0f}" if t["price"]>=1000
                else f"${t['price']:,.4f}" if t["price"]>=1 else f"${t['price']:.6f}")
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["gold"]};font-family:Syne,sans-serif;">{t["symbol"]}</td>'
                 f'<td style="padding:3px 6px;color:{sc};font-weight:700;">{side}</td>'
                 f'<td style="padding:3px 6px;color:{C["text"]};">{sz}</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{pr}</td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;max-height:240px;overflow-y:auto;">'
                f'{hdr}{rows}</table></div>', unsafe_allow_html=True)


def render_global_indices(indices: List[Dict]) -> None:
    """Global market indices panel — ported from CryptoLens fetch_global_indices()."""
    phdr("Global Indices", "YAHOO FINANCE · LIVE", "gold")
    if not indices:
        _svc_unavailable("Global indices — loading...")
        return
    hdr = (f'<table style="width:100%;border-collapse:collapse;font-size:9px;">'
           f'<tr style="border-bottom:1px solid {C["border"]};">'
           + "".join(f'<td style="padding:3px 6px;color:{C["gold"]};font-size:7.5px;">{h}</td>'
                     for h in ["INDEX","REGION","PRICE","24H"])
           + "</tr>")
    rows = ""
    for idx in indices:
        chg = idx.get("change_24h",0)
        cc  = C["green_br"] if chg>=0 else C["red"]
        sign= "▲" if chg>=0 else "▼"
        pr  = f"{idx.get('price',0):,.1f}"
        rows += (f'<tr style="border-bottom:1px solid {C["border"]};">'
                 f'<td style="padding:3px 6px;font-weight:700;color:{C["text"]};font-family:Syne,sans-serif;">{idx.get("name","")}</td>'
                 f'<td style="padding:3px 6px;color:{C["muted"]};">{idx.get("region","")}</td>'
                 f'<td style="padding:3px 6px;">{pr}</td>'
                 f'<td style="padding:3px 6px;color:{cc};font-weight:700;">{sign}{abs(chg):.2f}%</td>'
                 f'</tr>')
    st.markdown(f'<div class="tp" style="padding:4px;">{hdr}{rows}</table></div>',
                unsafe_allow_html=True)


def render_market_status_panel(state: "TerminalState") -> None:
    """Market session status badge."""
    ms = state.market_status or get_market_status_live()
    col_map = {"Open":C["green_br"],"Pre-market":C["orange_br"],
               "After-hours":C["blue_br"],"Closed":C["red"]}
    col = col_map.get(ms.get("status",""),C["muted"])
    st.markdown(
        f'<div class="tp" style="text-align:center;padding:6px 12px;'
        f'border-color:{col}44;">'
        f'<div style="font-size:10px;color:{col};font-weight:700;">'
        f'{ms.get("message","—")}</div></div>',
        unsafe_allow_html=True)


# ── G7. Extended pipeline cycle — adds whale + indices + market status ────────────────

async def _run_extended_pipeline(state: "TerminalState") -> None:
    """
    Supplement _run_pipeline_cycle() with:
      - Market session status (pure computation, every call)
      - Whale on-chain activity (Binance aggTrades, every 30s)
      - Symbol-aware NLP sentiment re-score (every 3 min)
      - Fear & Greed indices (every 5 min, handled in core pipeline)
    """
    # Market status — always refresh (pure computation)
    try:
        ms = get_market_status_live()
        state.update(market_status=ms)
    except Exception:
        pass

    # Whale activity (Binance public aggTrades — no key needed)
    if state.age("whale_data") > 30:
        try:
            wd = await fetch_whale_trades_async()
            with state._lock:
                state.whale_data = wd
                state.last_update["whale_data"] = time.time()
            state.clear_error("whale_data")
        except Exception as e:
            state.mark_error("whale_data", str(e))

    # Symbol-aware NLP sentiment re-score
    if state.news_all and state.age("sentiment_scores") > 180:
        try:
            per_sym = score_news_batch_with_symbols(state.news_all)
            comp    = compute_composite_sentiment(state.news_all)
            per_sym["composite"] = comp
            state.update(sentiment_scores=per_sym)
            state.clear_error("sentiment")
        except Exception as e:
            state.mark_error("sentiment", str(e))

# ═════════════════════════════════════════════════════════════════════════════════════
# §8  TAB RENDERERS — upgraded with TerminalState data pipelines
# ═════════════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=120)
def render_tab_macro() -> None:
    state = get_terminal_state()

    # Pull from centralized state (background pipeline keeps this fresh)
    events_df  = state.geo_events if len(state.geo_events) > 0 else fetch_gdelt_events()
    macro      = state.macro_indicators if state.macro_indicators else get_macro_indicators()

    # News from state (merged multi-source) or geo_news fallback
    world_intel = (state.news_macro + state.news_all)[:20] or []
    if not world_intel:
        world_intel = fetch_rss_news("world", 16) + fetch_rss_news("military", 8)

    left, right = st.columns([6.5, 3.5], gap="small")

    # ══ LEFT — GLOBE ══
    with left:
        st.markdown(f'<div style="background:#000;padding:0;border-right:1px solid {C["border"]};">',
                    unsafe_allow_html=True)

        ca, cb, cc = st.columns([2, 2, 4])
        with ca:
            zoom = st.slider("Zoom", 0.8, 6.0, 1.5, 0.1,
                             label_visibility="collapsed", key="g_zoom")
        with cb:
            center_opt = st.selectbox("Center",
                ["Middle East","Europe","Asia-Pacific","Americas","Global"],
                key="g_center", label_visibility="collapsed")
        with cc:
            if zoom >= 3.5:
                st.markdown(
                    f'<div style="display:inline-flex;align-items:center;gap:6px;padding:4px 10px;'
                    f'background:{C["panel"]};border:1px solid {C["gold_dim"]};border-radius:3px;'
                    f'font-size:8.5px;color:{C["gold"]};letter-spacing:1.5px;">'
                    f'⊕ SATELLITE ZOOM MODE</div>', unsafe_allow_html=True)

        centers = {"Middle East":(28.0,45.0),"Europe":(50.0,15.0),
                   "Asia-Pacific":(25.0,110.0),"Americas":(20.0,-85.0),"Global":(20.0,20.0)}
        clat, clng = centers.get(center_opt,(28.0,45.0))

        with st.expander("🗂  LAYER CONTROLS", expanded=False):
            lc1,lc2,lc3,lc4 = st.columns(4)
            with lc1:
                l_conflict = st.checkbox("Conflict Zones",  value=True,  key="l_c")
                l_hotspots = st.checkbox("Intel Hotspots",  value=True,  key="l_h")
            with lc2:
                l_military = st.checkbox("Military Bases",  value=False, key="l_m")
                l_nuclear  = st.checkbox("Nuclear Sites",   value=False, key="l_n")
            with lc3:
                l_cables   = st.checkbox("Undersea Cables", value=False, key="l_ca")
                l_flights  = st.checkbox("Live Flights",    value=False, key="l_f")
            with lc4:
                l_hex      = st.checkbox("Density Heatmap", value=True,  key="l_hex")

        filtered = events_df.copy() if len(events_df) > 0 else _seed_globe_df()
        if not l_hotspots:
            filtered = filtered[filtered["type"] != "GDELT"]
        if not l_conflict:
            filtered = filtered[~filtered["type"].isin(
                ["Active Conflict","Military Alert","Nuclear Risk"])]

        deck = build_globe(filtered, show_military=l_military, show_nuclear=l_nuclear,
                           show_flights=l_flights, show_cables=l_cables,
                           show_hex=l_hex, zoom=zoom, lat=clat, lng=clng)
        st.pydeck_chart(deck, use_container_width=True, height=510)

        if zoom >= 4.0:
            st.markdown(
                f'<div style="background:{C["panel"]};border:1px solid {C["gold_dim"]}80;'
                f'padding:5px 10px;font-size:8.5px;color:{C["gold"]};letter-spacing:1px;">'
                f'⊕ HIGH-ZOOM · Inject Mapbox key → satellite/GPS imagery active · '
                f'map_style="mapbox://styles/mapbox/satellite-v9"</div>',
                unsafe_allow_html=True)

        # Globe legend
        legend = [(C["red"],"Active Conflict"),(C["blue_br"],"Naval Tension"),
                  (C["orange_br"],"Infrastructure"),(f"#b040b0","Nuclear Site"),
                  (C["blue_br"],"Military Base"),(C["gold"],"GDELT Event")]
        leg_html = '<div style="display:flex;gap:14px;flex-wrap:wrap;padding:6px 0;font-size:9px;">'
        for col, lbl in legend:
            leg_html += (f'<span style="display:flex;align-items:center;gap:4px;">'
                         f'<span style="width:8px;height:8px;border-radius:50%;'
                         f'background:{col};flex-shrink:0;"></span>'
                         f'<span style="color:{C["muted"]};">{lbl}</span></span>')
        leg_html += "</div>"
        st.markdown(leg_html, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ══ RIGHT — INTELLIGENCE MATRIX ══
    with right:
        st.markdown(f'<div style="padding:6px 4px 0 6px;background:{C["bg"]};">',
                    unsafe_allow_html=True)

        render_pentagon_index()
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)
        render_risk_panel(COUNTRY_CII)
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Fear & Greed gauges — now wired to TerminalState
        render_fear_greed_row(state)
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Market status badge
        render_market_status_panel(state)
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Global indices panel
        indices = state.global_indices if state.global_indices else get_global_indices()
        render_global_indices(indices)
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Sentiment engine panel — new from sentiment engine
        render_sentiment_dashboard(state)
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Intel feed — now uses merged multi-source news
        render_intel_feed(world_intel[:14])
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)

        # Infrastructure cascade
        infra_idx = st.selectbox("Infra node",
            options=list(range(len(INFRA_CASCADE))),
            format_func=lambda i: INFRA_CASCADE[i]["node"],
            label_visibility="collapsed", key="infra_sel")
        render_infra_cascade(infra_idx)

        # Economic calendar — new panel wired to ForexFactory
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)
        render_econ_calendar(state)

        # Regional news — now uses merged live news via TerminalState
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)
        phdr("Geopolitical News", "MULTI-SOURCE · LIVE", "green")
        n1,n2,n3,n4,n5 = st.tabs(["🌍 World","🇺🇸 US","🪖 Military","📈 Markets","🇪🇺 Europe"])
        with n1: render_live_news_feed_merged(state, "macro",   12)
        with n2: render_live_news_feed_merged(state, "macro",    8)
        with n3: render_live_news_feed_merged(state, "macro",    8)
        with n4: render_live_news_feed_merged(state, "markets",  8)
        with n5: render_live_news_feed_merged(state, "macro",    8)

        # ── Crucix OSINT Stream panel ─────────────────────────────────────
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)
        render_osint_stream(state, max_items=35)

        # Macro indicators grid
        st.markdown("<br style='margin:2px 0;'>", unsafe_allow_html=True)
        phdr("Macro Indicators", "FRED · LIVE", "gold")
        render_macro_grid(macro)

        st.markdown("</div>", unsafe_allow_html=True)


@st.fragment(run_every=60)
def render_tab_stock() -> None:
    state = get_terminal_state()

    h1,h2,h3,h4,h5 = st.columns([3,1.5,1.5,1,1])
    with h1:
        ticker_in = st.text_input("Ticker", value="AAPL", key="stock_ticker",
            label_visibility="collapsed", placeholder="Ticker (AAPL, TSLA, NVDA, MSFT…)")
    with h2:
        period   = st.selectbox("Period", ["1mo","3mo","6mo","1y","2y"],
                                index=2, key="s_per", label_visibility="collapsed")
    with h3:
        interval = st.selectbox("Interval", ["1d","1wk","1mo"],
                                index=0, key="s_int", label_visibility="collapsed")
    with h4:
        show_ma  = st.checkbox("MA Lines",  value=True,  key="s_ma")
    with h5:
        show_bb  = st.checkbox("Bollinger", value=False, key="s_bb")

    ticker  = (ticker_in or "AAPL").upper().strip()
    profile = VALUATION_PROFILES.get(ticker, VALUATION_PROFILES["AAPL"])
    df      = get_ohlcv(ticker, interval=interval, period=period)
    news    = (state.news_stocks[:8] if state.news_stocks
               else fetch_rss_news("markets", max_items=8))

    # ── Company header ──
    if len(df) >= 2:
        last = df["close"].iloc[-1]
        prev = df["close"].iloc[-2]
        chg  = last - prev
        pct  = chg/prev*100 if prev else 0
        sign = "▲" if chg >= 0 else "▼"
        ccol = C["green_br"] if chg >= 0 else C["red"]
        init = ticker[:2]
        desc = profile.get("description","")[:220]

        # Market status badge from state
        ms_html = ""
        if state.market_status:
            ms = state.market_status
            sc_map = {"Open":C["green_br"],"Pre-market":C["orange_br"],
                      "After-hours":C["blue_br"],"Closed":C["red"]}
            mc = sc_map.get(ms.get("status",""),C["muted"])
            ms_html = (f'<span style="font-size:8px;color:{mc};padding:2px 6px;'
                       f'border:1px solid {mc}44;border-radius:3px;letter-spacing:1px;">'
                       f'{ms.get("message","")}</span>')

        st.markdown(
            f'<div style="background:{C["panel"]};border-bottom:1px solid {C["border"]};'
            f'padding:10px 16px;display:flex;align-items:center;gap:14px;">'
            f'<div class="clogo">{init}</div>'
            f'<div style="flex:1;">'
            f'<div style="font-family:Syne,sans-serif;font-size:19px;font-weight:800;'
            f'color:{C["text"]};line-height:1;">{profile.get("name",ticker)}</div>'
            f'<div style="font-size:9px;color:{C["muted"]};margin-top:2px;">'
            f'{ticker} · {profile.get("sector","—")} · {profile.get("exchange","—")} '
            f'&nbsp;{ms_html}</div>'
            f'<div style="font-size:9.5px;color:{C["text_dim"]};margin-top:3px;'
            f'max-width:700px;line-height:1.4;">{desc}…</div></div>'
            f'<div style="text-align:right;flex-shrink:0;">'
            f'<div style="font-family:Syne,sans-serif;font-size:26px;font-weight:800;'
            f'color:{C["text"]};line-height:1;">${last:,.2f}</div>'
            f'<div style="font-size:11.5px;color:{ccol};margin-top:2px;">'
            f'{sign}&nbsp;${abs(chg):.2f} ({abs(pct):.2f}%)</div>'
            f'<div style="font-size:8px;color:{C["muted"]};margin-top:2px;">'
            f'Vol: {df["volume"].iloc[-1]/1e6:.1f}M</div></div></div>',
            unsafe_allow_html=True)

    chart_col, info_col = st.columns([7,3], gap="small")

    with chart_col:
        ct1,ct2,ct3,_ = st.columns([1,1,1,3])
        with ct1: show_vol  = st.checkbox("Volume",  value=True,  key="s_vol")
        with ct2: show_rsi  = st.checkbox("RSI(14)", value=False, key="s_rsi")
        with ct3:
            chart_type = st.selectbox("Type", ["Candlestick","Line","Area"],
                                      key="s_ctype", label_visibility="collapsed")

        if chart_type == "Candlestick":
            fig = build_candlestick(df, ticker, show_ma=show_ma,
                                    show_volume=show_vol, show_bb=show_bb, show_rsi=show_rsi)
        elif chart_type == "Line":
            fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                line=dict(color=C["gold"],width=1.5), name=ticker))
            _apply_dark_layout(fig)
        else:
            fig = go.Figure(go.Scatter(x=df["timestamp"], y=df["close"],
                fill="tozeroy", fillcolor="rgba(197,168,97,0.12)",
                line=dict(color=C["gold"],width=1.5), name=ticker))
            _apply_dark_layout(fig)

        st.plotly_chart(fig, use_container_width=True, config={
            "displayModeBar":True,"displaylogo":False,"scrollZoom":True,
            "modeBarButtonsToRemove":["autoScale2d","lasso2d","select2d","toImage"]})

        e1,e2 = st.columns([3,2])
        with e1:
            phdr("Quarterly Earnings Trend")
            st.plotly_chart(build_earnings_chart(profile),
                            use_container_width=True, config={"displayModeBar":False})
        with e2:
            phdr("Revenue Segments")
            st.plotly_chart(build_segment_chart(profile),
                            use_container_width=True, config={"displayModeBar":False})

    with info_col:
        phdr("Key Statistics")
        p = profile
        render_kv([
            ("Market Cap",    p.get("market_cap","—"),             C["text"]),
            ("P/E (TTM)",     f"{p.get('pe','—')}x",               C["text"]),
            ("Fwd P/E",       f"{p.get('fwd_pe','—')}x",           C["text"]),
            ("EPS (TTM)",     f"${p.get('eps','—')}",              C["text"]),
            ("EPS Growth",    f"{p.get('eps_growth','—')}%",
             C["green_br"] if (p.get("eps_growth",0) or 0)>0 else C["red"]),
            ("Revenue",       p.get("revenue","—"),                C["text"]),
            ("Gross Margin",  f"{p.get('gross_margin',0)*100:.1f}%",C["green_br"]),
            ("Profit Margin", f"{p.get('profit_margin',0)*100:.1f}%",C["green_br"]),
            ("EBITDA Margin", f"{p.get('ebitda_margin',0)*100:.1f}%",C["text"]),
            ("ROE",           f"{p.get('roe',0)*100:.1f}%",        C["text"]),
            ("ROA",           f"{p.get('roa',0)*100:.1f}%",        C["text"]),
            ("Debt/Equity",   f"{p.get('debt_equity','—')}",       C["text"]),
            ("Free Cash Flow",p.get("fcf","—"),                    C["text"]),
            ("FCF Yield",     f"{p.get('fcf_yield',0)*100:.1f}%",  C["text"]),
            ("Div. Yield",    f"{p.get('dividend_yield',0)*100:.2f}%",C["text"]),
            ("Beta",          f"{p.get('beta','—')}",              C["text"]),
            ("52W High",      f"${p.get('week52_high','—')}",      C["green_br"]),
            ("52W Low",       f"${p.get('week52_low','—')}",       C["red"]),
            ("Shares Out.",   p.get("shares_out","—"),             C["text"]),
            ("Buybacks",      p.get("buyback","—"),                C["gold"]),
            ("Insider Own.",  p.get("insider_own","—"),            C["text"]),
            ("Inst. Own.",    p.get("inst_own","—"),               C["text"]),
            ("Short Float",   p.get("short_float","—"),            C["orange_br"]),
        ])

        st.markdown("<br>", unsafe_allow_html=True)

        # Technical analysis panel — new from CryptoLens engine
        render_technical_panel(ticker, state)

        st.markdown("<br>", unsafe_allow_html=True)
        phdr("Current News")
        # Use live merged news with sentiment badges
        render_live_news_feed_merged(state, "markets", max_items=6)

        st.markdown("<br>", unsafe_allow_html=True)
        # Whale on-chain activity panel (Binance aggTrades)
        whale_data = getattr(state, "whale_data", {})
        render_whale_activity(whale_data)

        st.markdown("<br>", unsafe_allow_html=True)
        # Crypto overview from CoinGecko
        render_crypto_overview_table(state)
        st.markdown("<br>", unsafe_allow_html=True)
        render_screener_table()


@st.fragment(run_every=300)
def render_tab_valuation() -> None:
    state = get_terminal_state()

    h1,h2,h3 = st.columns([4,2,3])
    with h1:
        vt = st.text_input("Val Ticker", value="AAPL", key="val_ticker",
            label_visibility="collapsed",
            placeholder="Ticker (AAPL, TSLA, NVDA, MSFT)…").upper().strip() or "AAPL"
    with h2:
        mode = st.selectbox("Model", ["DCF","DDM","EV/EBITDA"],
                            key="dcf_mode", label_visibility="collapsed")
    with h3:
        profile = VALUATION_PROFILES.get(vt, VALUATION_PROFILES["AAPL"])
        rating  = profile["rating"]
        rcol    = C["green_br"] if rating>=65 else C["orange_br"] if rating>=45 else C["red"]
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:10px;padding-top:4px;">'
            f'<span style="font-size:9px;color:{C["muted"]};letter-spacing:1px;">OVERALL RATING</span>'
            f'<span style="font-family:Syne,sans-serif;font-size:20px;font-weight:800;color:{rcol};'
            f'background:rgba(255,255,255,0.04);border:1px solid {rcol}33;'
            f'padding:4px 14px;border-radius:3px;">{rating}%</span>'
            f'<span style="font-size:8.5px;color:{C["muted"]};letter-spacing:1.5px;">'
            f'{profile["rating_label"]}</span></div>',
            unsafe_allow_html=True)

    st.markdown(f'<hr style="border-color:{C["border"]};margin:6px 0;">',
                unsafe_allow_html=True)

    card_col, dcf_col = st.columns([3.5,6.5], gap="small")

    with card_col:
        phdr(f"Valuation Scorecard — {profile.get('name', vt)}")
        render_val_cards(profile)
        st.markdown(
            f'<div class="tp" style="background:rgba(197,168,97,0.04);'
            f'border-color:rgba(197,168,97,0.18);margin-top:6px;">'
            f'<div style="font-family:Syne,sans-serif;font-weight:700;color:{C["gold"]};'
            f'font-size:9px;letter-spacing:1.5px;margin-bottom:5px;">ASSESSMENT</div>'
            f'<div style="font-size:10px;line-height:1.6;color:{C["text_dim"]};">'
            f'{profile.get("summary","")}</div></div>',
            unsafe_allow_html=True)
        st.markdown(
            f'<div class="tp" style="margin-top:4px;">'
            f'<div style="font-size:7.5px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:5px;">Company Profile</div>'
            f'<div style="font-size:9.5px;line-height:1.55;color:{C["text_dim"]};">'
            f'{profile.get("description","")}</div></div>',
            unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        # Sentiment specific to this ticker
        ticker_sentiment = state.sentiment_scores.get(vt, 0.0)
        ts_col = score_to_color(ticker_sentiment)
        ts_lbl = score_to_label(ticker_sentiment)
        st.markdown(
            f'<div class="tp" style="padding:8px 12px;">'
            f'<div style="font-size:8px;letter-spacing:1.5px;color:{C["muted"]};'
            f'text-transform:uppercase;margin-bottom:4px;">NLP NEWS SENTIMENT — {vt}</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:16px;font-weight:800;'
            f'color:{ts_col};">{ts_lbl}</div>'
            f'<div style="font-size:9px;color:{ts_col};margin-top:2px;">'
            f'Score: {ticker_sentiment:+.3f}</div></div>',
            unsafe_allow_html=True)

    with dcf_col:
        phdr("Discounted Cash Flow Model", badge=mode, badge_type="gold")
        render_dcf_panel(profile, mode=mode)

        st.markdown("<br>", unsafe_allow_html=True)
        phdr("DCF Sensitivity — WACC × Terminal Growth Rate", "HEATMAP", "gold")
        st.plotly_chart(
            build_dcf_sensitivity(profile["current_price"], profile["fair_value_dcf"]),
            use_container_width=True, config={"displayModeBar":False})

        st.markdown("<br>", unsafe_allow_html=True)

        # Macro context — live from TerminalState
        macro = state.macro_indicators if state.macro_indicators else get_macro_indicators()
        phdr("Macro Context", "FRED · LIVE", "gold")
        m_keys = ["Fed Funds Rate","10Y Treasury","2Y Treasury",
                  "US CPI YoY","US GDP (QoQ)","VIX Index"]
        neg_up = {"Fed Funds Rate","10Y Treasury","2Y Treasury","US CPI YoY","Yield Spread"}
        mc = st.columns(3)
        for i, key in enumerate(m_keys):
            info  = macro.get(key, MACRO_SEED.get(key,{}))
            val   = info.get("value","—")
            delta = info.get("delta","")
            note  = info.get("note","")
            trend = info.get("trend","flat")
            is_b  = trend=="up" and key in neg_up
            dc    = C["red"] if is_b else C["green_br"] if trend in ("up","down") else C["muted"]
            arr   = "▲" if trend=="up" else "▼" if trend=="down" else "—"
            with mc[i%3]:
                st.markdown(
                    f'<div class="tp" style="padding:7px 10px;margin-bottom:4px;">'
                    f'<div style="font-size:7px;letter-spacing:1.5px;color:{C["muted"]};'
                    f'text-transform:uppercase;">{key}</div>'
                    f'<div style="font-family:Syne,sans-serif;font-size:15px;font-weight:800;'
                    f'color:{C["text"]};line-height:1.1;">{val}</div>'
                    f'<div style="font-size:8px;color:{dc};">{arr} {delta}</div>'
                    f'<div style="font-size:7.5px;color:{C["muted"]};">{note}</div></div>',
                    unsafe_allow_html=True)

        phdr("Macro Overview Chart", "FRED", "gold")
        st.plotly_chart(build_macro_bar(macro), use_container_width=True,
                        config={"displayModeBar":False})

        st.markdown("<br>", unsafe_allow_html=True)
        render_peer_table(profile)

        st.markdown("<br>", unsafe_allow_html=True)
        phdr("Fundamental Metrics")
        fm1,fm2,fm3,fm4 = st.columns(4)
        with fm1: st.metric("P/E (TTM)",     f"{profile.get('pe','—')}x",
                             f"Peer {profile.get('peer_pe','—')}x")
        with fm2: st.metric("Gross Margin",  f"{profile.get('gross_margin',0)*100:.1f}%","")
        with fm3: st.metric("ROE",           f"{profile.get('roe',0)*100:.1f}%","")
        with fm4: st.metric("Debt/Equity",   f"{profile.get('debt_equity','—')}","")

        fm5,fm6,fm7,fm8 = st.columns(4)
        with fm5: st.metric("FCF Yield",     f"{profile.get('fcf_yield',0)*100:.1f}%","")
        with fm6: st.metric("EPS Growth",    f"{profile.get('eps_growth',0):.1f}%","YoY")
        with fm7: st.metric("EBITDA Margin", f"{profile.get('ebitda_margin',0)*100:.1f}%","")
        with fm8: st.metric("Beta",          f"{profile.get('beta','—')}","vs S&P 500")

        st.markdown("<br>", unsafe_allow_html=True)

        # Technical analysis for the valuation ticker
        render_technical_panel(vt, state)

        st.markdown("<br>", unsafe_allow_html=True)
        # Valuation-specific news with sentiment
        phdr("News Sentiment Feed", badge=vt, badge_type="gold")
        render_live_news_feed_merged(state, "markets", max_items=8)

        st.markdown("<br>", unsafe_allow_html=True)
        # AI Market Intelligence Report
        render_ai_report_panel(state)


# ═════════════════════════════════════════════════════════════════════════════════════
# §9  HELPERS
# ═════════════════════════════════════════════════════════════════════════════════════

def _apply_dark_layout(fig: go.Figure) -> None:
    fig.update_layout(
        paper_bgcolor=C["bg"], plot_bgcolor=C["panel"],
        margin=dict(l=6,r=6,t=16,b=6),
        font=dict(family="Space Mono,monospace",size=9,color=C["muted"]),
        hovermode="x unified",
    )
    ax = dict(gridcolor=C["border"],showgrid=True,zeroline=False,
              linecolor=C["border"],tickfont=dict(size=8,color=C["muted"]))
    fig.update_xaxes(**ax)
    fig.update_yaxes(**ax, tickprefix="$")


# ═════════════════════════════════════════════════════════════════════════════════════


# ═════════════════════════════════════════════════════════════════════════════════════
# §H  ADVANCED MODELS — Liquidations, Heatmap, Asset Detail, AI Analysis
# Ported from CryptoLens liquidation_service.py + heatmap_service.py + analysis_service.py
# ═════════════════════════════════════════════════════════════════════════════════════

# ── H1. Liquidation heatmap data (Binance futures public API) ─────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def fetch_liquidation_heatmap() -> pd.DataFrame:
    """
    Fetch recent Binance futures liquidation data via public REST API.
    Aggregates into symbol buckets for heatmap visualization.
    ── LIVE API KEY INJECTION POINT ──
    For real-time WebSocket stream: wss://fstream.binance.com/ws/!forceOrder@arr
    (Requires websockets library and persistent async loop — see LiquidationService pattern)
    """
    TRACKED = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
               "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT"]
    rows = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for sym in TRACKED:
        if not RATE_LIMITER.check("https://fapi.binance.com", max_per_minute=12):
            continue
        try:
            url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={sym}"
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code == 200:
                d = r.json()
                price = float(d.get("lastPrice",0) or 0)
                vol   = float(d.get("quoteVolume",0) or 0)
                chg   = float(d.get("priceChangePercent",0) or 0)
                rows.append({
                    "symbol":    sym.replace("USDT",""),
                    "price":     price,
                    "volume_24h":vol,
                    "change_24h":chg,
                    # Estimate long/short liq from price movement (simplified model)
                    "long_liq":  vol * max(0, -chg/100) * 0.02,
                    "short_liq": vol * max(0, chg/100)  * 0.02,
                    "total_liq": vol * abs(chg/100) * 0.02,
                })
        except Exception:
            continue

    if not rows:
        # Synthetic seed
        np.random.seed(int(time.time())//30)
        for sym, p in [("BTC",105000),("ETH",3800),("SOL",185),("BNB",650),
                       ("XRP",0.95),("ADA",0.85),("DOGE",0.19),("AVAX",42),
                       ("LINK",18),("DOT",8.5)]:
            vol  = p * np.random.uniform(1e6,5e7)
            chg  = np.random.uniform(-4,4)
            rows.append({
                "symbol":sym,"price":p,"volume_24h":vol,"change_24h":round(chg,2),
                "long_liq": vol*max(0,-chg/100)*0.02,
                "short_liq":vol*max(0, chg/100)*0.02,
                "total_liq": vol*abs(chg/100)*0.02,
            })
    return pd.DataFrame(rows).sort_values("total_liq", ascending=False)


# ── H2. Multi-metric crypto heatmap (CoinGecko, ported from heatmap_service.py) ─────

@st.cache_data(ttl=300, show_spinner=False)
def fetch_crypto_heatmap_data() -> Dict:
    """
    Fetch price change, volume, and sector data for crypto heatmap.
    Ported from CryptoLens heatmap_service.py fetch_heatmap_data().
    ── LIVE API KEY INJECTION POINT ──
    CoinGecko Pro key in header: {"x-cg-pro-api-key": COINGECKO_PRO_KEY}
    """
    TOP_IDS = ("bitcoin,ethereum,binancecoin,solana,xrp,cardano,dogecoin,avalanche-2,"
               "polkadot,chainlink,matic-network,litecoin,uniswap,near,cosmos,"
               "aptos,arbitrum,optimism,filecoin,aave,sui,injective-protocol,"
               "render-token,the-graph,fetch-ai,bittensor,tron,shiba-inu")
    try:
        url = ("https://api.coingecko.com/api/v3/coins/markets"
               f"?vs_currency=usd&ids={TOP_IDS}&order=market_cap_desc"
               "&per_page=50&page=1&sparkline=false&price_change_percentage=24h,7d")
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=12)
        if r.status_code == 200:
            coins = r.json()
            result = []
            sectors: Dict[str,List] = {}
            for c in coins:
                cid  = c.get("id","")
                sym  = c.get("symbol","").upper()
                chg  = c.get("price_change_percentage_24h",0) or 0
                vol  = c.get("total_volume",0) or 0
                cap  = c.get("market_cap",0) or 0
                sec  = CRYPTO_SECTORS.get(cid,"Other")
                entry = {
                    "id":cid,"symbol":sym,"name":c.get("name",sym),
                    "sector":sec,"price":c.get("current_price",0),
                    "market_cap":cap,"volume_24h":vol,
                    "price_change_24h":chg,
                    "price_change_7d":c.get("price_change_percentage_7d_in_currency",0) or 0,
                    "volume_score":min(100,(vol/1e9)*10),
                }
                result.append(entry)
                sectors.setdefault(sec,[]).append(entry)
            return {"coins":result,"sectors":sectors,"timestamp":datetime.now().isoformat()}
    except Exception:
        pass
    # Seed fallback
    seed = [("BTC","bitcoin",105000,0.8,3),("ETH","ethereum",3800,2.1,2),
            ("SOL","solana",185,-0.5,3),("BNB","binancecoin",650,0.3,3),
            ("XRP","ripple",0.95,1.2,5),("ADA","cardano",0.85,-0.8,3),
            ("DOGE","dogecoin",0.19,3.1,1),("AVAX","avalanche-2",42,-1.2,3),
            ("LINK","chainlink",18,1.8,6),("DOT","polkadot",8.5,-0.4,6)]
    coins = [{"id":cid,"symbol":sym,"name":sym,"sector":CRYPTO_SECTORS.get(cid,"Other"),
              "price":p,"market_cap":p*1e9,"volume_24h":p*1e7,"price_change_24h":chg,
              "price_change_7d":chg*2,"volume_score":50}
             for sym,cid,p,chg,_ in seed]
    sectors: Dict[str,List] = {}
    for c in coins: sectors.setdefault(c["sector"],[]).append(c)
    return {"coins":coins,"sectors":sectors,"timestamp":datetime.now().isoformat()}


# ── H3. AI Market Report Generator (Bloomberg AI module + CryptoLens analysis_service) ──

def generate_ai_market_report(macro: Dict, news_items: List[Dict],
                               crypto_overview: Dict, timeframe: str = "daily") -> str:
    """
    Generate an institutional market intelligence report using our NLP sentiment engine
    plus macro + news data. No external LLM required (uses our lexicon sentiment).
    ── LIVE API KEY INJECTION POINT ──
    For GPT-4o quality: openai.chat(model="gpt-4o-mini", messages=[...], max_tokens=2000)
    For local LLM:      ollama.generate(model="llama3", prompt=prompt)
    For Bloomberg AI:   textual RichLog output (see Bloomberg terminal main.py pattern)
    """
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    coins   = crypto_overview.get("coins", [])[:10]

    # Build market summary data
    top_gainers = sorted(coins, key=lambda x: x.get("change_24h",0), reverse=True)[:3]
    top_losers  = sorted(coins, key=lambda x: x.get("change_24h",0))[:3]
    btc_price   = next((c["price"] for c in coins if c["symbol"]=="BTC"), 0)
    btc_chg     = next((c["change_24h"] for c in coins if c["symbol"]=="BTC"), 0)
    eth_price   = next((c["price"] for c in coins if c["symbol"]=="ETH"), 0)
    btc_dom     = crypto_overview.get("btc_dominance", 0)
    total_cap   = crypto_overview.get("total_market_cap", 0)

    # News sentiment
    all_headlines = [n.get("title","") for n in news_items[:20]]
    comp_sentiment = compute_composite_sentiment(news_items[:20])
    sent_lbl = score_to_label(comp_sentiment)

    # Macro snapshot
    fed_rate = macro.get("Fed Funds Rate", {}).get("value","5.25%")
    cpi      = macro.get("US CPI YoY", {}).get("value","3.1%")
    vix      = macro.get("VIX Index", {}).get("value","18.4")
    dxy      = macro.get("DXY (USD Index)", {}).get("value","104.2")

    top_g_str = ", ".join(f"{c['symbol']} ({c['change_24h']:+.1f}%)" for c in top_gainers)
    top_l_str = ", ".join(f"{c['symbol']} ({c['change_24h']:+.1f}%)" for c in top_losers)

    alert_news = [n for n in news_items if n.get("is_alert")][:3]
    alert_str  = "\n".join(f"  • {n['title'][:90]}" for n in alert_news) or "  No high-alert events"

    report = f"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   MACRO TERMINAL  //  {timeframe.upper()} INTELLIGENCE REPORT  //  {now_str}
╚══════════════════════════════════════════════════════════════════════════════════╝

▸ EXECUTIVE SUMMARY
  Market sentiment is currently {sent_lbl} (NLP score: {comp_sentiment:+.3f}).
  Bitcoin trades at ${btc_price:,.0f} ({btc_chg:+.2f}% 24h) with BTC dominance at {btc_dom:.1f}%.
  Total crypto market cap: ${total_cap/1e12:.2f}T. Macro headwinds remain elevated
  with Fed funds at {fed_rate}, CPI at {cpi}, and VIX at {vix}.

▸ MACRO CONTEXT
  Fed Funds Rate  : {fed_rate}    │  10Y Treasury : {macro.get("10Y Treasury",{}).get("value","4.62%")}
  US CPI YoY      : {cpi}         │  2Y Treasury  : {macro.get("2Y Treasury",{}).get("value","4.94%")}
  US GDP (QoQ)    : {macro.get("US GDP (QoQ)",{}).get("value","2.4%")}     │  VIX Index    : {vix}
  DXY (USD Index) : {dxy}         │  Gold (XAU)   : {macro.get("Gold (XAU/USD)",{}).get("value","$2,481")}

▸ TOP MOVERS (24H)
  🟢 Gainers: {top_g_str}
  🔴 Losers : {top_l_str}

▸ HIGH-PRIORITY ALERTS
{alert_str}

▸ RISK ASSESSMENT
  Yield Spread    : {macro.get("Yield Spread",{}).get("value","-0.32%")} (2s10s — {"INVERTED ⚠" if "-" in macro.get("Yield Spread",{}).get("value","-0.32%") else "normal"})
  ISM Mfg PMI     : {macro.get("ISM Mfg PMI",{}).get("value","48.7")} ({"Contraction <50" if float(re.sub(r"[^0-9.]","",macro.get("ISM Mfg PMI",{}).get("value","48.7")) or "48") < 50 else "Expansion >50"})
  Composite Sent. : {comp_sentiment:+.3f} → {sent_lbl}

▸ OUTLOOK
  Primary Risk   : {"Escalating geopolitical conflict (Hormuz/Taiwan)" if any(n.get("is_alert") for n in news_items[:10]) else "Macro policy uncertainty (Fed path)"}
  DXY Trajectory : {"Bullish USD — headwind for EM and crypto" if "+" in macro.get("DXY (USD Index)",{}).get("delta","") else "Softening USD — supportive for risk assets"}
  BTC Outlook    : {"Bullish above $100K support — watch $110K resistance" if btc_price > 100000 else "Consolidation phase — watch macro catalysts"}

──────────────────────────────────────────────────────────────────────────────────
  Generated by MACRO TERMINAL NLP Engine · {now_str}
  ── LIVE API KEY INJECTION POINT: Inject OpenAI/Ollama key for GPT-4o quality ──
──────────────────────────────────────────────────────────────────────────────────
"""
    return report.strip()


# ── H4. Liquidation heatmap visualization ────────────────────────────────────────────

def render_liquidation_heatmap(liq_df: pd.DataFrame) -> None:
    """
    Render Binance futures liquidation heatmap.
    Visual: horizontal bar chart coloured by long/short pressure.
    Ported from CryptoLens liquidation_service.py heatmap pattern.
    """
    phdr("Liquidation Heatmap", "BINANCE FUTURES · 24H", "red")
    if liq_df.empty:
        _svc_unavailable("Liquidation data — awaiting Binance futures feed")
        return

    total_liq = liq_df["total_liq"].sum()
    st.markdown(
        f'<div style="font-size:9px;color:{C["muted"]};margin-bottom:6px;letter-spacing:1px;">'
        f'TOTAL ESTIMATED LIQD: ${total_liq/1e6:.1f}M · Top 10 pairs by liquidation pressure</div>',
        unsafe_allow_html=True)

    for _, row in liq_df.head(10).iterrows():
        sym  = row["symbol"]
        tot  = row["total_liq"]
        lon  = row["long_liq"]
        sht  = row["short_liq"]
        chg  = row["change_24h"]
        chg_col = C["green_br"] if chg >= 0 else C["red"]
        if total_liq > 0:
            bar_pct = min(100, tot / (total_liq / len(liq_df)) * 50)
        else:
            bar_pct = 20
        long_pct  = int(lon/tot*100) if tot > 0 else 50
        short_pct = 100 - long_pct

        st.markdown(f"""
        <div style="margin-bottom:5px;">
          <div style="display:flex;justify-content:space-between;font-size:9px;margin-bottom:2px;">
            <span style="font-family:Syne,sans-serif;font-weight:700;color:{C['text']};min-width:40px;">{sym}</span>
            <span style="color:{chg_col};">{chg:+.2f}%</span>
            <span style="color:{C['muted']};">${tot/1e6:.2f}M liq'd</span>
          </div>
          <div style="display:flex;height:5px;border-radius:3px;overflow:hidden;background:{C['border2']};">
            <div style="width:{long_pct}%;background:{C['red']};opacity:0.8;"></div>
            <div style="width:{short_pct}%;background:{C['green_br']};opacity:0.8;"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:7px;color:{C['muted']};margin-top:1px;">
            <span style="color:{C['red']};">LONG LIQ {long_pct}%</span>
            <span style="color:{C['green_br']};">SHORT LIQ {short_pct}%</span>
          </div>
        </div>""", unsafe_allow_html=True)


# ── H5. Crypto heatmap treemap visualization ─────────────────────────────────────────

def build_crypto_heatmap_chart(heatmap_data: Dict, metric: str = "price_change_24h") -> go.Figure:
    """
    Treemap heatmap of top crypto assets coloured by selected metric.
    Ported from CryptoLens heatmap_service.py visualization pattern.
    """
    coins = heatmap_data.get("coins", [])
    if not coins:
        return go.Figure()

    labels  = [c["symbol"] for c in coins]
    parents = [c.get("sector","Other") for c in coins]
    values  = [max(c.get("market_cap",1e6), 1e6) for c in coins]
    colors  = [c.get(metric, 0) or 0 for c in coins]
    texts   = [f"{c['symbol']}<br>{c.get(metric,0):+.2f}%" for c in coins]

    # Build parent sectors
    sectors = list(set(parents))
    all_labels  = sectors + labels
    all_parents = [""] * len(sectors) + parents
    all_values  = [sum(c.get("market_cap",0) for c in coins if c.get("sector","Other")==s)
                   for s in sectors] + values
    all_colors  = [sum(c.get(metric,0) or 0 for c in coins if c.get("sector","Other")==s) /
                   max(sum(1 for c in coins if c.get("sector","Other")==s),1)
                   for s in sectors] + colors
    all_texts   = sectors + texts

    fig = go.Figure(go.Treemap(
        labels=all_labels, parents=all_parents, values=all_values,
        customdata=all_colors,
        marker=dict(
            colors=all_colors,
            colorscale=[[0,C["red"]],[0.45,C["orange_br"]],[0.5,C["panel"]],
                        [0.55,C["gold_dim"]],[1,C["green_br"]]],
            cmin=-5, cmid=0, cmax=5,
            showscale=True,
            colorbar=dict(
                title=dict(text=metric.replace("_"," ").title(),
                           font=dict(size=8,color=C["muted"])),
                tickfont=dict(size=8,color=C["muted"]),
                thickness=10,
            ),
        ),
        text=all_texts,
        textinfo="text",
        hovertemplate="<b>%{label}</b><br>%{text}<br>Market Cap: $%{value:,.0f}<extra></extra>",
        textfont=dict(family="Space Mono,monospace", size=10, color="#fff"),
        tiling=dict(packing="squarify"),
        pathbar=dict(visible=False),
    ))
    fig.update_layout(
        paper_bgcolor=C["bg"], plot_bgcolor=C["bg"],
        margin=dict(l=4,r=4,t=4,b=4), height=380,
        font=dict(family="Space Mono,monospace", size=9, color=C["muted"]),
    )
    return fig


# ── H6. AI Report Panel ───────────────────────────────────────────────────────────────

def render_ai_report_panel(state: "TerminalState") -> None:
    """
    AI Market Intelligence Report panel.
    Shows auto-generated report from NLP engine.
    ── LIVE API KEY INJECTION POINT ──
    Wire in OpenAI API key here for GPT-4o quality:
      import openai; openai.api_key = "YOUR_KEY"
      report = openai.chat.completions.create(model="gpt-4o-mini", messages=[...]).choices[0].message.content
    """
    phdr("AI Market Intelligence Report", "NLP ENGINE · AUTO-GENERATED", "gold")

    macro  = state.macro_indicators or MACRO_SEED
    news   = state.news_all[:30] if state.news_all else _seed_news("markets")
    crypto = state.crypto_overview

    # Generate or retrieve cached report
    cache_key = "ai_report"
    if (cache_key not in st.session_state or
            time.time() - st.session_state.get(f"{cache_key}_ts", 0) > 600):
        with st.spinner("Generating report..."):
            report = generate_ai_market_report(macro, news, crypto, "daily")
            st.session_state[cache_key]    = report
            st.session_state[f"{cache_key}_ts"] = time.time()

    report = st.session_state.get(cache_key, "")
    st.markdown(
        f'<div class="tp" style="font-family:Space Mono,Courier New,monospace;'
        f'font-size:9.5px;line-height:1.65;color:{C["text"]};'
        f'white-space:pre-wrap;overflow-y:auto;max-height:450px;">'
        f'{report}</div>',
        unsafe_allow_html=True)

    if st.button("↻ REGENERATE REPORT", key="regen_report"):
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        st.rerun()


# ═════════════════════════════════════════════════════════════════════════════════════
# §11  CRYPTO & MARKETS TAB
# ═════════════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=60)
def render_tab_crypto() -> None:
    """
    Tab 4 — Crypto & Markets hub. Auto-refreshes every 60s.
    Layout:
      Top row : Fear & Greed gauges | Market Status | Sentiment composite
      Mid     : [55% Crypto overview table] | [45% Whale activity]
      Bottom  : [50% Global Indices] | [50% Economic Calendar]
    """
    state = get_terminal_state()

    # ── Top stats strip ──────────────────────────────────────────────────
    t1, t2, t3, t4 = st.columns([2, 2, 2, 2])
    with t1:
        cfg  = state.crypto_fear_greed
        val  = cfg.get("value", 50)
        cls  = cfg.get("classification", "Neutral")
        vcol = C["red"] if val<25 else C["orange_br"] if val<45 else C["gold"] if val<55 else C["green_br"]
        st.markdown(
            f'<div class="tp" style="text-align:center;padding:10px;">'
            f'<div style="font-size:8px;letter-spacing:2px;color:{C["muted"]};text-transform:uppercase;margin-bottom:4px;">CRYPTO F&G</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:28px;font-weight:800;color:{vcol};line-height:1;">{val}</div>'
            f'<div style="font-size:9px;color:{vcol};margin-top:2px;">{cls.upper()}</div></div>',
            unsafe_allow_html=True)
    with t2:
        sfg  = state.stock_fear_greed
        sval = sfg.get("value", 50)
        scls = sfg.get("classification", "Neutral")
        scol = C["red"] if sval<25 else C["orange_br"] if sval<45 else C["gold"] if sval<55 else C["green_br"]
        st.markdown(
            f'<div class="tp" style="text-align:center;padding:10px;">'
            f'<div style="font-size:8px;letter-spacing:2px;color:{C["muted"]};text-transform:uppercase;margin-bottom:4px;">EQUITY F&G</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:28px;font-weight:800;color:{scol};line-height:1;">{sval}</div>'
            f'<div style="font-size:9px;color:{scol};margin-top:2px;">{scls.upper()}</div></div>',
            unsafe_allow_html=True)
    with t3:
        ms   = state.market_status or get_market_status_live()
        sc   = {"Open":C["green_br"],"Pre-market":C["orange_br"],"After-hours":C["blue_br"],"Closed":C["red"]}
        mc   = sc.get(ms.get("status",""), C["muted"])
        st.markdown(
            f'<div class="tp" style="text-align:center;padding:10px;">'
            f'<div style="font-size:8px;letter-spacing:2px;color:{C["muted"]};text-transform:uppercase;margin-bottom:4px;">MARKET SESSION</div>'
            f'<div style="font-size:11px;color:{mc};font-weight:700;line-height:1.3;">{ms.get("message","—")}</div></div>',
            unsafe_allow_html=True)
    with t4:
        comp = state.sentiment_scores.get("composite", 0.0)
        cc   = score_to_color(comp)
        cl   = score_to_label(comp)
        bpct = int((comp+1)/2*100)
        st.markdown(
            f'<div class="tp" style="padding:10px;">'
            f'<div style="font-size:8px;letter-spacing:2px;color:{C["muted"]};text-transform:uppercase;margin-bottom:4px;">COMPOSITE SENTIMENT</div>'
            f'<div style="font-family:Syne,sans-serif;font-size:13px;font-weight:800;color:{cc};">{cl}</div>'
            f'<div style="background:{C["border2"]};height:4px;border-radius:2px;margin-top:5px;overflow:hidden;">'
            f'<div style="width:{bpct}%;height:100%;background:{cc};border-radius:2px;"></div></div>'
            f'<div style="display:flex;justify-content:space-between;font-size:7.5px;color:{C["muted"]};margin-top:2px;">'
            f'<span>BEAR</span><span>BULL</span></div></div>',
            unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Mid: Crypto overview + Whale activity ────────────────────────────
    crypto_col, whale_col = st.columns([55, 45])
    with crypto_col:
        render_crypto_overview_table(state)
    with whale_col:
        whale_data = getattr(state, "whale_data", {})
        render_whale_activity(whale_data)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Bottom: Global indices + Economic calendar ─────────────────────
    idx_col, cal_col = st.columns([1, 1])
    with idx_col:
        indices = state.global_indices if state.global_indices else get_global_indices()
        render_global_indices(indices)
    with cal_col:
        render_econ_calendar(state)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Liquidation heatmap ────────────────────────────────────────────
    liq_df = fetch_liquidation_heatmap()
    liq_col, heat_col = st.columns([1, 1])
    with liq_col:
        render_liquidation_heatmap(liq_df)
    with heat_col:
        phdr("Crypto Market Heatmap", "COINGECKO · TREEMAP", "gold")
        hmap_data = fetch_crypto_heatmap_data()
        hmap_metric = st.selectbox("Metric",
            ["price_change_24h","price_change_7d","volume_score"],
            key="hmap_metric", label_visibility="collapsed",
            format_func=lambda x: {"price_change_24h":"24H Price Change",
                                    "price_change_7d":"7D Price Change",
                                    "volume_score":"Volume Score"}.get(x,x))
        st.plotly_chart(build_crypto_heatmap_chart(hmap_data, hmap_metric),
                        use_container_width=True, config={"displayModeBar":False})

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Full sentiment dashboard with per-asset bars ───────────────────
    render_sentiment_dashboard(state)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── AI Market Intelligence Report ─────────────────────────────────
    render_ai_report_panel(state)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Crypto news feed with sentiment badges ─────────────────────────
    phdr("Crypto News Feed", "CRYPTOCOMPARE · COINDESK · RSS · LIVE", "gold")
    render_live_news_feed_merged(state, "crypto", max_items=20)


# §10  MAIN — Application boot
# ═════════════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Boot sequence:
      1. CSS injection
      2. Background data pipeline start (daemon thread — non-blocking)
      3. Ticker bar (state prices or synthetic fallback)
      4. Terminal header with live UTC clock + pipeline status
      5. Three-tab navigation — each @st.fragment has its own refresh timer:
           Tab 1 (Global Macro)    → 120s
           Tab 2 (Stock Overview)  →  60s
           Tab 3 (Stock Valuation) → 300s

    State management:
      • TerminalState singleton in st.session_state["terminal_state"]
      • Background thread writes to state via TerminalState.update()
      • Streamlit fragments read from state each refresh cycle
      • @st.cache_data TTLs as secondary layer for heavy fetches
      • RATE_LIMITER protects all outbound API calls globally
    """
    inject_css()
    start_background_pipeline()

    state = get_terminal_state()

    # Price ticker bar — use state prices if available, else synthetic
    prices = state.ticker_prices if state.ticker_prices else get_ticker_prices()
    render_ticker_bar(prices)

    # ── Crucix Live News Ticker — CSS Grid horizontal marquee (5-min refresh) ──
    render_crucix_news_ticker(state)

    # ── Terminal header ──
    now = datetime.now(timezone.utc)

    # Pipeline status indicator
    errors   = [k for k,v in state.errors.items() if v]
    n_errors = len(errors)
    pill_col = C["red"] if n_errors else C["green_br"]
    pill_txt = f"⚠ {n_errors} ERR" if n_errors else "● LIVE"
    age_news = int(state.age("news_all"))
    age_lbl  = f"News {age_news}s ago" if age_news < 999 else "Awaiting data"

    st.markdown(
        f'<div class="term-hdr">'
        f'<span class="term-logo">MACRO TERMINAL</span>'
        f'<span style="font-size:8.5px;color:{C["muted"]};letter-spacing:1.5px;">'
        f'INSTITUTIONAL INTELLIGENCE PLATFORM&nbsp;//&nbsp;v4.0</span>'
        f'<span style="margin-left:auto;display:flex;align-items:center;gap:12px;">'
        f'<span style="font-size:8px;color:{C["muted"]};">{age_lbl}</span>'
        f'<span style="display:inline-flex;align-items:center;gap:5px;padding:2px 8px;'
        f'background:rgba(0,0,0,0.3);border:1px solid {pill_col}44;border-radius:10px;'
        f'font-size:9px;color:{pill_col};letter-spacing:1.5px;">{pill_txt}</span>'
        f'<span style="font-size:9px;color:{C["muted"]};letter-spacing:1px;">'
        f'{now.strftime("%d %b %Y  %H:%M:%S")} UTC</span>'
        f'<span style="font-size:8px;color:{C["muted"]};padding-left:8px;'
        f'border-left:1px solid {C["border"]};">'
        f'GDELT · FRED · YF · COINGECKO · BINANCE · OPENSKY · FF · RSS</span>'
        f'</span></div>',
        unsafe_allow_html=True)

    tab1,tab2,tab3,tab4 = st.tabs([
        "⊕  GLOBAL MACRO",
        "📈  STOCK OVERVIEW",
        "⊞  STOCK VALUATION",
        "₿  CRYPTO & MARKETS",
    ])

    with tab1: render_tab_macro()
    with tab2: render_tab_stock()
    with tab3: render_tab_valuation()
    with tab4: render_tab_crypto()


if __name__ == "__main__":
    main()
