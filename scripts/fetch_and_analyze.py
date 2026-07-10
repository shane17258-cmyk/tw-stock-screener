#!/usr/bin/env python3
"""台股月線/周線選股系統 - 數據擷取與分析"""

import json
import os
import sys
import time
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(ROOT_DIR, 'docs', 'data')
DETAIL_DIR = os.path.join(OUTPUT_DIR, 'detail')
CACHE_DIR = os.path.join(ROOT_DIR, '.cache')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DETAIL_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

MAX_WORKERS = 5
MIN_TRADING_DAYS = 500
MA_PERIODS = [5, 10, 20, 60, 120]
COLORS = {'MA5': '#FF6B6B', 'MA10': '#FFD93D', 'MA20': '#6BCB77', 'MA60': '#4D96FF', 'MA120': '#9B59B6'}

FALLBACK_STOCKS = [
    {"symbol": "1101", "name": "台泥"}, {"symbol": "1102", "name": "亞泥"}, {"symbol": "1201", "name": "味全"},
    {"symbol": "1216", "name": "統一"}, {"symbol": "1220", "name": "台榮"}, {"symbol": "1225", "name": "福懋油"},
    {"symbol": "1227", "name": "佳格"}, {"symbol": "1229", "name": "聯華"}, {"symbol": "1232", "name": "大統益"},
    {"symbol": "1234", "name": "黑松"}, {"symbol": "1301", "name": "臺塑"}, {"symbol": "1303", "name": "南亞"},
    {"symbol": "1304", "name": "臺聚"}, {"symbol": "1305", "name": "華夏"}, {"symbol": "1307", "name": "三芳"},
    {"symbol": "1308", "name": "亞聚"}, {"symbol": "1309", "name": "台達化"}, {"symbol": "1310", "name": "台苯"},
    {"symbol": "1312", "name": "國喬"}, {"symbol": "1313", "name": "聯成"}, {"symbol": "1314", "name": "中石化"},
    {"symbol": "1319", "name": "東陽"}, {"symbol": "1324", "name": "地球"}, {"symbol": "1326", "name": "臺化"},
    {"symbol": "1337", "name": "再生"}, {"symbol": "1340", "name": "勝悅"}, {"symbol": "1341", "name": "富林"},
    {"symbol": "1402", "name": "遠東新"}, {"symbol": "1409", "name": "新纖"}, {"symbol": "1410", "name": "南染"},
    {"symbol": "1413", "name": "宏洲"}, {"symbol": "1414", "name": "東和"}, {"symbol": "1416", "name": "廣豐"},
    {"symbol": "1417", "name": "嘉裕"}, {"symbol": "1418", "name": "東華"}, {"symbol": "1419", "name": "新紡"},
    {"symbol": "1423", "name": "利華"}, {"symbol": "1432", "name": "大魯閣"}, {"symbol": "1434", "name": "福懋"},
    {"symbol": "1435", "name": "中福"}, {"symbol": "1436", "name": "華友聯"}, {"symbol": "1437", "name": "勤益控"},
    {"symbol": "1438", "name": "三地開發"}, {"symbol": "1439", "name": "意德士"}, {"symbol": "1440", "name": "南紡"},
    {"symbol": "1441", "name": "大東"}, {"symbol": "1442", "name": "名軒"}, {"symbol": "1443", "name": "立益"},
    {"symbol": "1446", "name": "宏和"}, {"symbol": "1447", "name": "力鵬"}, {"symbol": "1449", "name": "佳和"},
    {"symbol": "1451", "name": "年興"}, {"symbol": "1452", "name": "宏益"}, {"symbol": "1453", "name": "大將"},
    {"symbol": "1454", "name": "台富"}, {"symbol": "1455", "name": "集盛"}, {"symbol": "1456", "name": "怡華"},
    {"symbol": "1457", "name": "宜進"}, {"symbol": "1459", "name": "聯發"}, {"symbol": "1460", "name": "宏遠"},
    {"symbol": "1463", "name": "強盛"}, {"symbol": "1464", "name": "得力"}, {"symbol": "1465", "name": "偉全"},
    {"symbol": "1466", "name": "聚隆"}, {"symbol": "1467", "name": "德細"}, {"symbol": "1468", "name": "昶和"},
    {"symbol": "1470", "name": "大統新創"}, {"symbol": "1471", "name": "首利"}, {"symbol": "1472", "name": "三洋實業"},
    {"symbol": "1473", "name": "台南"}, {"symbol": "1474", "name": "弘裕"}, {"symbol": "1475", "name": "本業"},
    {"symbol": "1476", "name": "儒鴻"}, {"symbol": "1477", "name": "聚陽"}, {"symbol": "1503", "name": "士電"},
    {"symbol": "1504", "name": "東元"}, {"symbol": "1513", "name": "中興電"}, {"symbol": "1514", "name": "亞力"},
    {"symbol": "1515", "name": "力山"}, {"symbol": "1516", "name": "川飛"}, {"symbol": "1517", "name": "利奇"},
    {"symbol": "1519", "name": "華城"}, {"symbol": "1521", "name": "大億"}, {"symbol": "1522", "name": "堤維西"},
    {"symbol": "1524", "name": "耿鼎"}, {"symbol": "1525", "name": "江申"}, {"symbol": "1526", "name": "日馳"},
    {"symbol": "1527", "name": "鑽全"}, {"symbol": "1528", "name": "恩德"}, {"symbol": "1529", "name": "樂事綠能"},
    {"symbol": "1530", "name": "亞崴"}, {"symbol": "1531", "name": "高林股"}, {"symbol": "1532", "name": "勤美"},
    {"symbol": "1533", "name": "車王電"}, {"symbol": "1535", "name": "中宇"}, {"symbol": "1536", "name": "和大"},
    {"symbol": "1537", "name": "廣隆"}, {"symbol": "1538", "name": "正峰"}, {"symbol": "1539", "name": "巨庭"},
    {"symbol": "1540", "name": "喬福"}, {"symbol": "1541", "name": "錩泰"}, {"symbol": "1558", "name": "伸興"},
    {"symbol": "1560", "name": "中砂"}, {"symbol": "1563", "name": "巧新"}, {"symbol": "1568", "name": "倉佑"},
    {"symbol": "1582", "name": "信錦"}, {"symbol": "1583", "name": "程泰"}, {"symbol": "1587", "name": "吉茂"},
    {"symbol": "1589", "name": "永冠"}, {"symbol": "1590", "name": "亞德客"}, {"symbol": "1592", "name": "聖暉"},
    {"symbol": "1597", "name": "直得"}, {"symbol": "1603", "name": "華電"}, {"symbol": "1604", "name": "聲寶"},
    {"symbol": "1605", "name": "華新"}, {"symbol": "1608", "name": "華榮"}, {"symbol": "1609", "name": "大亞"},
    {"symbol": "1611", "name": "中電"}, {"symbol": "1612", "name": "宏泰"}, {"symbol": "1614", "name": "三洋電"},
    {"symbol": "1615", "name": "大山"}, {"symbol": "1616", "name": "億泰"}, {"symbol": "1617", "name": "榮星"},
    {"symbol": "1618", "name": "合機"}, {"symbol": "1626", "name": "艾美特"}, {"symbol": "1701", "name": "中化"},
    {"symbol": "1702", "name": "南僑"}, {"symbol": "1707", "name": "葡萄王"}, {"symbol": "1708", "name": "東鹼"},
    {"symbol": "1709", "name": "和益"}, {"symbol": "1710", "name": "東聯"}, {"symbol": "1711", "name": "永光"},
    {"symbol": "1712", "name": "興農"}, {"symbol": "1713", "name": "國化"}, {"symbol": "1714", "name": "和桐"},
    {"symbol": "1715", "name": "亞化"}, {"symbol": "1717", "name": "長興"}, {"symbol": "1718", "name": "中纖"},
    {"symbol": "1720", "name": "生達"}, {"symbol": "1721", "name": "三晃"}, {"symbol": "1722", "name": "台肥"},
    {"symbol": "1723", "name": "中碳"}, {"symbol": "1725", "name": "元禎"}, {"symbol": "1726", "name": "永記"},
    {"symbol": "1727", "name": "中華化"}, {"symbol": "1730", "name": "花仙子"}, {"symbol": "1731", "name": "美吾華"},
    {"symbol": "1732", "name": "毛寶"}, {"symbol": "1733", "name": "五鼎"}, {"symbol": "1734", "name": "杏輝"},
    {"symbol": "1735", "name": "日勝化"}, {"symbol": "1736", "name": "喬山"}, {"symbol": "1737", "name": "臺鹽"},
    {"symbol": "1752", "name": "南光"}, {"symbol": "1762", "name": "中化生"}, {"symbol": "1773", "name": "勝一"},
    {"symbol": "1776", "name": "展宇"}, {"symbol": "1783", "name": "和康生"}, {"symbol": "1786", "name": "科妍"},
    {"symbol": "1789", "name": "神隆"}, {"symbol": "1795", "name": "美時"}, {"symbol": "1802", "name": "臺玻"},
    {"symbol": "1805", "name": "寶徠"}, {"symbol": "1806", "name": "冠軍"}, {"symbol": "1808", "name": "潤隆"},
    {"symbol": "1809", "name": "中釉"}, {"symbol": "1810", "name": "和成"}, {"symbol": "1817", "name": "凱撒衛"},
    {"symbol": "1902", "name": "台紙"}, {"symbol": "1903", "name": "士紙"}, {"symbol": "1904", "name": "正隆"},
    {"symbol": "1905", "name": "華紙"}, {"symbol": "1906", "name": "寶隆"}, {"symbol": "1907", "name": "永豐餘"},
    {"symbol": "1962", "name": "豐興"}, {"symbol": "1968", "name": "臻鼎"}, {"symbol": "2002", "name": "中鋼"},
    {"symbol": "2006", "name": "東和鋼鐵"}, {"symbol": "2007", "name": "燁興"}, {"symbol": "2008", "name": "高興昌"},
    {"symbol": "2009", "name": "第一銅"}, {"symbol": "2010", "name": "春源"}, {"symbol": "2012", "name": "春雨"},
    {"symbol": "2013", "name": "中鋼構"}, {"symbol": "2014", "name": "中鴻"}, {"symbol": "2015", "name": "豐興"},
    {"symbol": "2017", "name": "官田鋼"}, {"symbol": "2020", "name": "美亞"}, {"symbol": "2022", "name": "聚亨"},
    {"symbol": "2023", "name": "燁輝"}, {"symbol": "2024", "name": "志聯"}, {"symbol": "2025", "name": "千興"},
    {"symbol": "2027", "name": "大成鋼"}, {"symbol": "2028", "name": "威致"}, {"symbol": "2029", "name": "盛餘"},
    {"symbol": "2030", "name": "彰源"}, {"symbol": "2031", "name": "新光鋼"}, {"symbol": "2032", "name": "新鋼"},
    {"symbol": "2033", "name": "佳大"}, {"symbol": "2034", "name": "允強"}, {"symbol": "2038", "name": "海光"},
    {"symbol": "2049", "name": "上銀"}, {"symbol": "2059", "name": "川湖"}, {"symbol": "2062", "name": "橋椿"},
    {"symbol": "2066", "name": "宏佳騰"}, {"symbol": "2101", "name": "南港"}, {"symbol": "2102", "name": "泰豐"},
    {"symbol": "2103", "name": "台橡"}, {"symbol": "2104", "name": "國際中橡"}, {"symbol": "2105", "name": "正新"},
    {"symbol": "2106", "name": "建大"}, {"symbol": "2107", "name": "厚生"}, {"symbol": "2108", "name": "南帝"},
    {"symbol": "2109", "name": "華豐"}, {"symbol": "2114", "name": "鑫永銓"}, {"symbol": "2115", "name": "六暉"},
    {"symbol": "2201", "name": "裕隆"}, {"symbol": "2204", "name": "中華"}, {"symbol": "2206", "name": "三陽工業"},
    {"symbol": "2207", "name": "和泰車"}, {"symbol": "2208", "name": "台船"}, {"symbol": "2211", "name": "長榮鋼"},
    {"symbol": "2227", "name": "裕日車"}, {"symbol": "2228", "name": "劍麟"}, {"symbol": "2231", "name": "為升"},
    {"symbol": "2233", "name": "宇隆"}, {"symbol": "2236", "name": "百達"}, {"symbol": "2239", "name": "英利"},
    {"symbol": "2241", "name": "艾姆勒"}, {"symbol": "2243", "name": "宏旭"}, {"symbol": "2250", "name": "IKKA"},
    {"symbol": "2254", "name": "巨鎧"}, {"symbol": "2258", "name": "台銘"}, {"symbol": "2301", "name": "光寶科"},
    {"symbol": "2302", "name": "麗正"}, {"symbol": "2303", "name": "聯電"}, {"symbol": "2305", "name": "全友"},
    {"symbol": "2308", "name": "台達電"}, {"symbol": "2312", "name": "金寶"}, {"symbol": "2313", "name": "華通"},
    {"symbol": "2314", "name": "台揚"}, {"symbol": "2316", "name": "楠梓電"}, {"symbol": "2317", "name": "鴻海"},
    {"symbol": "2321", "name": "東訊"}, {"symbol": "2323", "name": "中環"}, {"symbol": "2324", "name": "仁寶"},
    {"symbol": "2327", "name": "國巨"}, {"symbol": "2328", "name": "廣宇"}, {"symbol": "2329", "name": "華泰"},
    {"symbol": "2330", "name": "台積電"}, {"symbol": "2331", "name": "精英"}, {"symbol": "2332", "name": "友訊"},
    {"symbol": "2337", "name": "旺宏"}, {"symbol": "2338", "name": "光罩"}, {"symbol": "2340", "name": "台亞"},
    {"symbol": "2342", "name": "茂矽"}, {"symbol": "2344", "name": "華邦電"}, {"symbol": "2345", "name": "智邦"},
    {"symbol": "2347", "name": "聯強"}, {"symbol": "2348", "name": "海悅"}, {"symbol": "2349", "name": "錸德"},
    {"symbol": "2351", "name": "順德"}, {"symbol": "2352", "name": "佳世達"}, {"symbol": "2353", "name": "宏碁"},
    {"symbol": "2354", "name": "鴻準"}, {"symbol": "2355", "name": "敬鵬"}, {"symbol": "2356", "name": "英業達"},
    {"symbol": "2357", "name": "華碩"}, {"symbol": "2358", "name": "廷鑫"}, {"symbol": "2359", "name": "所羅門"},
    {"symbol": "2360", "name": "致茂"}, {"symbol": "2362", "name": "藍天"}, {"symbol": "2363", "name": "矽統"},
    {"symbol": "2364", "name": "倫飛"}, {"symbol": "2365", "name": "昆盈"}, {"symbol": "2367", "name": "燿華"},
    {"symbol": "2368", "name": "金像電"}, {"symbol": "2369", "name": "菱生"}, {"symbol": "2371", "name": "大同"},
    {"symbol": "2373", "name": "震旦行"}, {"symbol": "2374", "name": "佳能"}, {"symbol": "2375", "name": "凱美"},
    {"symbol": "2376", "name": "技嘉"}, {"symbol": "2377", "name": "微星"}, {"symbol": "2379", "name": "瑞昱"},
    {"symbol": "2380", "name": "虹光"}, {"symbol": "2382", "name": "廣達"}, {"symbol": "2383", "name": "台光電"},
    {"symbol": "2385", "name": "群光"}, {"symbol": "2387", "name": "精元"}, {"symbol": "2388", "name": "威盛"},
    {"symbol": "2390", "name": "云辰"}, {"symbol": "2392", "name": "正崴"}, {"symbol": "2393", "name": "億光"},
    {"symbol": "2395", "name": "研華"}, {"symbol": "2397", "name": "友通"}, {"symbol": "2399", "name": "映泰"},
    {"symbol": "2401", "name": "凌陽"}, {"symbol": "2402", "name": "毅嘉"}, {"symbol": "2404", "name": "漢唐"},
    {"symbol": "2405", "name": "輔信"}, {"symbol": "2406", "name": "國碩"}, {"symbol": "2408", "name": "南亞科"},
    {"symbol": "2409", "name": "友達"}, {"symbol": "2412", "name": "中華電"}, {"symbol": "2413", "name": "環科"},
    {"symbol": "2414", "name": "精技"}, {"symbol": "2415", "name": "錩新"}, {"symbol": "2417", "name": "圓剛"},
    {"symbol": "2419", "name": "仲琦"}, {"symbol": "2420", "name": "新巨"}, {"symbol": "2421", "name": "建準"},
    {"symbol": "2423", "name": "固緯"}, {"symbol": "2424", "name": "隴華"}, {"symbol": "2425", "name": "承啟"},
    {"symbol": "2426", "name": "鼎元"}, {"symbol": "2427", "name": "三商電"}, {"symbol": "2428", "name": "興勤"},
    {"symbol": "2429", "name": "銘旺科"}, {"symbol": "2430", "name": "燦坤"}, {"symbol": "2431", "name": "聯昌"},
    {"symbol": "2432", "name": "倚天酷碁"}, {"symbol": "2433", "name": "互盛電"}, {"symbol": "2434", "name": "統懋"},
    {"symbol": "2436", "name": "偉詮電"}, {"symbol": "2438", "name": "翔耀"}, {"symbol": "2439", "name": "美律"},
    {"symbol": "2440", "name": "太空梭"}, {"symbol": "2441", "name": "超豐"}, {"symbol": "2442", "name": "新美齊"},
    {"symbol": "2443", "name": "昶虹"}, {"symbol": "2444", "name": "兆勁"}, {"symbol": "2449", "name": "京元電子"},
    {"symbol": "2450", "name": "神腦"}, {"symbol": "2451", "name": "創見"}, {"symbol": "2453", "name": "凌群"},
    {"symbol": "2454", "name": "聯發科"}, {"symbol": "2455", "name": "全新"}, {"symbol": "2456", "name": "奇力新"},
    {"symbol": "2457", "name": "飛宏"}, {"symbol": "2458", "name": "義隆"}, {"symbol": "2459", "name": "敦吉"},
    {"symbol": "2460", "name": "建通"}, {"symbol": "2461", "name": "光群雷"}, {"symbol": "2462", "name": "良得電"},
    {"symbol": "2464", "name": "盟立"}, {"symbol": "2465", "name": "麗臺"}, {"symbol": "2466", "name": "冠西電"},
    {"symbol": "2467", "name": "志聖"}, {"symbol": "2468", "name": "華經"}, {"symbol": "2471", "name": "資通"},
    {"symbol": "2472", "name": "立隆電"}, {"symbol": "2474", "name": "可成"}, {"symbol": "2476", "name": "鉅祥"},
    {"symbol": "2477", "name": "美隆電"}, {"symbol": "2478", "name": "大毅"}, {"symbol": "2480", "name": "敦陽科"},
    {"symbol": "2481", "name": "強茂"}, {"symbol": "2482", "name": "連宇"}, {"symbol": "2483", "name": "百容"},
    {"symbol": "2484", "name": "希華"}, {"symbol": "2485", "name": "兆赫"}, {"symbol": "2486", "name": "一詮"},
    {"symbol": "2488", "name": "漢平"}, {"symbol": "2489", "name": "瑞軒"}, {"symbol": "2491", "name": "吉祥全"},
    {"symbol": "2492", "name": "華新科"}, {"symbol": "2493", "name": "揚博"}, {"symbol": "2495", "name": "普安"},
    {"symbol": "2496", "name": "卓越"}, {"symbol": "2497", "name": "怡利電"}, {"symbol": "2498", "name": "宏達電"},
    {"symbol": "2499", "name": "東貝"}, {"symbol": "2501", "name": "國建"}, {"symbol": "2504", "name": "國產"},
    {"symbol": "2505", "name": "國揚"}, {"symbol": "2506", "name": "太設"}, {"symbol": "2509", "name": "全坤建"},
    {"symbol": "2511", "name": "太子"}, {"symbol": "2514", "name": "龍邦"}, {"symbol": "2515", "name": "中工"},
    {"symbol": "2516", "name": "新建"}, {"symbol": "2520", "name": "冠德"}, {"symbol": "2524", "name": "京城"},
    {"symbol": "2527", "name": "宏璟"}, {"symbol": "2528", "name": "皇普"}, {"symbol": "2530", "name": "華建"},
    {"symbol": "2534", "name": "宏盛"}, {"symbol": "2535", "name": "達欣工"}, {"symbol": "2536", "name": "宏普"},
    {"symbol": "2537", "name": "聯上發"}, {"symbol": "2538", "name": "基泰"}, {"symbol": "2539", "name": "櫻花建"},
    {"symbol": "2540", "name": "愛山林"}, {"symbol": "2542", "name": "興富發"}, {"symbol": "2543", "name": "皇昌"},
    {"symbol": "2545", "name": "皇翔"}, {"symbol": "2546", "name": "根基"}, {"symbol": "2547", "name": "日勝生"},
    {"symbol": "2548", "name": "華固"}, {"symbol": "2549", "name": "富邦媒"}, {"symbol": "2552", "name": "怡華"},
    {"symbol": "2553", "name": "鼎基"}, {"symbol": "2555", "name": "惠普"}, {"symbol": "2557", "name": "潤弘"},
    {"symbol": "2558", "name": "帝寶"}, {"symbol": "2562", "name": "潤泰新"}, {"symbol": "2563", "name": "潤泰全"},
    {"symbol": "2577", "name": "達麗"}, {"symbol": "2581", "name": "富旺"}, {"symbol": "2596", "name": "綠意"},
    {"symbol": "2601", "name": "益航"}, {"symbol": "2603", "name": "長榮"}, {"symbol": "2605", "name": "新興"},
    {"symbol": "2606", "name": "裕民"}, {"symbol": "2607", "name": "榮運"}, {"symbol": "2608", "name": "嘉里大榮"},
    {"symbol": "2609", "name": "陽明"}, {"symbol": "2610", "name": "華航"}, {"symbol": "2611", "name": "長榮航"},
    {"symbol": "2612", "name": "中航"}, {"symbol": "2613", "name": "中櫃"}, {"symbol": "2614", "name": "東森"},
    {"symbol": "2615", "name": "萬海"}, {"symbol": "2616", "name": "山隆"}, {"symbol": "2617", "name": "台航"},
    {"symbol": "2618", "name": "長榮航太"}, {"symbol": "2630", "name": "亞航"}, {"symbol": "2633", "name": "臺灣高鐵"},
    {"symbol": "2634", "name": "漢翔"}, {"symbol": "2636", "name": "台驊"}, {"symbol": "2637", "name": "慧洋"},
    {"symbol": "2642", "name": "宅配通"}, {"symbol": "2645", "name": "長榮航勤"}, {"symbol": "2701", "name": "萬企"},
    {"symbol": "2702", "name": "華園"}, {"symbol": "2704", "name": "國賓"}, {"symbol": "2705", "name": "六福"},
    {"symbol": "2706", "name": "第一店"}, {"symbol": "2707", "name": "晶華"}, {"symbol": "2712", "name": "遠雄來"},
    {"symbol": "2718", "name": "晶悅"}, {"symbol": "2719", "name": "遠雄"}, {"symbol": "2722", "name": "夏都"},
    {"symbol": "2723", "name": "美食"}, {"symbol": "2727", "name": "王品"}, {"symbol": "2731", "name": "雄獅"},
    {"symbol": "2739", "name": "寒舍"}, {"symbol": "2748", "name": "雲品"}, {"symbol": "2752", "name": "豆府"},
    {"symbol": "2753", "name": "八方雲集"}, {"symbol": "2762", "name": "世界健身"}, {"symbol": "2801", "name": "彰銀"},
    {"symbol": "2809", "name": "京城銀"}, {"symbol": "2812", "name": "臺中銀"}, {"symbol": "2816", "name": "旺旺保"},
    {"symbol": "2820", "name": "華票"}, {"symbol": "2823", "name": "中壽"}, {"symbol": "2832", "name": "合庫金"},
    {"symbol": "2834", "name": "臺企銀"}, {"symbol": "2836", "name": "高雄銀"}, {"symbol": "2838", "name": "聯邦銀"},
    {"symbol": "2845", "name": "遠東銀"}, {"symbol": "2849", "name": "安泰銀"}, {"symbol": "2850", "name": "新產"},
    {"symbol": "2851", "name": "中再保"}, {"symbol": "2852", "name": "第一保"}, {"symbol": "2855", "name": "統一證"},
    {"symbol": "2867", "name": "三商壽"}, {"symbol": "2880", "name": "華南金"}, {"symbol": "2881", "name": "富邦金"},
    {"symbol": "2882", "name": "國泰金"}, {"symbol": "2883", "name": "開發金"}, {"symbol": "2884", "name": "玉山金"},
    {"symbol": "2885", "name": "元大金"}, {"symbol": "2886", "name": "兆豐金"}, {"symbol": "2887", "name": "台新金"},
    {"symbol": "2888", "name": "新光金"}, {"symbol": "2889", "name": "國票金"}, {"symbol": "2890", "name": "永豐金"},
    {"symbol": "2891", "name": "中信金"}, {"symbol": "2892", "name": "第一金"}, {"symbol": "2897", "name": "王道銀"},
    {"symbol": "2901", "name": "欣欣"}, {"symbol": "2903", "name": "遠百"}, {"symbol": "2904", "name": "匯僑"},
    {"symbol": "2905", "name": "三商"}, {"symbol": "2906", "name": "高林"}, {"symbol": "2908", "name": "特力"},
    {"symbol": "2910", "name": "統領"}, {"symbol": "2911", "name": "麗嬰房"}, {"symbol": "2912", "name": "統一超"},
    {"symbol": "2913", "name": "台灣農林"}, {"symbol": "2915", "name": "潤泰全"}, {"symbol": "2916", "name": "滿心"},
    {"symbol": "2923", "name": "鼎固"}, {"symbol": "2929", "name": "淘帝"}, {"symbol": "2939", "name": "凱羿"},
    {"symbol": "2945", "name": "三能"}, {"symbol": "3002", "name": "歐格"}, {"symbol": "3003", "name": "健和興"},
    {"symbol": "3004", "name": "豐達科"}, {"symbol": "3005", "name": "神基"}, {"symbol": "3006", "name": "晶豪科"},
    {"symbol": "3008", "name": "大立光"}, {"symbol": "3010", "name": "華立"}, {"symbol": "3011", "name": "今皓"},
    {"symbol": "3013", "name": "晟銘電"}, {"symbol": "3014", "name": "聯陽"}, {"symbol": "3015", "name": "全漢"},
    {"symbol": "3016", "name": "嘉晶"}, {"symbol": "3017", "name": "奇鋐"}, {"symbol": "3018", "name": "隆銘綠能"},
    {"symbol": "3019", "name": "亞光"}, {"symbol": "3021", "name": "鴻名"}, {"symbol": "3022", "name": "威強電"},
    {"symbol": "3023", "name": "信邦"}, {"symbol": "3024", "name": "憶聲"}, {"symbol": "3025", "name": "星通"},
    {"symbol": "3026", "name": "禾伸堂"}, {"symbol": "3027", "name": "盛達"}, {"symbol": "3028", "name": "增你強"},
    {"symbol": "3029", "name": "零壹"}, {"symbol": "3030", "name": "德律"}, {"symbol": "3031", "name": "佰鴻"},
    {"symbol": "3032", "name": "偉訓"}, {"symbol": "3033", "name": "威健"}, {"symbol": "3034", "name": "聯詠"},
    {"symbol": "3035", "name": "智原"}, {"symbol": "3036", "name": "文曄"}, {"symbol": "3037", "name": "欣興"},
    {"symbol": "3038", "name": "全台"}, {"symbol": "3040", "name": "遠見"}, {"symbol": "3041", "name": "揚智"},
    {"symbol": "3042", "name": "晶技"}, {"symbol": "3043", "name": "科風"}, {"symbol": "3044", "name": "健鼎"},
    {"symbol": "3045", "name": "台灣大"}, {"symbol": "3046", "name": "建碁"}, {"symbol": "3047", "name": "訊舟"},
    {"symbol": "3048", "name": "益登"}, {"symbol": "3049", "name": "精金"}, {"symbol": "3050", "name": "鈺德"},
    {"symbol": "3051", "name": "力特"}, {"symbol": "3052", "name": "夆典"}, {"symbol": "3054", "name": "立萬"},
    {"symbol": "3055", "name": "蔚華科"}, {"symbol": "3056", "name": "總太"}, {"symbol": "3057", "name": "喬鼎"},
    {"symbol": "3058", "name": "立德"}, {"symbol": "3059", "name": "華晶科"}, {"symbol": "3060", "name": "銘異"},
    {"symbol": "3062", "name": "建漢"}, {"symbol": "3063", "name": "飛捷"}, {"symbol": "3064", "name": "泰偉"},
    {"symbol": "3066", "name": "李長榮"}, {"symbol": "3067", "name": "全域"}, {"symbol": "3070", "name": "奇鈦科"},
    {"symbol": "3071", "name": "協禧"}, {"symbol": "3073", "name": "普格"}, {"symbol": "3078", "name": "僑威"},
    {"symbol": "3080", "name": "佳能"}, {"symbol": "3081", "name": "聯亞"}, {"symbol": "3085", "name": "新零售"},
    {"symbol": "3086", "name": "華義"}, {"symbol": "3088", "name": "艾訊"}, {"symbol": "3090", "name": "日電貿"},
    {"symbol": "3092", "name": "鴻碩"}, {"symbol": "3094", "name": "台硝"}, {"symbol": "3095", "name": "及成"},
    {"symbol": "3096", "name": "越峰"}, {"symbol": "3105", "name": "穩懋"}, {"symbol": "3110", "name": "日電貿"},
    {"symbol": "3114", "name": "好德"}, {"symbol": "3115", "name": "富榮"}, {"symbol": "3116", "name": "鈺創"},
    {"symbol": "3117", "name": "年程"}, {"symbol": "3118", "name": "進階"}, {"symbol": "3122", "name": "笙泉"},
    {"symbol": "3126", "name": "信億"}, {"symbol": "3128", "name": "昇銳"}, {"symbol": "3130", "name": "一零四"},
    {"symbol": "3131", "name": "弘塑"}, {"symbol": "3132", "name": "大昌"}, {"symbol": "3136", "name": "融程電"},
    {"symbol": "3141", "name": "晶宏"}, {"symbol": "3147", "name": "大綜"}, {"symbol": "3149", "name": "正達"},
    {"symbol": "3150", "name": "鈺寶"}, {"symbol": "3151", "name": "智易"}, {"symbol": "3152", "name": "璟德"},
    {"symbol": "3162", "name": "惠特"}, {"symbol": "3163", "name": "波若威"}, {"symbol": "3164", "name": "景岳"},
    {"symbol": "3167", "name": "大量"}, {"symbol": "3168", "name": "眾福"}, {"symbol": "3169", "name": "亞信"},
    {"symbol": "3171", "name": "新洲"}, {"symbol": "3176", "name": "基亞"}, {"symbol": "3178", "name": "公準"},
    {"symbol": "3189", "name": "景碩"}, {"symbol": "3191", "name": "和進"}, {"symbol": "3202", "name": "樺晟"},
    {"symbol": "3206", "name": "志豐"}, {"symbol": "3209", "name": "全科"}, {"symbol": "3211", "name": "順達"},
    {"symbol": "3213", "name": "茂訊"}, {"symbol": "3217", "name": "優群"}, {"symbol": "3218", "name": "大學光"},
    {"symbol": "3219", "name": "倚強"}, {"symbol": "3221", "name": "台嘉碩"}, {"symbol": "3224", "name": "MetaAge"},
    {"symbol": "3226", "name": "至寶電"}, {"symbol": "3227", "name": "原相"}, {"symbol": "3228", "name": "金麗科"},
    {"symbol": "3229", "name": "晟鈦"}, {"symbol": "3230", "name": "錦明"}, {"symbol": "3231", "name": "緯創"},
    {"symbol": "3232", "name": "昱捷"}, {"symbol": "3234", "name": "光環"}, {"symbol": "3236", "name": "千如"},
    {"symbol": "3252", "name": "海灣"}, {"symbol": "3257", "name": "虹冠電"}, {"symbol": "3259", "name": "鑫創"},
    {"symbol": "3260", "name": "威剛"}, {"symbol": "3264", "name": "欣銓"}, {"symbol": "3265", "name": "台星科"},
    {"symbol": "3266", "name": "昇陽"}, {"symbol": "3268", "name": "海德威"}, {"symbol": "3272", "name": "東碩"},
    {"symbol": "3276", "name": "宇環"}, {"symbol": "3284", "name": "元太"}, {"symbol": "3285", "name": "微端"},
    {"symbol": "3287", "name": "廣寰科"}, {"symbol": "3288", "name": "點晶"}, {"symbol": "3289", "name": "宜特"},
    {"symbol": "3290", "name": "東浦"}, {"symbol": "3293", "name": "鈊象"}, {"symbol": "3294", "name": "英濟"},
    {"symbol": "3296", "name": "勝德"}, {"symbol": "3297", "name": "杭特"}, {"symbol": "3305", "name": "昇貿"},
    {"symbol": "3308", "name": "聯德"}, {"symbol": "3310", "name": "佳穎"}, {"symbol": "3311", "name": "閎暉"},
    {"symbol": "3312", "name": "弘憶股"}, {"symbol": "3313", "name": "斐成"}, {"symbol": "3314", "name": "微矽"},
    {"symbol": "3315", "name": "旭品"}, {"symbol": "3317", "name": "尼克森"}, {"symbol": "3321", "name": "同泰"},
    {"symbol": "3322", "name": "建舜電"}, {"symbol": "3323", "name": "加百裕"}, {"symbol": "3324", "name": "雙鴻"},
    {"symbol": "3325", "name": "旭品"}, {"symbol": "3338", "name": "泰碩"}, {"symbol": "3339", "name": "華廣"},
    {"symbol": "3346", "name": "麗清"}, {"symbol": "3349", "name": "寶德"}, {"symbol": "3356", "name": "奇偶"},
    {"symbol": "3357", "name": "臺慶科"}, {"symbol": "3360", "name": "尚立"}, {"symbol": "3362", "name": "先進光"},
    {"symbol": "3363", "name": "上詮"}, {"symbol": "3367", "name": "英華達"}, {"symbol": "3372", "name": "典範"},
    {"symbol": "3373", "name": "熱映"}, {"symbol": "3374", "name": "精材"}, {"symbol": "3376", "name": "新日興"},
    {"symbol": "3379", "name": "彬台"}, {"symbol": "3380", "name": "明泰"}, {"symbol": "3383", "name": "新世紀"},
    {"symbol": "3388", "name": "崇越電"}, {"symbol": "3390", "name": "旭軟"}, {"symbol": "3402", "name": "漢科"},
    {"symbol": "3406", "name": "玉晶光"}, {"symbol": "3413", "name": "京鼎"}, {"symbol": "3416", "name": "融程電"},
    {"symbol": "3419", "name": "譁裕"}, {"symbol": "3426", "name": "台興"}, {"symbol": "3432", "name": "台端"},
    {"symbol": "3437", "name": "榮創"}, {"symbol": "3441", "name": "聯一光"}, {"symbol": "3443", "name": "創意"},
    {"symbol": "3444", "name": "利機"}, {"symbol": "3447", "name": "展達"}, {"symbol": "3450", "name": "聯鈞"},
    {"symbol": "3454", "name": "晶睿"}, {"symbol": "3455", "name": "由田"}, {"symbol": "3465", "name": "進泰電子"},
    {"symbol": "3466", "name": "致振"}, {"symbol": "3479", "name": "安勤"}, {"symbol": "3481", "name": "群創"},
    {"symbol": "3483", "name": "力致"}, {"symbol": "3484", "name": "崧騰"}, {"symbol": "3489", "name": "森寶"},
    {"symbol": "3490", "name": "單井"}, {"symbol": "3491", "name": "昇達科"}, {"symbol": "3492", "name": "長盛"},
    {"symbol": "3494", "name": "誠研"}, {"symbol": "3498", "name": "陽程"}, {"symbol": "3501", "name": "維熹"},
    {"symbol": "3504", "name": "揚明光"}, {"symbol": "3508", "name": "位速"}, {"symbol": "3515", "name": "華擎"},
    {"symbol": "3516", "name": "亞帝歐"}, {"symbol": "3518", "name": "柏騰"}, {"symbol": "3519", "name": "綠能"},
    {"symbol": "3520", "name": "振維"}, {"symbol": "3521", "name": "鴻翊"}, {"symbol": "3522", "name": "御頂"},
    {"symbol": "3523", "name": "迎輝"}, {"symbol": "3526", "name": "凡甲"}, {"symbol": "3527", "name": "聚積"},
    {"symbol": "3528", "name": "安馳"}, {"symbol": "3529", "name": "力旺"}, {"symbol": "3530", "name": "晶相光"},
    {"symbol": "3532", "name": "台勝科"}, {"symbol": "3533", "name": "嘉澤"}, {"symbol": "3535", "name": "晶彩科"},
    {"symbol": "3536", "name": "誠創"}, {"symbol": "3537", "name": "堡達"}, {"symbol": "3540", "name": "曜越"},
    {"symbol": "3541", "name": "西柏"}, {"symbol": "3543", "name": "州巧"}, {"symbol": "3545", "name": "敦泰"},
    {"symbol": "3546", "name": "宇峻"}, {"symbol": "3548", "name": "兆利"}, {"symbol": "3550", "name": "聯穎"},
    {"symbol": "3551", "name": "世禾"}, {"symbol": "3552", "name": "同致"}, {"symbol": "3555", "name": "重鳥鵬"},
    {"symbol": "3556", "name": "禾瑞亞"}, {"symbol": "3557", "name": "嘉威"}, {"symbol": "3558", "name": "神準"},
    {"symbol": "3563", "name": "牧德"}, {"symbol": "3564", "name": "其陽"}, {"symbol": "3567", "name": "逸昌"},
    {"symbol": "3570", "name": "大塚"}, {"symbol": "3576", "name": "聯合再生"}, {"symbol": "3577", "name": "泓格"},
    {"symbol": "3580", "name": "友威科"}, {"symbol": "3583", "name": "辛耘"}, {"symbol": "3587", "name": "閎康"},
    {"symbol": "3588", "name": "通嘉"}, {"symbol": "3591", "name": "奕力"}, {"symbol": "3592", "name": "瑞鼎"},
    {"symbol": "3593", "name": "力銘"}, {"symbol": "3594", "name": "磐儀"}, {"symbol": "3595", "name": "山太士"},
    {"symbol": "3596", "name": "智易"}, {"symbol": "3597", "name": "映興"}, {"symbol": "3605", "name": "宏致"},
    {"symbol": "3607", "name": "谷崧"}, {"symbol": "3609", "name": "三一東林"}, {"symbol": "3611", "name": "鼎翰"},
    {"symbol": "3615", "name": "安可"}, {"symbol": "3617", "name": "碩天"}, {"symbol": "3622", "name": "洋華"},
    {"symbol": "3623", "name": "富晶通"}, {"symbol": "3624", "name": "光頡"}, {"symbol": "3625", "name": "西勝"},
    {"symbol": "3627", "name": "華信科"}, {"symbol": "3628", "name": "盈正"}, {"symbol": "3630", "name": "新鉅科"},
    {"symbol": "3631", "name": "晟田"}, {"symbol": "3632", "name": "研勤"}, {"symbol": "3638", "name": "IML"},
    {"symbol": "3645", "name": "達邁"}, {"symbol": "3646", "name": "艾恩特"}, {"symbol": "3652", "name": "精聯"},
    {"symbol": "3653", "name": "健策"}, {"symbol": "3658", "name": "中探針"}, {"symbol": "3661", "name": "世芯"},
    {"symbol": "3664", "name": "光耀"}, {"symbol": "3665", "name": "貿聯"}, {"symbol": "3669", "name": "圓展"},
    {"symbol": "3672", "name": "康聯訊"}, {"symbol": "3673", "name": "TPK"}, {"symbol": "3675", "name": "德微"},
    {"symbol": "3678", "name": "聯享"}, {"symbol": "3679", "name": "新至陞"}, {"symbol": "3680", "name": "家登"},
    {"symbol": "3682", "name": "亞太電"}, {"symbol": "3684", "name": "榮昌"}, {"symbol": "3685", "name": "政翔"},
    {"symbol": "3686", "name": "達能"}, {"symbol": "3687", "name": "歐買尬"}, {"symbol": "3688", "name": "華立捷"},
    {"symbol": "3689", "name": "湧德"}, {"symbol": "3690", "name": "友輝"}, {"symbol": "3691", "name": "碩禾"},
    {"symbol": "3693", "name": "營邦"}, {"symbol": "3694", "name": "海華"}, {"symbol": "3698", "name": "隆達"},
    {"symbol": "3701", "name": "大眾控"}, {"symbol": "3702", "name": "大聯大"}, {"symbol": "3703", "name": "欣陸"},
    {"symbol": "3704", "name": "合勤控"}, {"symbol": "3705", "name": "永信"}, {"symbol": "3706", "name": "神達"},
    {"symbol": "3707", "name": "漢磊"}, {"symbol": "3708", "name": "上緯投控"}, {"symbol": "3709", "name": "鑫科"},
    {"symbol": "3710", "name": "連展投控"}, {"symbol": "3711", "name": "日月光投控"}, {"symbol": "3712", "name": "永崴投控"},
    {"symbol": "3713", "name": "新晶投控"}, {"symbol": "3714", "name": "富采"}, {"symbol": "3715", "name": "定穎投控"},
    {"symbol": "3716", "name": "中化投控"}, {"symbol": "4104", "name": "東生華"}, {"symbol": "4105", "name": "台灣東洋"},
    {"symbol": "4106", "name": "雃博"}, {"symbol": "4107", "name": "邦特"}, {"symbol": "4108", "name": "懷特"},
    {"symbol": "4109", "name": "加捷生醫"}, {"symbol": "4110", "name": "喬鼎"}, {"symbol": "4111", "name": "濟生"},
    {"symbol": "4112", "name": "仁新"}, {"symbol": "4113", "name": "聯上"}, {"symbol": "4114", "name": "健喬"},
    {"symbol": "4115", "name": "善德"}, {"symbol": "4116", "name": "明基醫"}, {"symbol": "4117", "name": "普生"},
    {"symbol": "4118", "name": "臺醫"}, {"symbol": "4119", "name": "旭富"}, {"symbol": "4120", "name": "友華"},
    {"symbol": "4121", "name": "優盛"}, {"symbol": "4122", "name": "國光生"}, {"symbol": "4123", "name": "晟德"},
    {"symbol": "4124", "name": "佳醫"}, {"symbol": "4125", "name": "太醫"}, {"symbol": "4126", "name": "太景"},
    {"symbol": "4127", "name": "天良"}, {"symbol": "4128", "name": "中天"}, {"symbol": "4129", "name": "聯合"},
    {"symbol": "4130", "name": "健亞"}, {"symbol": "4131", "name": "浩泰"}, {"symbol": "4132", "name": "國鼎"},
    {"symbol": "4133", "name": "亞諾法"}, {"symbol": "4134", "name": "欣耀"}, {"symbol": "4135", "name": "麗彤"},
    {"symbol": "4136", "name": "太和"}, {"symbol": "4137", "name": "麗豐"}, {"symbol": "4138", "name": "曜亞"},
    {"symbol": "4139", "name": "馬光"}, {"symbol": "4140", "name": "康聯"}, {"symbol": "4141", "name": "龍燈"},
    {"symbol": "4142", "name": "國光生"}, {"symbol": "4143", "name": "康呈"}, {"symbol": "4144", "name": "康聯-KY"},
    {"symbol": "4147", "name": "中裕"}, {"symbol": "4148", "name": "全宇生技"}, {"symbol": "4153", "name": "鈺緯"},
    {"symbol": "4154", "name": "康樂"}, {"symbol": "4155", "name": "訊映"}, {"symbol": "4157", "name": "太景"},
    {"symbol": "4160", "name": "創源"}, {"symbol": "4161", "name": "聿新科"}, {"symbol": "4162", "name": "智擎"},
    {"symbol": "4163", "name": "鐿鈦"}, {"symbol": "4164", "name": "承業醫"}, {"symbol": "4165", "name": "博晟生醫"},
    {"symbol": "4166", "name": "紅木"}, {"symbol": "4167", "name": "昱展"}, {"symbol": "4168", "name": "台灣銘板"},
    {"symbol": "4171", "name": "瑞基"}, {"symbol": "4172", "name": "友霖"}, {"symbol": "4173", "name": "久裕"},
    {"symbol": "4174", "name": "浩鼎"}, {"symbol": "4175", "name": "杏一"}, {"symbol": "4176", "name": "展逸"},
    {"symbol": "4177", "name": "柏登"}, {"symbol": "4178", "name": "綠茵"}, {"symbol": "4179", "name": "鑫品"},
    {"symbol": "4180", "name": "安克"}, {"symbol": "4181", "name": "華宇藥"}, {"symbol": "4182", "name": "福永"},
    {"symbol": "4183", "name": "福永生技"}, {"symbol": "4184", "name": "昕琦"}, {"symbol": "4185", "name": "ABC"},
    {"symbol": "4186", "name": "尖端醫"}, {"symbol": "4187", "name": "瑩碩生技"}, {"symbol": "4188", "name": "達邦蛋白"},
    {"symbol": "4189", "name": "膠原科技"}, {"symbol": "4190", "name": "佐登"}, {"symbol": "4191", "name": "法德藥"},
    {"symbol": "4192", "name": "北極星藥業"}, {"symbol": "4193", "name": "博謙"}, {"symbol": "4194", "name": "禾生技"},
    {"symbol": "4195", "name": "基米"}, {"symbol": "4196", "name": "冠亞"}, {"symbol": "4197", "name": "膠原科技"},
    {"symbol": "4198", "name": "欣大健康"}, {"symbol": "4199", "name": "華安"}, {"symbol": "4205", "name": "中華食"},
    {"symbol": "4207", "name": "環泰"}, {"symbol": "4306", "name": "炎洲"}, {"symbol": "4414", "name": "如興"},
    {"symbol": "4420", "name": "光明"}, {"symbol": "4426", "name": "利勤"}, {"symbol": "4430", "name": "耀億"},
    {"symbol": "4432", "name": "銘旺實"}, {"symbol": "4433", "name": "興采"}, {"symbol": "4438", "name": "廣越"},
    {"symbol": "4439", "name": "冠星"}, {"symbol": "4440", "name": "宜新"}, {"symbol": "4502", "name": "源恆"},
    {"symbol": "4506", "name": "崇友"}, {"symbol": "4510", "name": "高鋒"}, {"symbol": "4513", "name": "福裕"},
    {"symbol": "4526", "name": "東台"}, {"symbol": "4527", "name": "堤維西"}, {"symbol": "4528", "name": "江興鍛"},
    {"symbol": "4529", "name": "利汎"}, {"symbol": "4530", "name": "宏易"}, {"symbol": "4532", "name": "瑞智"},
    {"symbol": "4533", "name": "協易機"}, {"symbol": "4534", "name": "慶騰"}, {"symbol": "4535", "name": "至興"},
    {"symbol": "4536", "name": "拓凱"}, {"symbol": "4537", "name": "旭東"}, {"symbol": "4538", "name": "大詠城"},
    {"symbol": "4540", "name": "全球傳動"}, {"symbol": "4541", "name": "坤悅"}, {"symbol": "4542", "name": "科嶠"},
    {"symbol": "4543", "name": "萬在"}, {"symbol": "4544", "name": "春日"}, {"symbol": "4545", "name": "銘鈺"},
    {"symbol": "4546", "name": "長亨"}, {"symbol": "4549", "name": "桓達"}, {"symbol": "4550", "name": "長佳"},
    {"symbol": "4551", "name": "智伸科"}, {"symbol": "4552", "name": "力達"}, {"symbol": "4553", "name": "盛復"},
    {"symbol": "4554", "name": "橙的"}, {"symbol": "4555", "name": "氣立"}, {"symbol": "4556", "name": "旭暉"},
    {"symbol": "4557", "name": "永新"}, {"symbol": "4558", "name": "寶緯"}, {"symbol": "4559", "name": "元翎"},
    {"symbol": "4560", "name": "強信"}, {"symbol": "4561", "name": "健椿"}, {"symbol": "4562", "name": "穎漢"},
    {"symbol": "4563", "name": "百德"}, {"symbol": "4564", "name": "元翎"}, {"symbol": "4565", "name": "宏偉"},
    {"symbol": "4566", "name": "時碩工業"}, {"symbol": "4567", "name": "科定"}, {"symbol": "4568", "name": "科際精密"},
    {"symbol": "4569", "name": "六方科"}, {"symbol": "4570", "name": "杰力"}, {"symbol": "4571", "name": "鈺邦"},
    {"symbol": "4572", "name": "駐龍"}, {"symbol": "4573", "name": "鏵友益"}, {"symbol": "4574", "name": "鏵友益"},
    {"symbol": "4575", "name": "銓寶"}, {"symbol": "4576", "name": "大銀微系統"}, {"symbol": "4577", "name": "達航"},
    {"symbol": "4578", "name": "總格"}, {"symbol": "4580", "name": "捷流閥業"}, {"symbol": "4581", "name": "光隆精密"},
    {"symbol": "4582", "name": "聚恆"}, {"symbol": "4583", "name": "台灣精銳"}, {"symbol": "4584", "name": "君帆"},
    {"symbol": "4585", "name": "銳澤"}, {"symbol": "4586", "name": "愛派司"}, {"symbol": "4587", "name": "寶元"},
    {"symbol": "4588", "name": "玖鼎電力"}, {"symbol": "4589", "name": "碩陽"}, {"symbol": "4590", "name": "原創生醫"},
    {"symbol": "4702", "name": "中美實"}, {"symbol": "4706", "name": "大恭"}, {"symbol": "4707", "name": "磐亞"},
    {"symbol": "4711", "name": "永純"}, {"symbol": "4712", "name": "南璋"}, {"symbol": "4714", "name": "永捷"},
    {"symbol": "4716", "name": "大立"}, {"symbol": "4717", "name": "有益"}, {"symbol": "4720", "name": "德淵"},
    {"symbol": "4721", "name": "美琪瑪"}, {"symbol": "4722", "name": "國精化"}, {"symbol": "4724", "name": "宣捷"},
    {"symbol": "4725", "name": "信昌化"}, {"symbol": "4726", "name": "永昕"}, {"symbol": "4728", "name": "雙美"},
    {"symbol": "4729", "name": "熒茂"}, {"symbol": "4730", "name": "通用"}, {"symbol": "4731", "name": "豪展"},
    {"symbol": "4732", "name": "彥臣"}, {"symbol": "4733", "name": "上緯"}, {"symbol": "4735", "name": "豪展"},
    {"symbol": "4736", "name": "泰博"}, {"symbol": "4737", "name": "華廣"}, {"symbol": "4738", "name": "尚化"},
    {"symbol": "4739", "name": "康普"}, {"symbol": "4740", "name": "華上"}, {"symbol": "4741", "name": "泓瀚"},
    {"symbol": "4743", "name": "合一"}, {"symbol": "4744", "name": "皇將"}, {"symbol": "4745", "name": "合富"},
    {"symbol": "4746", "name": "台耀"}, {"symbol": "4747", "name": "強生"}, {"symbol": "4748", "name": "立康"},
    {"symbol": "4749", "name": "新應材"}, {"symbol": "4750", "name": "商之器"}, {"symbol": "4751", "name": "景凱"},
    {"symbol": "4752", "name": "聯亞藥"}, {"symbol": "4753", "name": "漢田"}, {"symbol": "4754", "name": "國碳科"},
    {"symbol": "4755", "name": "德河"}, {"symbol": "4756", "name": "順藥"}, {"symbol": "4757", "name": "三鼎生技"},
    {"symbol": "4758", "name": "冠亞"}, {"symbol": "4760", "name": "勤凱"}, {"symbol": "4761", "name": "富動科"},
    {"symbol": "4762", "name": "永彰"}, {"symbol": "4763", "name": "材料"}, {"symbol": "4764", "name": "雙鍵"},
    {"symbol": "4765", "name": "磐采"}, {"symbol": "4766", "name": "南寶"}, {"symbol": "4767", "name": "誠泰科技"},
    {"symbol": "4768", "name": "晶呈科技"}, {"symbol": "4769", "name": "美達科技"}, {"symbol": "4770", "name": "上品"},
    {"symbol": "4771", "name": "望隼"}, {"symbol": "4772", "name": "台特化"}, {"symbol": "4773", "name": "高福"},
    {"symbol": "4774", "name": "信紘科"}, {"symbol": "4775", "name": "聚賢"}, {"symbol": "4803", "name": "網家"},
    {"symbol": "4804", "name": "大略"}, {"symbol": "4806", "name": "昇華"}, {"symbol": "4807", "name": "誠美材"},
    {"symbol": "4808", "name": "台汽電"}, {"symbol": "4809", "name": "鼎翰"}, {"symbol": "4810", "name": "中連"},
    {"symbol": "4811", "name": "元勝"}, {"symbol": "4812", "name": "百達"}, {"symbol": "4813", "name": "茂生農經"},
    {"symbol": "4814", "name": "欣雄"}, {"symbol": "4815", "name": "華鎂鑫"}, {"symbol": "4816", "name": "保瑞"},
    {"symbol": "4817", "name": "北基"}, {"symbol": "4818", "name": "龍生"}, {"symbol": "4819", "name": "旭源"},
    {"symbol": "4820", "name": "中信"}, {"symbol": "4821", "name": "宏碁智醫"}, {"symbol": "4822", "name": "福裕"},
    {"symbol": "4823", "name": "達輝"}, {"symbol": "4824", "name": "旭德"}, {"symbol": "4825", "name": "旭然"},
    {"symbol": "4826", "name": "三福化"}, {"symbol": "4827", "name": "永悅健康"}, {"symbol": "4828", "name": "宏碁創達"},
    {"symbol": "4829", "name": "宏碁智通"}, {"symbol": "4830", "name": "程曦"}, {"symbol": "4831", "name": "群利科技"},
    {"symbol": "4832", "name": "曼哈頓"}, {"symbol": "4833", "name": "國邑"}, {"symbol": "4834", "name": "博瑞達"},
    {"symbol": "4835", "name": "鑫傳"}, {"symbol": "4903", "name": "聯光通"}, {"symbol": "4904", "name": "遠傳"},
    {"symbol": "4905", "name": "臺聯電"}, {"symbol": "4906", "name": "正文"}, {"symbol": "4907", "name": "富宇"},
    {"symbol": "4908", "name": "前鼎"}, {"symbol": "4909", "name": "新復興"}, {"symbol": "4910", "name": "博智"},
    {"symbol": "4911", "name": "德英"}, {"symbol": "4912", "name": "聯德"}, {"symbol": "4915", "name": "致伸"},
    {"symbol": "4916", "name": "事欣科"}, {"symbol": "4917", "name": "商丞"}, {"symbol": "4918", "name": "F-鼎固"},
    {"symbol": "4919", "name": "新唐"}, {"symbol": "4920", "name": "勝悅"}, {"symbol": "4921", "name": "力銘"},
    {"symbol": "4923", "name": "永冠"}, {"symbol": "4924", "name": "宏碩系統"}, {"symbol": "4925", "name": "智微"},
    {"symbol": "4927", "name": "泰鼎"}, {"symbol": "4928", "name": "尚茂"}, {"symbol": "4929", "name": "晶呈科技"},
    {"symbol": "4930", "name": "燦星網"}, {"symbol": "4931", "name": "新盛力"}, {"symbol": "4933", "name": "友輝"},
    {"symbol": "4934", "name": "太極"}, {"symbol": "4935", "name": "茂林"}, {"symbol": "4936", "name": "耕興"},
    {"symbol": "4937", "name": "亞洲教育"}, {"symbol": "4938", "name": "和碩"}, {"symbol": "4939", "name": "亞電"},
    {"symbol": "4940", "name": "艾恩特"}, {"symbol": "4941", "name": "華義"}, {"symbol": "4942", "name": "嘉彰"},
    {"symbol": "4943", "name": "康控"}, {"symbol": "4944", "name": "兆遠"}, {"symbol": "4945", "name": "友威科"},
    {"symbol": "4946", "name": "辣椒"}, {"symbol": "4947", "name": "志豐"}, {"symbol": "4948", "name": "笙科"},
    {"symbol": "4949", "name": "有成"}, {"symbol": "4950", "name": "牧東"}, {"symbol": "4951", "name": "精拓科"},
    {"symbol": "4952", "name": "凌通"}, {"symbol": "4953", "name": "緯軟"}, {"symbol": "4954", "name": "光聯"},
    {"symbol": "4955", "name": "鑫科"}, {"symbol": "4956", "name": "光鋐"}, {"symbol": "4957", "name": "眾達"},
    {"symbol": "4958", "name": "臻鼎"}, {"symbol": "4959", "name": "均華"}, {"symbol": "4960", "name": "誠美材"},
    {"symbol": "4961", "name": "天鈺"}, {"symbol": "4962", "name": "雙鴻"}, {"symbol": "4963", "name": "和勤"},
    {"symbol": "4964", "name": "世界"}, {"symbol": "4965", "name": "鑫銓"}, {"symbol": "4966", "name": "譜瑞"},
    {"symbol": "4967", "name": "十銓"}, {"symbol": "4968", "name": "立積"}, {"symbol": "4969", "name": "連展"},
    {"symbol": "4970", "name": "誠品"}, {"symbol": "4971", "name": "IET"}, {"symbol": "4972", "name": "湯石"},
    {"symbol": "4973", "name": "廣穎"}, {"symbol": "4974", "name": "亞泰"}, {"symbol": "4975", "name": "水霖"},
    {"symbol": "4976", "name": "佳霖"}, {"symbol": "4977", "name": "眾達-KY"}, {"symbol": "4978", "name": "宏碁資訊"},
    {"symbol": "4979", "name": "華星光"}, {"symbol": "4980", "name": "佐臻"}, {"symbol": "4981", "name": "鄉林"},
    {"symbol": "4982", "name": "冠星"}, {"symbol": "4983", "name": "鑫聯大投控"}, {"symbol": "4984", "name": "倍力"},
    {"symbol": "4985", "name": "永彰"}, {"symbol": "4986", "name": "耕興"}, {"symbol": "4987", "name": "科誠"},
    {"symbol": "4988", "name": "湯石"}, {"symbol": "4989", "name": "榮科"}, {"symbol": "4990", "name": "晶達"},
    {"symbol": "4991", "name": "環宇"}, {"symbol": "4992", "name": "長盛"}, {"symbol": "4993", "name": "芯測"},  # truncated for display
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_stock_list():
    try:
        url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
        resp = requests.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
        data = resp.json()
        stocks = []
        seen = set()
        for item in data:
            code = str(item.get('公司代號', '')).strip()
            name = str(item.get('公司簡稱', '')).strip()
            if code.isdigit() and len(code) == 4 and code not in seen:
                seen.add(code)
                stocks.append({'symbol': code, 'name': name})
        log(f"從TWSE API取得 {len(stocks)} 檔股票")
        return stocks
    except Exception as e:
        log(f"TWSE API失敗 ({e})，使用內建清單")
        return FALLBACK_STOCKS

def download_stock(symbol):
    cache_path = os.path.join(CACHE_DIR, f"{symbol}.parquet")
    if os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        age_hours = (datetime.now().timestamp() - mtime) / 3600
        if age_hours < 24:
            try:
                df = pd.read_parquet(cache_path)
                if len(df) >= MIN_TRADING_DAYS:
                    return symbol, df
            except:
                pass
    for suffix in ['.TW', '.TWO']:
        try:
            ticker = yf.Ticker(symbol + suffix)
            df = ticker.history(period='max')
            if not df.empty and len(df) >= MIN_TRADING_DAYS:
                df.to_parquet(cache_path)
                return symbol, df
        except:
            continue
    return symbol, None

def calc_mas(df, periods=MA_PERIODS):
    ma_df = pd.DataFrame(index=df.index)
    ma_df['Close'] = df['Close']
    ma_df['Volume'] = df['Volume']
    for p in periods:
        ma_df[f'MA{p}'] = df['Close'].rolling(window=p).mean()
    return ma_df

def check_filter(ma_df):
    if len(ma_df) < 130:
        return False
    cols = ['MA5', 'MA10', 'MA20', 'MA60', 'MA120']
    latest = ma_df.iloc[-1]
    prev = ma_df.iloc[-2]
    for c in cols:
        if pd.isna(latest[c]) or pd.isna(prev[c]):
            return False
    if not (latest['MA5'] > latest['MA10'] > latest['MA20'] > latest['MA60'] > latest['MA120']):
        return False
    values = [latest[c] for c in cols]
    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            if values[j] <= 0:
                return False
            if abs(values[i] - values[j]) / values[j] >= 0.20:
                return False
    for c in cols:
        if latest[c] <= prev[c]:
            return False
    return True

def calc_max_deviation(latest):
    cols = ['MA5', 'MA10', 'MA20', 'MA60', 'MA120']
    values = [latest[c] for c in cols]
    max_dev = 0.0
    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            if values[j] > 0:
                dev = abs(values[i] - values[j]) / values[j]
                max_dev = max(max_dev, dev)
    return round(max_dev * 100, 2)

def format_chart_data(ohlc_df, ma_df, max_points=60):
    return {
        'dates': [d.strftime('%Y-%m-%d') for d in ohlc_df.index[-max_points:]],
        'open': [round(float(v), 2) for v in ohlc_df['Open'].values[-max_points:]],
        'high': [round(float(v), 2) for v in ohlc_df['High'].values[-max_points:]],
        'low': [round(float(v), 2) for v in ohlc_df['Low'].values[-max_points:]],
        'close': [round(float(v), 2) for v in ohlc_df['Close'].values[-max_points:]],
        'volume': [int(float(v)) for v in ohlc_df['Volume'].values[-max_points:]],
        'ma5': [round(float(v), 2) if not pd.isna(v) else None for v in ma_df['MA5'].values[-max_points:]],
        'ma10': [round(float(v), 2) if not pd.isna(v) else None for v in ma_df['MA10'].values[-max_points:]],
        'ma20': [round(float(v), 2) if not pd.isna(v) else None for v in ma_df['MA20'].values[-max_points:]],
        'ma60': [round(float(v), 2) if not pd.isna(v) else None for v in ma_df['MA60'].values[-max_points:]],
        'ma120': [round(float(v), 2) if not pd.isna(v) else None for v in ma_df['MA120'].values[-max_points:]],
    }

def resample_data(df):
    df_idx = pd.to_datetime(df.index)
    if hasattr(df_idx, 'tz') and df_idx.tz is not None:
        df_idx = df_idx.tz_localize(None)
    df = df.copy()
    df.index = df_idx
    monthly = df.resample('M').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()
    weekly = df.resample('W-FRI').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()
    return monthly, weekly

def process_stock(symbol, name, df):
    monthly, weekly = resample_data(df)
    m_monthly = calc_mas(monthly)
    m_weekly = calc_mas(weekly)
    monthly_pass = check_filter(m_monthly)
    weekly_pass = check_filter(m_weekly)
    if not monthly_pass and not weekly_pass:
        return None
    result = {'symbol': symbol, 'name': name, 'monthly_pass': monthly_pass, 'weekly_pass': weekly_pass, '_df': df}
    if monthly_pass:
        latest = m_monthly.iloc[-1]
        result['monthly'] = {
            'close': round(float(latest['Close']), 2),
            'ma5': round(float(latest['MA5']), 2),
            'ma10': round(float(latest['MA10']), 2),
            'ma20': round(float(latest['MA20']), 2),
            'ma60': round(float(latest['MA60']), 2),
            'ma120': round(float(latest['MA120']), 2),
            'deviation_max': calc_max_deviation(latest)
        }
        result['_monthly_data'] = format_chart_data(monthly, m_monthly)
    if weekly_pass:
        latest = m_weekly.iloc[-1]
        result['weekly'] = {
            'close': round(float(latest['Close']), 2),
            'ma5': round(float(latest['MA5']), 2),
            'ma10': round(float(latest['MA10']), 2),
            'ma20': round(float(latest['MA20']), 2),
            'ma60': round(float(latest['MA60']), 2),
            'ma120': round(float(latest['MA120']), 2),
            'deviation_max': calc_max_deviation(latest)
        }
        result['_weekly_data'] = format_chart_data(weekly, m_weekly)
    m_daily = calc_mas(df)
    result['_daily_data'] = format_chart_data(df, m_daily, max_points=252)
    return result

def main():
    log("=== 台股月線/周線選股系統 ===")
    stocks = get_stock_list()
    total = len(stocks)
    log(f"總共 {total} 檔股票待分析")

    monthly_results = []
    weekly_results = []
    detail_stocks = []
    processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock, s['symbol']): s for s in stocks}
        for future in as_completed(futures):
            s = futures[future]
            try:
                symbol, df = future.result()
                if df is not None:
                    result = process_stock(symbol, s['name'], df)
                    if result:
                        if result.get('monthly_pass'):
                            monthly_results.append({
                                'symbol': result['symbol'], 'name': result['name'], **result['monthly']
                            })
                        if result.get('weekly_pass'):
                            weekly_results.append({
                                'symbol': result['symbol'], 'name': result['name'], **result['weekly']
                            })
                        if result.get('monthly_pass') or result.get('weekly_pass'):
                            detail_stocks.append(result)
                processed += 1
                if processed % 50 == 0:
                    log(f"已處理 {processed}/{total} 檔...")
            except Exception as e:
                log(f"處理 {s['symbol']} 錯誤: {e}")
                processed += 1

    summary = {
        'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_analyzed': total,
        'monthly_count': len(monthly_results),
        'weekly_count': len(weekly_results),
        'monthly': monthly_results,
        'weekly': weekly_results
    }
    for r in detail_stocks:
        r.pop('_df', None)
        r.pop('_daily_data', None)
        r.pop('_monthly_data', None)
        r.pop('_weekly_data', None)
    with open(os.path.join(OUTPUT_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log(f"摘要已儲存: 月線 {len(monthly_results)} 檔, 周線 {len(weekly_results)} 檔")

    for result in detail_stocks:
        detail = {'symbol': result['symbol'], 'name': result['name']}
        if result.get('_daily_data'):
            detail['daily'] = result['_daily_data']
        if result.get('_monthly_data'):
            detail['monthly'] = result['_monthly_data']
        elif '_df' in result:
            monthly, _ = resample_data(result['_df'])
            detail['monthly'] = format_chart_data(monthly, calc_mas(monthly))
        if result.get('_weekly_data'):
            detail['weekly'] = result['_weekly_data']
        elif '_df' in result:
            _, weekly = resample_data(result['_df'])
            detail['weekly'] = format_chart_data(weekly, calc_mas(weekly))
        with open(os.path.join(DETAIL_DIR, f"{result['symbol']}.json"), 'w', encoding='utf-8') as f:
            json.dump(detail, f, ensure_ascii=False)

    log(f"=== 分析完成! 月線符合: {len(monthly_results)}, 周線符合: {len(weekly_results)} ===")

if __name__ == '__main__':
    main()
