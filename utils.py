import requests
import io
import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime

log = logging.getLogger("bot.utils")

GAMMA_API = "https://gamma-api.polymarket.com/events"

def fetch_market(slug):
    try:
        url = f"{GAMMA_API}?slug={slug}"
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            return None
        data = res.json()
        if not data:
            return None
        event = data[0] if isinstance(data, list) else data
        
        markets = []
        for m in event.get("markets", []):
            outcomes = m.get("outcomes", ["Yes", "No"])
            prices = m.get("outcomePrices", ["0.5", "0.5"])
            if isinstance(outcomes, str):
                import json
                try: outcomes = json.loads(outcomes)
                except: outcomes = ["Yes", "No"]
            if isinstance(prices, str):
                import json
                try: prices = json.loads(prices)
                except: prices = ["0.5", "0.5"]
            
            price_yes = int(float(prices[0]) * 100) if len(prices) > 0 else 50
            price_no = int(float(prices[1]) * 100) if len(prices) > 1 else 50
            
            markets.append({
                "question": m.get("question", event.get("title", "")),
                "token_yes": m.get("clobTokenIds", ["", ""])[0] if isinstance(m.get("clobTokenIds"), list) else "",
                "token_no": m.get("clobTokenIds", ["", ""])[1] if len(m.get("clobTokenIds", [])) > 1 else "",
                "price_yes": price_yes,
                "price_no": price_no,
                "active": m.get("active", True)
            })
        
        options = []
        if markets:
            options.append({"label": "Yes", "prob": markets[0]["price_yes"]})
            options.append({"label": "No", "prob": markets[0]["price_no"]})

        return {
            "title": event.get("title", slug),
            "markets": markets,
            "options": options
        }
    except Exception as e:
        log.exception(f"fetch_market error: {e}")
        return None

def build_trend(last_probs, md):
    msg = ""
    try:
        options = md.get("options", [])
        for opt in options:
            label = opt.get("label")
            prob = opt.get("prob")
            if label is not None and prob is not None:
                old_prob = last_probs.get(label, prob)
                diff = prob - old_prob
                diff_str = f" (+{diff}%)" if diff > 0 else (f" ({diff}%)" if diff < 0 else "")
                msg += f"• {label}: *{prob}%*{diff_str}\n"
    except Exception as e:
        log.exception(f"build_trend error: {e}")
    return msg

def threshold_exceeded(last_probs, md, threshold):
    try:
        for opt in md.get("options", []):
            label = opt.get("label")
            prob = opt.get("prob")
            if label in last_probs:
                if abs(prob - last_probs[label]) >= threshold:
                    return True
    except Exception as e:
        log.exception(f"threshold_exceeded error: {e}")
    return False

def generate_plot(history_data, market_name):
    try:
        if not history_data:
            return None
        
        timestamps = [item[0] for item in history_data]
        dates = [datetime.fromtimestamp(ts) for ts in timestamps]
        
        labels = set()
        for _, probs in history_data:
            if isinstance(probs, dict):
                labels.update(probs.keys())
        
        plt.figure(figsize=(10, 5))
        for label in labels:
            y = [probs.get(label, 0) for _, probs in history_data]
            plt.plot(dates, y, marker='o', label=label)
        
        plt.title(f"Рынок: {market_name}")
        plt.xlabel("Время")
        plt.ylabel("Вероятность (%)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        log.exception(f"generate_plot error: {e}")
        return None