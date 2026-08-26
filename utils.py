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

def calc_pnl_curve(trades):
    """Готовит кривую доходности и просадки из истории сделок.

    trades — список dict со сделками (как из get_trade_statistics),
    порядок не важен, здесь сортируем по timestamp по возрастанию.

    Возвращает dict:
      dates       — время закрытия каждой сделки (по возрастанию)
      cum_pnl     — накопленный PnL на каждый момент
      drawdown    — просадка от локального пика (<=0) на каждый момент
      max_drawdown— максимальная просадка (наибольшее по модулю значение)
      total_pnl   — суммарный PnL
      wins/losses — количество прибыльных / убыточных сделок
    """
    if not trades:
        return None

    trades_sorted = sorted(trades, key=lambda t: t.get("timestamp", 0))
    dates = [datetime.fromtimestamp(t["timestamp"]) for t in trades_sorted]

    cum_pnl, drawdown = [], []
    running, peak = 0.0, float("-inf")
    wins = losses = 0
    for t in trades_sorted:
        pnl = t.get("pnl", 0) or 0
        running += pnl
        if pnl > 0: wins += 1
        elif pnl < 0: losses += 1
        peak = max(peak, running)
        cum_pnl.append(running)
        drawdown.append(running - peak)

    return {
        "dates": dates,
        "cum_pnl": cum_pnl,
        "drawdown": drawdown,
        "max_drawdown": min(drawdown) if drawdown else 0.0,
        "total_pnl": round(cum_pnl[-1], 2) if cum_pnl else 0.0,
        "wins": wins,
        "losses": losses,
        "count": len(trades_sorted),
    }


def generate_pnl_plot(trades, is_demo=False):
    """График доходности (накопленный PnL) и просадки по сделкам.

    Отдельно строится для демо и реальных сделок — вызывающий код сам
    передаёт уже отфильтрованный по is_demo список trades.
    Возвращает io.BytesIO с PNG или None, если сделок нет.
    """
    try:
        curve = calc_pnl_curve(trades)
        if not curve or curve["count"] < 1:
            return None

        dates, cum_pnl, drawdown = curve["dates"], curve["cum_pnl"], curve["drawdown"]
        mode_label = "ДЕМО 🎮" if is_demo else "РЕАЛ 💰"
        line_color = "#2ecc71" if cum_pnl[-1] >= 0 else "#e74c3c"

        # Для одной сделки line-графику нечего рисовать между двумя точками —
        # добавляем точку "0" перед первой сделкой, чтобы был виден старт.
        if len(dates) == 1:
            dates = [dates[0], dates[0]]
            cum_pnl = [0.0, cum_pnl[0]]
            drawdown = [0.0, drawdown[0]]

        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(10, 7), sharex=True,
            gridspec_kw={"height_ratios": [2, 1]}
        )

        ax1.plot(dates, cum_pnl, marker='o', color=line_color, linewidth=2)
        ax1.axhline(0, color='gray', linewidth=0.8, linestyle='--')
        ax1.set_title(f"PnL — {mode_label} | сделок: {curve['count']} | "
                       f"итого: {'+' if curve['total_pnl'] >= 0 else ''}{curve['total_pnl']}$")
        ax1.set_ylabel("Накопленный PnL, $")
        ax1.grid(True, alpha=0.3)

        ax2.fill_between(dates, drawdown, 0, color='#e74c3c', alpha=0.35)
        ax2.plot(dates, drawdown, color='#e74c3c', linewidth=1)
        ax2.set_ylabel("Просадка, $")
        ax2.set_xlabel("Время")
        ax2.grid(True, alpha=0.3)

        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        log.exception(f"generate_pnl_plot error: {e}")
        return None


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