import logging
import liq_strategy as ls
import trend_strategy as ts

log = logging.getLogger("bot.strategies")


def get_strategies_list():
    """Реестр автономных торговых систем бота.

    Каждая система — отдельный модуль со своим состоянием, настройками
    (`liq_*` / `td_*`) и статистикой (trade_history.strategy). Системы
    работают независимо: свои фоновые задачи, свои серии мартингейла.
    """
    return [
        {"id": "liquidations", "name": "Каскад ликвидаций (Polymarket)", "desc": "Ловит ликвидации и торгует Bitcoin Up/Down", "active": ls.is_active()},
        {"id": "trend", "name": "Движение за рынком (свечи)", "desc": "Вход по направлению последней закрытой свечи, FAK, мартингейл, TP отложником/FAK", "active": ts.is_active()},
    ]
