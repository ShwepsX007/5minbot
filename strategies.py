import logging
import liq_strategy as ls

log = logging.getLogger("bot.strategies")


def get_strategies_list():
    return [
        {"id": "liquidations", "name": "Каскад ликвидаций (Polymarket)", "desc": "Ловит ликвидации и торгует Bitcoin Up/Down"},
    ]
