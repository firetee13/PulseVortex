from __future__ import annotations

"""Symbol classification helpers shared across CLI and GUI components."""

from functools import lru_cache

_CRYPTO_TICKERS = (
    "BTC",
    "ETH",
    "XRP",
    "ADA",
    "SOL",
    "DOGE",
    "BNB",
    "DOT",
    "AVAX",
    "LINK",
    "LNK",
    "LTC",
    "BCH",
    "XLM",
    "TRX",
    "ETC",
    "UNI",
    "ATOM",
    "APT",
    "SHIB",
    "PEPE",
    "AVX",
    "DOG",
    "XTZ",
)

_INDEX_KEYWORDS = (
    "US30",
    "US100",
    "US500",
    "SP500",
    "SPX",
    "NDX",
    "NAS100",
    "USTEC",
    "DAX",
    "DE30",
    "DE40",
    "GER30",
    "GER40",
    "FTSE",
    "UK100",
    "CAC",
    "FCHI",
    "FR40",
    "JP225",
    "NIKKEI",
    "N225",
    "AUS200",
    "ASX200",
    "HK50",
    "HSI",
    "ES35",
    "IBEX",
    "IT40",
    "EU50",
    "STOXX",
)

_ISO_CCY = {
    "USD",
    "EUR",
    "JPY",
    "GBP",
    "AUD",
    "NZD",
    "CAD",
    "CHF",
    "NOK",
    "SEK",
    "DKK",
    "ZAR",
    "TRY",
    "MXN",
    "PLN",
    "CZK",
    "HUF",
    "CNH",
    "CNY",
    "HKD",
    "SGD",
}

_METALS = {"XAU", "XAG", "XPT", "XPD"}


@lru_cache(maxsize=None)
def classify_symbol(symbol: str | None) -> str:
    """Classify a symbol as 'forex', 'crypto', 'indices', or 'other'."""

    s = (symbol or "").upper()
    if not s:
        return "other"

    if any(token in s for token in _CRYPTO_TICKERS):
        return "crypto"

    if any(keyword in s for keyword in _INDEX_KEYWORDS):
        return "indices"

    def _is_pair(value: str) -> bool:
        if len(value) >= 6:
            base = value[:3]
            quote = value[3:6]
            if (base in _ISO_CCY or base in _METALS) and (quote in _ISO_CCY):
                return True
        return False

    if _is_pair(s):
        return "forex"

    if s.endswith("USD") and any(token in s for token in _CRYPTO_TICKERS):
        return "crypto"

    if any(ch.isdigit() for ch in s):
        return "indices"

    return "forex"


def is_crypto_symbol(symbol: str | None) -> bool:
    """Return True if symbol is classified as crypto."""

    return classify_symbol(symbol) == "crypto"
