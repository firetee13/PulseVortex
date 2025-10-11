import unittest
from unittest.mock import patch

from monitor.core import symbols as symbols_module
from monitor.core.symbols import classify_symbol, is_crypto_symbol


class SymbolClassificationTests(unittest.TestCase):
    def test_crypto_detection_by_token(self) -> None:
        self.assertEqual(classify_symbol("BTCUSD"), "crypto")
        self.assertTrue(is_crypto_symbol("DOGEUSD"))

    def test_index_detection_by_keyword_or_digits(self) -> None:
        self.assertEqual(classify_symbol("US500"), "indices")
        self.assertEqual(classify_symbol("JP225"), "indices")

    def test_forex_pair_detection(self) -> None:
        self.assertEqual(classify_symbol("eurusd"), "forex")
        self.assertEqual(classify_symbol("XAUUSD"), "forex")

    def test_other_when_symbol_missing(self) -> None:
        self.assertEqual(classify_symbol(None), "other")
        self.assertEqual(classify_symbol(""), "other")

    def test_crypto_detection_with_usd_suffix_fallback(self) -> None:
        class DynamicTokens:
            def __init__(self) -> None:
                self._first = True

            def __iter__(self):
                if self._first:
                    self._first = False
                    return iter(("NOMATCH",))
                return iter(("XBT",))

        classify_symbol.cache_clear()
        with patch.object(symbols_module, "_CRYPTO_TICKERS", DynamicTokens()):
            self.assertEqual(classify_symbol("XBTUSD"), "crypto")
        classify_symbol.cache_clear()


if __name__ == "__main__":
    unittest.main()
