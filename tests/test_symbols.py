import unittest

from monitor.symbols import classify_symbol, is_crypto_symbol


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


if __name__ == "__main__":
    unittest.main()
