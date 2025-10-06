import unittest
from datetime import datetime, timezone

from monitor.mt5_client import earliest_hit_from_ticks


class EarliestHitFromTicksTests(unittest.TestCase):

    def setUp(self) -> None:
        self.epoch = datetime(2025, 9, 26, 12, 4, 8, tzinfo=timezone.utc).timestamp()

    def test_sell_tp_detected_when_only_bid_present(self) -> None:
        # For sell orders, we need ask price to detect TP (ask <= tp)
        tick = {"time": self.epoch, "bid": 11958.0, "ask": 11958.0}
        hit = earliest_hit_from_ticks([tick], direction="sell", sl=12010.0, tp=11960.0, server_offset_hours=0)
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.kind, "TP")
        self.assertAlmostEqual(hit.price, 11958.0)

    def test_sell_sl_detected_when_only_bid_present(self) -> None:
        # For sell orders, we need ask price to detect SL (ask >= sl)
        tick = {"time": self.epoch, "bid": 12080.0, "ask": 12080.0}
        hit = earliest_hit_from_ticks([tick], direction="sell", sl=12050.0, tp=11960.0, server_offset_hours=0)
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.kind, "SL")
        self.assertAlmostEqual(hit.price, 12080.0)


if __name__ == "__main__":
    unittest.main()
