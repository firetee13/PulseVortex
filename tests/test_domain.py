import unittest
from datetime import datetime, timezone
from monitor.domain import Setup, Hit, TickFetchStats

UTC = timezone.utc


class TestDomain(unittest.TestCase):
    def test_setup_initializes_correctly(self):
        setup = Setup(
            id=1,
            symbol='EURUSD',
            direction='Buy',
            sl=1.1,
            tp=1.2,
            entry_price=1.15,
            as_of_utc=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        )
        self.assertEqual(setup.id, 1)
        self.assertEqual(setup.symbol, 'EURUSD')
        self.assertEqual(setup.direction, 'Buy')
        self.assertEqual(setup.sl, 1.1)
        self.assertEqual(setup.tp, 1.2)
        self.assertEqual(setup.entry_price, 1.15)
        self.assertEqual(setup.as_of_utc, datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC))

    def test_hit_initializes_correctly(self):
        hit_time = datetime(2023, 1, 1, 13, 0, 0, tzinfo=UTC)
        hit = Hit(
            kind='TP',
            time_utc=hit_time,
            price=1.2
        )
        self.assertEqual(hit.kind, 'TP')
        self.assertEqual(hit.time_utc, hit_time)
        self.assertEqual(hit.price, 1.2)

    def test_tick_fetch_stats_initializes_correctly(self):
        stats = TickFetchStats(
            pages=5,
            total_ticks=1000,
            elapsed_s=2.5,
            fetch_s=2.0,
            early_stop=True
        )
        self.assertEqual(stats.pages, 5)
        self.assertEqual(stats.total_ticks, 1000)
        self.assertEqual(stats.elapsed_s, 2.5)
        self.assertEqual(stats.fetch_s, 2.0)
        self.assertEqual(stats.early_stop, True)


if __name__ == '__main__':
    unittest.main()