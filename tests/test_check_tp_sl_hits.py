
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from monitor.cli.hit_checker import RateBar, _bar_crosses_price, _evaluate_setup
from monitor.core.domain import Setup, TickFetchStats


UTC = timezone.utc


class BarCrossesPriceGapTests(unittest.TestCase):

    def setUp(self) -> None:
        start = datetime(2025, 9, 26, 12, 4, tzinfo=UTC)
        self.bar = RateBar(
            start_utc=start,
            end_utc=start + timedelta(minutes=1),
            low=0.0,
            high=0.0,
        )

    def test_buy_gap_up_above_tp_counts_as_hit(self) -> None:
        bar = self.bar.__class__(start_utc=self.bar.start_utc, end_utc=self.bar.end_utc, low=11970.0, high=11980.0)
        setup = SimpleNamespace(direction='buy', sl=11860.0, tp=11960.0)
        self.assertTrue(_bar_crosses_price(bar, setup, spread_guard=0.0))

    def test_sell_gap_down_below_tp_counts_as_hit(self) -> None:
        bar = self.bar.__class__(start_utc=self.bar.start_utc, end_utc=self.bar.end_utc, low=11930.0, high=11940.0)
        setup = SimpleNamespace(direction='sell', sl=12010.0, tp=11960.0)
        self.assertTrue(_bar_crosses_price(bar, setup, spread_guard=0.0))

    def test_sell_gap_up_above_sl_counts_as_hit(self) -> None:
        bar = self.bar.__class__(start_utc=self.bar.start_utc, end_utc=self.bar.end_utc, low=12020.0, high=12040.0)
        setup = SimpleNamespace(direction='sell', sl=12010.0, tp=11960.0)
        self.assertTrue(_bar_crosses_price(bar, setup, spread_guard=0.0))

    def test_buy_gap_down_below_sl_counts_as_hit(self) -> None:
        bar = self.bar.__class__(start_utc=self.bar.start_utc, end_utc=self.bar.end_utc, low=11820.0, high=11840.0)
        setup = SimpleNamespace(direction='buy', sl=11860.0, tp=11960.0)
        self.assertTrue(_bar_crosses_price(bar, setup, spread_guard=0.0))


class EvaluateSetupQuietHoursTests(unittest.TestCase):

    def test_evaluate_setup_skips_quiet_hours_ranges(self) -> None:
        setup = Setup(
            id=1,
            symbol='NZDCHF',
            direction='buy',
            sl=0.95,
            tp=1.05,
            entry_price=None,
            as_of_utc=datetime(2025, 9, 28, 20, 44, tzinfo=UTC),
        )
        bars = [
            RateBar(
                start_utc=datetime(2025, 9, 28, 20, 44, tzinfo=UTC),
                end_utc=datetime(2025, 9, 28, 22, 0, tzinfo=UTC),
                low=0.9,
                high=1.1,
            )
        ]
        now_utc = datetime(2025, 9, 28, 22, 0, tzinfo=UTC)
        captured_ranges = []
        stats = TickFetchStats(pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False)

        def fake_scan_for_hit(**kwargs):
            captured_ranges.append((kwargs['start_utc'], kwargs['end_utc']))
            return None, stats, 1

        with patch('monitor.cli.hit_checker.scan_for_hit_with_chunks', side_effect=fake_scan_for_hit):
            result = _evaluate_setup(
                setup,
                last_checked_utc=setup.as_of_utc,
                bars=bars,
                resolved_symbol='NZDCHF',
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=0.0,
                trace_ticks=False,
            )

        expected_ranges = [
            (datetime(2025, 9, 28, 20, 44, tzinfo=UTC), datetime(2025, 9, 28, 20, 45, tzinfo=UTC)),
            (datetime(2025, 9, 28, 21, 59, tzinfo=UTC), datetime(2025, 9, 28, 22, 0, tzinfo=UTC)),
        ]
        self.assertEqual(captured_ranges, expected_ranges)
        self.assertEqual(result.windows, len(expected_ranges))
        self.assertIsNone(result.hit)
        self.assertEqual(result.last_checked_utc, now_utc)

    def test_evaluate_setup_all_quiet_advances_cursor_without_ticks(self) -> None:
        setup = Setup(
            id=2,
            symbol='NZDCHF',
            direction='buy',
            sl=0.95,
            tp=1.05,
            entry_price=None,
            as_of_utc=datetime(2025, 9, 28, 21, 0, tzinfo=UTC),
        )
        now_utc = datetime(2025, 9, 28, 21, 30, tzinfo=UTC)

        with patch('monitor.cli.hit_checker.scan_for_hit_with_chunks') as fake_scan:
            fake_scan.return_value = (None, TickFetchStats(pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False), 0)
            result = _evaluate_setup(
                setup,
                last_checked_utc=setup.as_of_utc,
                bars=[],
                resolved_symbol='NZDCHF',
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=0.0,
                trace_ticks=False,
            )

        fake_scan.assert_not_called()
        self.assertEqual(result.windows, 0)
        self.assertIsNone(result.hit)
        self.assertEqual(result.last_checked_utc, now_utc)


if __name__ == '__main__':
    unittest.main()
