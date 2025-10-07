import timelapse_setups as tls


def test_demo_forex_spread_augmented(monkeypatch):
    monkeypatch.setattr(tls, "_is_demo_account", lambda: True)
    monkeypatch.setattr(tls, "classify_symbol", lambda symbol: "forex")
    monkeypatch.setattr(tls, "_symbol_point", lambda symbol: 0.0001)
    original = 0.0
    augmented = tls._augment_spread_for_demo("EURUSD", original)
    assert augmented == original + (0.0001 * tls.DEMO_FOREX_SPREAD_POINTS)


def test_real_account_no_adjust(monkeypatch):
    monkeypatch.setattr(tls, "_is_demo_account", lambda: False)
    monkeypatch.setattr(tls, "classify_symbol", lambda symbol: "forex")
    monkeypatch.setattr(tls, "_symbol_point", lambda symbol: 0.0001)
    assert tls._augment_spread_for_demo("EURUSD", 0.0002) == 0.0002


def test_non_forex_no_adjust(monkeypatch):
    monkeypatch.setattr(tls, "_is_demo_account", lambda: True)
    monkeypatch.setattr(tls, "classify_symbol", lambda symbol: "crypto")
    monkeypatch.setattr(tls, "_symbol_point", lambda symbol: 0.01)
    assert tls._augment_spread_for_demo("BTCUSD", 5.0) == 5.0
