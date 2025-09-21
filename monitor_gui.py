#!/usr/bin/env python3
"""
Legacy Tkinter GUI removed.

This lightweight wrapper starts the Dash web UI instead.
Preferred entrypoints:
- Run_Monitors.bat (Windows)
- python dash_app.py (cross-platform)
"""

from __future__ import annotations

import os


def main() -> None:
    try:
        # Import Dash app defined in [dash_app.py](dash_app.py:1)
        from dash_app import app
    except Exception as e:
        raise RuntimeError(
            "Dash UI unavailable. Install dependencies and run: python dash_app.py"
        ) from e
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="127.0.0.1", port=port, debug=True)


if __name__ == "__main__":
    main()
