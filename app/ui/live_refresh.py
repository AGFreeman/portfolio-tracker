"""Helpers for periodic UI refresh when live price updates are enabled."""
from __future__ import annotations

from typing import Optional

import streamlit as st

from app.services.prices import LIVE_QUOTES_REFRESH_SEC


def live_quotes_run_every() -> Optional[float]:
    """Streamlit fragment interval in seconds, or None when live updates are off."""
    if bool(st.session_state.get("live_price_updates_enabled", False)):
        return float(LIVE_QUOTES_REFRESH_SEC)
    return None
