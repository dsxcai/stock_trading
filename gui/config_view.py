from __future__ import annotations

import html

from gui.services import RuntimeConfigSnapshot, SignalConfigSnapshot


def _render_runtime_subsection(title: str, note: str, body: str) -> str:
    return (
        '<section class="config-subsection">'
        '<div class="config-subsection-head">'
        f'<h4>{html.escape(title)}</h4>'
        f'<div class="form-note">{note}</div>'
        '</div>'
        f'{body}'
        '</section>'
    )


def render_signal_config_editor(snapshot: SignalConfigSnapshot) -> str:
    rows = []
    for ticker in snapshot.candidate_tickers:
        current_window = int(snapshot.selected_windows.get(ticker, 50))
        options = [50, 100]
        if current_window not in options:
            options = [current_window, 50, 100]
        option_html = "".join(
            f'<option value="{value}" {"selected" if value == current_window else ""}>SMA{value}</option>'
            for value in options
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(ticker)}</td>"
            f'<td class="compact"><input type="checkbox" name="ticker_enabled_{html.escape(ticker)}" {"checked" if ticker in snapshot.selected_windows else ""}></td>'
            f'<td class="compact"><select name="ticker_window_{html.escape(ticker)}">{option_html}</select></td>'
            "</tr>"
        )
    return (
        '<form class="stack" method="post" action="/save-signal-config" data-busy-message="Saving the signal config and refreshing the report, please wait..." data-async-submit="1" data-progress-kind="save-config">'
        '<table class="config-table">'
        '<thead><tr><th>Ticker</th><th>Enable</th><th>SMA</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        "</table>"
        '<label>Custom Tickers'
        '<input type="text" name="custom_tickers" placeholder="TSLA, QQQ, NFLX">'
        "</label>"
        '<label>Custom SMA'
        '<select name="custom_window"><option value="50">SMA50</option><option value="100">SMA100</option></select>'
        "</label>"
        '<div class="form-note">This section writes back to <code>state_engine.strategy.tactical.indicators</code>. Saving also regenerates the selected report when possible.</div>'
        '<button type="submit">Save Signal Config</button>'
        '</form>'
    )


def render_config_panel(runtime_config: RuntimeConfigSnapshot, signal_config: SignalConfigSnapshot) -> str:
    precision_fields = [
        ("usd_amount", "USD Amount"),
        ("display_price", "Display Price"),
        ("display_pct", "Display Percent"),
        ("trade_cash_amount", "Trade Cash Amount"),
        ("trade_dedupe_amount", "Trade Dedupe Amount"),
        ("state_selected_fields", "State Selected Fields"),
        ("backtest_amount", "Backtest Amount"),
        ("backtest_price", "Backtest Price"),
        ("backtest_rate", "Backtest Rate"),
        ("backtest_cost_param", "Backtest Cost Param"),
    ]
    precision_inputs = "".join(
        f'<label>{html.escape(label)}'
        f'<input type="number" min="0" name="{html.escape(key)}" value="{runtime_config.numeric_precision.get(key, 0)}">'
        "</label>"
        for key, label in precision_fields
    )
    buy_fee_rate_value = format(runtime_config.buy_fee_rate, "g")
    sell_fee_rate_value = format(runtime_config.sell_fee_rate, "g")
    general_section = _render_runtime_subsection(
        "General",
        "High-level document metadata for generated reports.",
        (
            '<div class="config-grid">'
            f'<label class="config-span-2">Document Title / <code>meta.doc</code><input type="text" name="doc" value="{html.escape(runtime_config.doc)}"></label>'
            '</div>'
        ),
    )
    ledger_section = _render_runtime_subsection(
        "Ledger Paths",
        "Runtime file paths used for states, trade ledger refreshes, and cash-event writes.",
        (
            '<div class="config-grid">'
            f'<label>Trades File<input type="text" name="trades_file" value="{html.escape(runtime_config.trades_file)}"></label>'
            f'<label>Cash Events File<input type="text" name="cash_events_file" value="{html.escape(runtime_config.cash_events_file)}"></label>'
            '</div>'
        ),
    )
    execution_section = _render_runtime_subsection(
        "Execution Costs",
        "Use separate fee rates for buy-side sizing and sell-side reclaim estimates.",
        (
            '<div class="config-grid">'
            f'<label>Buy Fee Rate<input type="number" step="0.000001" name="buy_fee_rate" value="{html.escape(buy_fee_rate_value)}"></label>'
            f'<label>Sell Fee Rate<input type="number" step="0.000001" name="sell_fee_rate" value="{html.escape(sell_fee_rate_value)}"></label>'
            '</div>'
        ),
    )
    bucket_section = _render_runtime_subsection(
        "Portfolio Buckets",
        "Default ticker routing for core, tactical, and tactical cash-pool buckets.",
        (
            '<div class="config-grid">'
            f'<label class="config-span-2">Core Bucket Tickers<textarea name="core_tickers" placeholder="SPY&#10;ARKQ">{html.escape(runtime_config.core_tickers_text)}</textarea><span class="form-note">One ticker per line, or use commas.</span></label>'
            f'<label class="config-span-2">Tactical Bucket Tickers<textarea name="tactical_tickers" placeholder="QQQ&#10;SMH">{html.escape(runtime_config.tactical_tickers_text)}</textarea><span class="form-note">These tickers default into the <code>tactical</code> bucket.</span></label>'
            f'<label>Tactical Cash Pool Ticker<input type="text" name="tactical_cash_pool_ticker" value="{html.escape(runtime_config.tactical_cash_pool_ticker)}" placeholder="META"></label>'
            f'<label class="config-span-2">Tactical Cash Pool Bucket Tickers<textarea name="tactical_cash_pool_tickers" placeholder="META">{html.escape(runtime_config.tactical_cash_pool_tickers_text)}</textarea><span class="form-note">Optional extra tickers that should default into the <code>tactical_cash_pool</code> bucket.</span></label>'
            '</div>'
        ),
    )
    market_data_section = _render_runtime_subsection(
        "Market Data",
        "Configure FX aliases and per-ticker CSV overrides used by the live engine.",
        (
            '<div class="config-grid">'
            f'<label class="config-span-2">FX Pairs<textarea name="fx_pairs" placeholder="usd_twd=TWD=X">{html.escape(runtime_config.fx_pairs_text)}</textarea><span class="form-note">One alias per line using <code>alias=ticker</code>.</span></label>'
            f'<label class="config-span-2">CSV Sources<textarea name="csv_sources" placeholder="AAPL=AAPL.csv">{html.escape(runtime_config.csv_sources_text)}</textarea><span class="form-note">Optional overrides using <code>TICKER=relative/or/absolute/path.csv</code>.</span></label>'
            '</div>'
        ),
    )
    calendar_section = _render_runtime_subsection(
        "Trading Calendar",
        "Override market closures and early closes used by date resolution.",
        (
            '<div class="config-grid">'
            f'<label class="config-span-2">Closed Trading Days<textarea name="closed_days" placeholder="2026-12-25=Christmas Day">{html.escape(runtime_config.closed_days_text)}</textarea><span class="form-note">One line per closure using <code>YYYY-MM-DD=Reason</code>.</span></label>'
            f'<label class="config-span-2">Early Close Trading Days<textarea name="early_close_days" placeholder="2026-12-24=13:00|Christmas Eve">{html.escape(runtime_config.early_closes_text)}</textarea><span class="form-note">One line per event using <code>YYYY-MM-DD=HH:MM|Reason</code>.</span></label>'
            '</div>'
        ),
    )
    reporting_section = _render_runtime_subsection(
        "Reporting",
        "Display precision and trade-detail compression rules.",
        (
            '<div class="config-grid">'
            f"{precision_inputs}"
            f'<label>Previous Trade Days (Simplified)<input type="number" min="0" name="keep_prev_trade_days_simplified" value="{runtime_config.keep_prev_trade_days_simplified}"></label>'
            '</div>'
        ),
    )
    return (
        '<div class="config-shell">'
        '<section class="config-section">'
        '<div class="config-section-head">'
        '<h3>Runtime Config</h3>'
        '<div class="subtle">Edit every runtime option that the live state engine currently uses. These fields write back to <code>config.json</code> in canonical <code>state_engine</code> structure.</div>'
        '</div>'
        '<form class="stack" method="post" action="/save-runtime-config" data-busy-message="Saving the runtime config and refreshing the report, please wait..." data-async-submit="1" data-progress-kind="save-config">'
        f'{general_section}'
        f'{ledger_section}'
        f'{execution_section}'
        f'{bucket_section}'
        f'{market_data_section}'
        f'{calendar_section}'
        f'{reporting_section}'
        '<div class="form-note">Saving preserves the current tactical indicator table below, then regenerates the selected report when possible.</div>'
        '<button type="submit">Save Runtime Config</button>'
        '</form>'
        '</section>'
        '<section class="config-section">'
        '<div class="config-section-head">'
        '<h3>Signal Config</h3>'
        '<div class="subtle">Manage <code>state_engine.strategy.tactical.indicators</code> with the existing ticker/SMA editor.</div>'
        '</div>'
        f'{render_signal_config_editor(signal_config)}'
        '</section>'
        '</div>'
    )
