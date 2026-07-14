import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(
    page_title="Crypto Streaming Pipeline",
    page_icon="◧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Design tokens ───────────────────────────────────────────────────
INK     = "#0A0D12"
SURFACE = "#131821"
LINE    = "#212936"
TEXT    = "#E6E9EF"
MUTED   = "#6E7889"
BULL    = "#26A69A"
BEAR    = "#EF5350"
ACCENT  = "#E8B23C"

st.html(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');

  .stApp {{ background: {INK}; }}
  header[data-testid="stHeader"] {{ background: transparent; }}
  .block-container {{ padding-top: 2.2rem; max-width: 1500px; }}

  html, body, [class*="css"] {{
      font-family: 'IBM Plex Sans', sans-serif; color: {TEXT};
  }}

  .masthead {{
      display: flex; align-items: baseline; gap: 14px;
      border-bottom: 1px solid {LINE};
      padding-bottom: 14px; margin-bottom: 22px;
  }}
  .masthead h1 {{
      font-size: 1.55rem; font-weight: 600; margin: 0; letter-spacing: -0.01em;
  }}
  .masthead .sub {{
      font-family: 'IBM Plex Mono', monospace; font-size: 0.72rem;
      color: {MUTED}; text-transform: uppercase; letter-spacing: 0.12em;
  }}

  /* Signature: the pipeline flow bar */
  .flow {{
      display: flex; border: 1px solid {LINE}; border-radius: 10px;
      background: {SURFACE}; overflow: hidden; margin-bottom: 20px;
  }}
  .stage {{
      flex: 1; padding: 13px 16px; border-right: 1px solid {LINE};
      position: relative;
  }}
  .stage:last-child {{ border-right: none; }}
  .stage .k {{
      font-family: 'IBM Plex Mono', monospace; font-size: 0.63rem;
      letter-spacing: 0.14em; text-transform: uppercase; color: {MUTED};
      display: block; margin-bottom: 5px;
  }}
  .stage .v {{
      font-family: 'IBM Plex Mono', monospace; font-variant-numeric: tabular-nums;
      font-size: 0.95rem; font-weight: 500; color: {TEXT};
  }}
  .stage .v em {{ color: {MUTED}; font-style: normal; font-size: 0.78rem; }}
  .stage.live::before {{
      content: ""; position: absolute; left: 0; top: 0; bottom: 0;
      width: 2px; background: {ACCENT};
  }}

  .price {{
      font-family: 'IBM Plex Mono', monospace; font-variant-numeric: tabular-nums;
      font-size: 2.9rem; font-weight: 500; letter-spacing: -0.02em; line-height: 1.05;
  }}
  .delta {{
      font-family: 'IBM Plex Mono', monospace; font-size: 0.9rem; font-weight: 500;
      padding: 3px 9px; border-radius: 5px; margin-left: 10px; vertical-align: middle;
  }}
  .up   {{ color: {BULL}; background: rgba(38,166,154,.12); }}
  .down {{ color: {BEAR}; background: rgba(239,83,80,.12); }}

  .stat {{ border-left: 1px solid {LINE}; padding-left: 16px; }}
  .stat .k {{
      font-family: 'IBM Plex Mono', monospace; font-size: 0.63rem;
      letter-spacing: 0.14em; text-transform: uppercase; color: {MUTED};
      display: block; margin-bottom: 6px;
  }}
  .stat .v {{
      font-family: 'IBM Plex Mono', monospace; font-variant-numeric: tabular-nums;
      font-size: 1.15rem; font-weight: 500;
  }}

  .card {{
      border: 1px solid {LINE}; border-radius: 10px;
      background: {SURFACE}; padding: 18px 20px;
  }}
  .card h3 {{
      font-size: 0.72rem; font-weight: 600; color: {MUTED};
      text-transform: uppercase; letter-spacing: 0.12em; margin: 0 0 14px 0;
  }}

  .stTabs [data-baseweb="tab-list"] {{ gap: 4px; border-bottom: 1px solid {LINE}; }}
  .stTabs [data-baseweb="tab"] {{
      font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem;
      letter-spacing: 0.04em; background: transparent; color: {MUTED};
      border-radius: 6px 6px 0 0; padding: 8px 18px;
  }}
  .stTabs [aria-selected="true"] {{ color: {TEXT}; background: {SURFACE}; }}

  [data-testid="stDataFrame"] {{ border: 1px solid {LINE}; border-radius: 8px; }}
</style>
""")


# ── Data ────────────────────────────────────────────────────────────
@st.cache_data
def load():
    con = duckdb.connect("crypto_analytics/crypto.duckdb", read_only=True)
    ohlc = con.execute("select * from main.fct_ohlc order by candle_minute").df()
    trades = con.execute("select * from main.stg_trades").df()
    con.close()
    return ohlc, trades


def latest_session(df, gap_minutes=30):
    """
    Collection ran in bursts across several days, so the table has long gaps.
    Keep only the most recent unbroken run — a chart spanning a multi-day
    void is a chart of nothing.
    """
    if df.empty:
        return df
    times = pd.Series(sorted(df["candle_minute"].unique()))
    gaps = times.diff() > pd.Timedelta(minutes=gap_minutes)
    start = times[gaps[gaps].index[-1]] if gaps.any() else times.iloc[0]
    return df[df["candle_minute"] >= start]


ohlc_all, trades_all = load()
ohlc = latest_session(ohlc_all)

session_start = ohlc["candle_minute"].min()
trades = trades_all[trades_all["traded_at"] >= session_start]
products = sorted(ohlc["product"].unique())

# ── Masthead ────────────────────────────────────────────────────────
st.markdown(
    '<div class="masthead">'
    '<h1>Crypto Streaming Pipeline</h1>'
    '<span class="sub">live market data · aws · dbt</span>'
    '</div>',
    unsafe_allow_html=True,
)

# ── Signature: pipeline flow bar ────────────────────────────────────
st.markdown(f"""
<div class="flow">
  <div class="stage live">
    <span class="k">01 &middot; Source</span>
    <span class="v">Coinbase <em>WebSocket</em></span>
  </div>
  <div class="stage">
    <span class="k">02 &middot; Ingest</span>
    <span class="v">{len(trades):,} <em>records &rarr; Kinesis</em></span>
  </div>
  <div class="stage">
    <span class="k">03 &middot; Process</span>
    <span class="v">Lambda <em>batched &rarr; S3</em></span>
  </div>
  <div class="stage">
    <span class="k">04 &middot; Lake</span>
    <span class="v">S3 <em>partitioned by date</em></span>
  </div>
  <div class="stage">
    <span class="k">05 &middot; Transform</span>
    <span class="v">dbt <em>2 models &middot; 15 tests</em></span>
  </div>
  <div class="stage">
    <span class="k">06 &middot; Serve</span>
    <span class="v">{len(ohlc):,} <em>candles &middot; DuckDB</em></span>
  </div>
</div>
""", unsafe_allow_html=True)

st.caption(
    f"Showing the most recent continuous session: "
    f"{session_start:%d %b %H:%M} – {ohlc['candle_minute'].max():%H:%M} UTC"
)

# ── Per-product tabs ────────────────────────────────────────────────
for tab, product in zip(st.tabs(products), products):
    with tab:
        df = ohlc[ohlc["product"] == product].copy()
        pt = trades[trades["product"] == product]

        last, first = df["close_price"].iloc[-1], df["open_price"].iloc[0]
        chg = last - first
        pct = chg / first * 100
        cls, arrow = ("up", "▲") if chg >= 0 else ("down", "▼")

        buys = int((pt["side"] == "buy").sum())
        sells = int((pt["side"] == "sell").sum())

        c0, c1, c2, c3, c4 = st.columns([2.4, 1, 1, 1, 1])
        c0.markdown(
            f'<div class="price">${last:,.2f}'
            f'<span class="delta {cls}">{arrow} {pct:+.3f}%</span></div>',
            unsafe_allow_html=True,
        )
        for col, k, v in [
            (c1, "Session high", f"${df['high_price'].max():,.2f}"),
            (c2, "Session low",  f"${df['low_price'].min():,.2f}"),
            (c3, "Trades",       f"{len(pt):,}"),
            (c4, "Value traded", f"${pt['trade_value'].sum():,.0f}"),
        ]:
            col.markdown(
                f'<div class="stat"><span class="k">{k}</span>'
                f'<span class="v">{v}</span></div>',
                unsafe_allow_html=True,
            )

        st.write("")

        # ── Candles + moving average + volume ──────────────────────
        df["ma"] = df["close_price"].rolling(5, min_periods=1).mean()

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.04, row_heights=[0.75, 0.25],
        )
        fig.add_trace(go.Candlestick(
            x=df["candle_minute"],
            open=df["open_price"], high=df["high_price"],
            low=df["low_price"], close=df["close_price"],
            increasing=dict(line=dict(color=BULL, width=1), fillcolor=BULL),
            decreasing=dict(line=dict(color=BEAR, width=1), fillcolor=BEAR),
            name="OHLC", whiskerwidth=0.4,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=df["candle_minute"], y=df["ma"],
            line=dict(color=ACCENT, width=1.4, dash="dot"),
            name="5-min MA", hovertemplate="MA %{y:$,.2f}<extra></extra>",
        ), row=1, col=1)

        fig.add_trace(go.Bar(
            x=df["candle_minute"], y=df["volume"],
            marker=dict(
                color=[BULL if c >= o else BEAR
                       for c, o in zip(df["close_price"], df["open_price"])],
                opacity=0.55,
            ),
            name="Volume", showlegend=False,
        ), row=2, col=1)

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
            height=520, margin=dict(l=8, r=8, t=28, b=8),
            xaxis_rangeslider_visible=False,
            font=dict(family="IBM Plex Mono, monospace", size=11, color=MUTED),
            legend=dict(orientation="h", y=1.06, x=0, bgcolor="rgba(0,0,0,0)"),
            hovermode="x unified", bargap=0.35,
        )
        fig.update_xaxes(gridcolor=LINE, zeroline=False, showspikes=True,
                         spikecolor=MUTED, spikethickness=1, spikedash="dot")
        fig.update_yaxes(gridcolor=LINE, zeroline=False, tickprefix="$", row=1, col=1)
        fig.update_yaxes(gridcolor=LINE, zeroline=False, row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)

        # ── Bottom row ─────────────────────────────────────────────
        left, right = st.columns([1, 1.6])

        with left:
            st.markdown('<div class="card"><h3>Order flow</h3>', unsafe_allow_html=True)
            total = buys + sells
            bpct = buys / total * 100 if total else 0
            flow = go.Figure()
            flow.add_trace(go.Bar(y=["f"], x=[buys], orientation="h",
                                  marker=dict(color=BULL),
                                  hovertemplate="Buy %{x:,}<extra></extra>"))
            flow.add_trace(go.Bar(y=["f"], x=[sells], orientation="h",
                                  marker=dict(color=BEAR),
                                  hovertemplate="Sell %{x:,}<extra></extra>"))
            flow.update_layout(
                barmode="stack", template="plotly_dark",
                paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
                height=80, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                xaxis=dict(visible=False), yaxis=dict(visible=False),
            )
            st.plotly_chart(flow, use_container_width=True)
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;'
                f'font-family:IBM Plex Mono,monospace;font-size:0.8rem;">'
                f'<span style="color:{BULL}">BUY {buys:,} &middot; {bpct:.1f}%</span>'
                f'<span style="color:{BEAR}">{100-bpct:.1f}% &middot; {sells:,} SELL</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )

        with right:
            st.markdown('<div class="card"><h3>Busiest minutes</h3>',
                        unsafe_allow_html=True)
            top = (df.nlargest(5, "trade_count")[
                       ["candle_minute", "trade_count", "volume", "close_price"]]
                   .rename(columns={"candle_minute": "Minute", "trade_count": "Trades",
                                    "volume": "Volume", "close_price": "Close"}))
            top["Minute"] = top["Minute"].dt.strftime("%H:%M")
            st.dataframe(top, use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with st.expander("Full OHLC dataset"):
            st.dataframe(df, use_container_width=True, hide_index=True)