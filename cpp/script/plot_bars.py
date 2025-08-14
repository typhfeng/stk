import os
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np


PLOT_MODE = "candlestick"  # ohlc | candlestick | shapes
DATA_CSV_PATH = "output/603968_bar_resampled.csv"
OUTPUT_HTML_PATH = "output/plot.html"
TITLE = "Bars @ 1s (Sessions Only)"
INCLUDE_VWAP = True
MAX_SHAPES = 20000


def add_timestamp_column(frame: pd.DataFrame) -> pd.DataFrame:
    """Add timestamp column as unix timestamp for efficiency"""
    assert set(["year", "month", "day", "hour", "minute", "second"]) <= set(frame.columns)
    # Convert to timestamp more efficiently
    dt = pd.to_datetime(frame[["year", "month", "day", "hour", "minute", "second"]])
    frame = frame.copy()
    frame['timestamp'] = dt.astype('int64') // 10**9  # Convert to unix timestamp
    return frame


def build_session_timestamps(unique_days: np.ndarray) -> np.ndarray:
    """Build session timestamps as unix timestamps for efficiency"""
    session_timestamps = []
    for day_ts in unique_days:
        # Convert day timestamp to start of day
        day_start = (day_ts // 86400) * 86400  # Round down to start of day
        
        # Morning session: 9:30-11:30 (2 hours = 7200 seconds)
        morning_start = day_start + 9*3600 + 30*60  # 9:30
        morning_end = day_start + 11*3600 + 30*60   # 11:30
        morning_range = np.arange(morning_start, morning_end + 1, 1)
        
        # Afternoon session: 13:00-15:00 (2 hours = 7200 seconds)  
        afternoon_start = day_start + 13*3600       # 13:00
        afternoon_end = day_start + 15*3600         # 15:00
        afternoon_range = np.arange(afternoon_start, afternoon_end + 1, 1)
        
        session_timestamps.append(morning_range)
        session_timestamps.append(afternoon_range)
    
    if len(session_timestamps) == 0:
        return np.array([], dtype='int64')
    return np.concatenate(session_timestamps)


def expand_to_seconds_sessions_only(frame: pd.DataFrame) -> pd.DataFrame:
    """Efficiently expand to session seconds without filling gaps with dummy bars"""
    # Add timestamp column
    frame = add_timestamp_column(frame)
    
    # Drop datetime columns and set timestamp as index
    frame = frame.drop(columns=["year", "month", "day", "hour", "minute", "second"])
    frame = frame.set_index('timestamp').sort_index()
    
    # Get unique days from timestamps
    unique_days = np.unique((np.array(frame.index.values, dtype=np.int64) // 86400) * 86400)
    
    # Build all session timestamps
    session_timestamps = build_session_timestamps(unique_days)
    
    # Create result dataframe with only session timestamps
    result = pd.DataFrame(index=session_timestamps)
    
    # Only include actual data points, don't fill gaps
    # This avoids creating bars where ohlc=previous_close
    existing_data = frame[frame.index.isin(session_timestamps)]
    
    # Merge existing data
    result = result.join(existing_data, how='left')
    
    # Convert index to categorical integers for plotting efficiency
    result.index = pd.Categorical(range(len(result.index)))
    result['original_timestamp'] = session_timestamps
    
    return result.dropna()


def make_figure(frame: pd.DataFrame) -> go.Figure:
    assert set(["open", "high", "low", "close"]) <= set(frame.columns)
    fig = go.Figure()
    if PLOT_MODE == "ohlc":
        fig.add_trace(
            go.Ohlc(
                x=frame.index,
                open=frame["open"],
                high=frame["high"],
                low=frame["low"],
                close=frame["close"],
                name="OHLC",
            )
        )
    elif PLOT_MODE == "candlestick":
        fig.add_trace(
            go.Candlestick(
                x=frame.index,
                open=frame["open"],
                high=frame["high"],
                low=frame["low"],
                close=frame["close"],
                name="Candles",
                # whiskerwidth=0.1,
                # increasing_line_width=4,
                # decreasing_line_width=4,
            )
        )
    elif PLOT_MODE == "shapes":
        # Only use actual data points, no downsampling for gaps
        valid_data = frame.dropna()
        n = len(valid_data)
        step = max(1, n // MAX_SHAPES)
        
        shapes = []
        for i in range(0, n, step):
            row = valid_data.iloc[i]
            t = valid_data.index[i]
            lo, hi, op, cl = row["low"], row["high"], row["open"], row["close"]
            color = "#00aa00" if cl >= op else "#aa0000"
            
            # Single wick line (thin)
            shapes.append({
                "type": "line",
                "x0": t, "x1": t, "y0": lo, "y1": hi,
                "line": {"color": color, "width": 1}
            })
            
            # Single body rect (thick)
            shapes.append({
                "type": "rect",
                "x0": t - 0.4, "x1": t + 0.4,
                "y0": min(op, cl), "y1": max(op, cl),
                "fillcolor": color, "line": {"width": 0}
            })
        
        fig.update_layout(shapes=shapes)
    if "vwap" in frame.columns:
        if INCLUDE_VWAP:
            fig.add_trace(
                go.Scattergl(
                    x=frame.index,
                    y=frame["vwap"],
                    mode="lines",
                    name="VWAP",
                    line=dict(width=1.2, color="#1f77b4"),
                )
            )
    fig.update_layout(
        title=TITLE,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def main():
    data = pd.read_csv(DATA_CSV_PATH)
    data_expanded = expand_to_seconds_sessions_only(data)
    figure = make_figure(data_expanded)
    html = pio.to_html(figure, full_html=True, include_plotlyjs=True)
    with open(OUTPUT_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
