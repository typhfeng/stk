import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


@dataclass
class Config:
    # IO
    CSV_PATH: str = os.path.join("config", "sample", "PredictorPortsFull.csv")
    OUTPUT_PATH: str = os.path.join("output", "signals_overlapped.html")

    # Columns
    DATE_COL: str = "date"
    SIGNAL_COL: str = "signalname"
    PORT_COL: str = "port"
    RET_COL: str = "ret"  # in percent, e.g., 2.5 means 2.5%

    # Filters
    PORT_FILTER = None  # e.g., "01" to only use a specific port
    START_DATE = None  # e.g., "1990-01-01"
    END_DATE = None  # e.g., "2024-12-31"

    # Plot options
    PLOT_MODE: str = "cumulative"  # force cumulative for subplots
    MAX_SIGNALS = None  # cap number of signals to plot by data availability
    LINE_WIDTH: float = 1.1
    ALPHA: float = 0.9
    AUTO_OPEN: bool = True

    # Data cleaning
    RET_ABS_CLIP: float = 50.0  # cap monthly returns to +/- this percent

    # Benchmark (S&P 500) options
    BENCHMARK_SIGNAL_CANDIDATES = ["SP500", "S&P500", "SPX", "MKT", "MARKET"]
    BENCHMARK_FILE = None  # Optional: path to external benchmark CSV with columns [date, ret]
    BENCHMARK_DATE_COL = "date"
    BENCHMARK_RET_COL = "ret"  # percent
    BENCHMARK_NAME = "S&P 500 avg"
    BENCHMARK_LINE_WIDTH: float = 4.0
    BENCHMARK_COLOR = "red"
    BENCHMARK_LINE_DASH = "dash"
    BENCHMARK_AVG_FROM_INTERNET: bool = True
    BENCHMARK_AVG_MONTHLY_RET_PCT = None  # if set (e.g., 0.7), skip internet
    BENCHMARK_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500"

    # NASDAQ benchmark
    NASDAQ_NAME = "NASDAQ avg"
    NASDAQ_LINE_WIDTH: float = 3.5
    NASDAQ_COLOR = "blue"
    NASDAQ_LINE_DASH = "dot"
    NASDAQ_AVG_FROM_INTERNET: bool = True
    NASDAQ_AVG_MONTHLY_RET_PCT = None
    NASDAQ_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQCOM"

    # Long/Short definition
    # If specified, use these port labels to compute long/short (mean across them).
    # Otherwise, use max(ret) as long and min(ret) as short per (date, signalname).
    LONG_PORT_LABELS = None  # e.g., ["10"] or ["05","06","07","08","09","10"]
    SHORT_PORT_LABELS = None  # e.g., ["01"] or ["01","02","03","04","05"]


class SignalPlotter:
    def __init__(self, config: Config):
        self.cfg = config

    def load_data(self) -> pd.DataFrame:
        # Read only needed columns for speed
        use_cols = [self.cfg.DATE_COL, self.cfg.SIGNAL_COL, self.cfg.PORT_COL, self.cfg.RET_COL]
        df = pd.read_csv(self.cfg.CSV_PATH, usecols=use_cols)
        assert set([self.cfg.DATE_COL, self.cfg.SIGNAL_COL, self.cfg.RET_COL]).issubset(df.columns)

        # Parse dates
        df[self.cfg.DATE_COL] = pd.to_datetime(df[self.cfg.DATE_COL])

        # Optional filters
        if self.cfg.PORT_FILTER is not None and self.cfg.PORT_COL in df.columns:
            df = df[df[self.cfg.PORT_COL] == self.cfg.PORT_FILTER]

        if self.cfg.START_DATE is not None:
            df = df[df[self.cfg.DATE_COL] >= pd.to_datetime(self.cfg.START_DATE)]
        if self.cfg.END_DATE is not None:
            df = df[df[self.cfg.DATE_COL] <= pd.to_datetime(self.cfg.END_DATE)]

        # Clip monthly returns to reduce noise/outliers
        df[self.cfg.RET_COL] = df[self.cfg.RET_COL].clip(-self.cfg.RET_ABS_CLIP, self.cfg.RET_ABS_CLIP)

        return df

    def _pivot_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        ret_pivot = df.pivot(index=self.cfg.DATE_COL, columns=self.cfg.SIGNAL_COL, values=self.cfg.RET_COL)
        ret_pivot.sort_index(inplace=True)
        if self.cfg.MAX_SIGNALS is not None:
            counts = ret_pivot.notna().sum().sort_values(ascending=False)
            keep_cols = counts.index[: self.cfg.MAX_SIGNALS]
            ret_pivot = ret_pivot.loc[:, keep_cols]
        return ret_pivot

    def _compute_long_short_tables(self, df: pd.DataFrame):
        # df has columns: date, signalname, port, ret
        # Compute long and short per (date, signalname)
        g = df.groupby([self.cfg.DATE_COL, self.cfg.SIGNAL_COL])

        if self.cfg.LONG_PORT_LABELS is not None:
            long_series = (
                df[df[self.cfg.PORT_COL].isin(self.cfg.LONG_PORT_LABELS)]
                .groupby([self.cfg.DATE_COL, self.cfg.SIGNAL_COL])[self.cfg.RET_COL]
                .mean()
            )
        else:
            long_series = g[self.cfg.RET_COL].max()

        if self.cfg.SHORT_PORT_LABELS is not None:
            short_series = (
                df[df[self.cfg.PORT_COL].isin(self.cfg.SHORT_PORT_LABELS)]
                .groupby([self.cfg.DATE_COL, self.cfg.SIGNAL_COL])[self.cfg.RET_COL]
                .mean()
            )
        else:
            short_series = g[self.cfg.RET_COL].min()

        ls_series = long_series - short_series

        long_df = long_series.reset_index().pivot(index=self.cfg.DATE_COL, columns=self.cfg.SIGNAL_COL, values=self.cfg.RET_COL)
        short_df = short_series.reset_index().pivot(index=self.cfg.DATE_COL, columns=self.cfg.SIGNAL_COL, values=self.cfg.RET_COL)
        ls_df = ls_series.reset_index().pivot(index=self.cfg.DATE_COL, columns=self.cfg.SIGNAL_COL, values=self.cfg.RET_COL)

        # Sort and optionally cap number of signals
        for tbl_name, tbl in ("long", long_df), ("short", short_df), ("ls", ls_df):
            tbl.sort_index(inplace=True)
            if self.cfg.MAX_SIGNALS is not None:
                counts = tbl.notna().sum().sort_values(ascending=False)
                keep_cols = counts.index[: self.cfg.MAX_SIGNALS]
                tbl = tbl.loc[:, keep_cols]
            if tbl_name == "long":
                long_df = tbl
            elif tbl_name == "short":
                short_df = tbl
            else:
                ls_df = tbl

        return long_df, short_df, ls_df

    def _compute_cumulative_index(self, monthly_ret_pct: pd.DataFrame) -> pd.DataFrame:
        # Additive cumulative sum in percentage points, baseline = 0 at first valid
        def cum_additive_from_first_valid(col: pd.Series) -> pd.Series:
            mask = col.notna()
            if not mask.any():
                return pd.Series(index=col.index, dtype=float)
            idx = col.index
            s = col[mask]
            cum = s.cumsum()
            cum = cum - cum.iloc[0]
            out = pd.Series(np.nan, index=idx, dtype=float)
            out.loc[s.index] = cum.values
            return out

        cum_sum = monthly_ret_pct.apply(cum_additive_from_first_valid, axis=0)
        return cum_sum

    def _compute_benchmark_cum(self, df: pd.DataFrame) -> pd.Series | None:
        # Try from candidate signalnames
        bench = None
        candidates = [c for c in self.cfg.BENCHMARK_SIGNAL_CANDIDATES if c in df[self.cfg.SIGNAL_COL].unique()]
        if len(candidates) > 0:
            name = candidates[0]
            tmp = df[df[self.cfg.SIGNAL_COL] == name]
            s = tmp.groupby(self.cfg.DATE_COL)[self.cfg.RET_COL].mean().sort_index()
            s = s.clip(-self.cfg.RET_ABS_CLIP, self.cfg.RET_ABS_CLIP)
            cum = s.cumsum()
            if len(cum) > 0:
                cum = cum - cum.iloc[0]
            bench = cum
        elif self.cfg.BENCHMARK_FILE is not None and os.path.exists(self.cfg.BENCHMARK_FILE):
            bdf = pd.read_csv(self.cfg.BENCHMARK_FILE, usecols=[self.cfg.BENCHMARK_DATE_COL, self.cfg.BENCHMARK_RET_COL])
            bdf[self.cfg.BENCHMARK_DATE_COL] = pd.to_datetime(bdf[self.cfg.BENCHMARK_DATE_COL])
            s = (
                bdf.groupby(self.cfg.BENCHMARK_DATE_COL)[self.cfg.BENCHMARK_RET_COL]
                .mean()
                .sort_index()
                .clip(-self.cfg.RET_ABS_CLIP, self.cfg.RET_ABS_CLIP)
            )
            cum = s.cumsum()
            if len(cum) > 0:
                cum = cum - cum.iloc[0]
            bench = cum
        return bench

    def _fetch_index_avg_monthly_pct_from_fred(self, url: str) -> float:
        bdf = pd.read_csv(url)
        bdf.columns = [c.lower() for c in bdf.columns]
        date_col = "date" if "date" in bdf.columns else bdf.columns[0]
        val_col = [c for c in bdf.columns if c != date_col][0]
        bdf[date_col] = pd.to_datetime(bdf[date_col])
        bdf = bdf[[date_col, val_col]].dropna()
        bdf = bdf.set_index(date_col).sort_index()
        monthly = bdf.resample("ME").last()
        monthly_ret_pct = monthly.pct_change() * 100.0
        monthly_ret_pct = monthly_ret_pct.clip(-self.cfg.RET_ABS_CLIP, self.cfg.RET_ABS_CLIP)
        avg = float(monthly_ret_pct.mean().values[0])
        return avg

    def _get_benchmark_avg_monthly_pct(self) -> float:
        if self.cfg.BENCHMARK_AVG_MONTHLY_RET_PCT is not None:
            return float(self.cfg.BENCHMARK_AVG_MONTHLY_RET_PCT)
        if self.cfg.BENCHMARK_AVG_FROM_INTERNET:
            return self._fetch_index_avg_monthly_pct_from_fred(self.cfg.BENCHMARK_FRED_URL)
        # Fallback constant (approx long-run nominal monthly %)
        return 0.8

    def _get_nasdaq_avg_monthly_pct(self) -> float:
        if self.cfg.NASDAQ_AVG_MONTHLY_RET_PCT is not None:
            return float(self.cfg.NASDAQ_AVG_MONTHLY_RET_PCT)
        if self.cfg.NASDAQ_AVG_FROM_INTERNET:
            return self._fetch_index_avg_monthly_pct_from_fred(self.cfg.NASDAQ_FRED_URL)
        # Fallback approximate
        return 1.0

    @staticmethod
    def _build_avg_line(idx: pd.DatetimeIndex, avg_pct: float) -> pd.Series:
        steps = np.arange(len(idx), dtype=float)
        values = steps * avg_pct
        return pd.Series(values, index=idx)

    @staticmethod
    def _last_valid_points(frame: pd.DataFrame):
        last_points = {}
        for col in frame.columns:
            series = frame[col].dropna()
            if series.shape[0] == 0:
                continue
            last_points[col] = (series.index[-1], float(series.iloc[-1]))
        return last_points

    def plot(self) -> None:
        df = self.load_data()
        long_tbl, short_tbl, ls_tbl = self._compute_long_short_tables(df)
        # Always cumulative for subplots
        long_plot = self._compute_cumulative_index(long_tbl)
        short_plot = self._compute_cumulative_index(short_tbl)
        ls_plot = self._compute_cumulative_index(ls_tbl)
        avg_pct_sp = self._get_benchmark_avg_monthly_pct()
        avg_pct_ndq = self._get_nasdaq_avg_monthly_pct()
        ylabel = "Additive cumulative return (%)"

        os.makedirs(os.path.dirname(self.cfg.OUTPUT_PATH), exist_ok=True)

        from plotly.subplots import make_subplots

        fig = make_subplots(rows=1, cols=3, subplot_titles=("Long", "Short", "Long - Short"), shared_yaxes=True)

        for name in long_plot.columns:
            fig.add_trace(
                go.Scatter(x=long_plot.index, y=long_plot[name], mode="lines", name=str(name),
                           line=dict(width=self.cfg.LINE_WIDTH), opacity=self.cfg.ALPHA, legendgroup=str(name), showlegend=False),
                row=1, col=1,
            )
        for name in short_plot.columns:
            fig.add_trace(
                go.Scatter(x=short_plot.index, y=short_plot[name], mode="lines", name=str(name),
                           line=dict(width=self.cfg.LINE_WIDTH), opacity=self.cfg.ALPHA, legendgroup=str(name), showlegend=False),
                row=1, col=2,
            )
        for name in ls_plot.columns:
            fig.add_trace(
                go.Scatter(x=ls_plot.index, y=ls_plot[name], mode="lines", name=str(name),
                           line=dict(width=self.cfg.LINE_WIDTH), opacity=self.cfg.ALPHA, legendgroup=str(name), showlegend=False),
                row=1, col=3,
            )

        # Add benchmark average straight lines (S&P 500 and NASDAQ) to all subplots
        sp_long = self._build_avg_line(long_plot.index, avg_pct_sp)
        sp_short = self._build_avg_line(short_plot.index, -avg_pct_sp)
        sp_ls = self._build_avg_line(ls_plot.index, avg_pct_sp)

        ndq_long = self._build_avg_line(long_plot.index, avg_pct_ndq)
        ndq_short = self._build_avg_line(short_plot.index, -avg_pct_ndq)
        ndq_ls = self._build_avg_line(ls_plot.index, avg_pct_ndq)

        # S&P lines
        fig.add_trace(
            go.Scatter(x=sp_long.index, y=sp_long.values, mode="lines", name=self.cfg.BENCHMARK_NAME,
                       line=dict(width=self.cfg.BENCHMARK_LINE_WIDTH, color=self.cfg.BENCHMARK_COLOR, dash=self.cfg.BENCHMARK_LINE_DASH),
                       opacity=1.0, legendgroup="__sp__"),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=sp_short.index, y=sp_short.values, mode="lines", name=self.cfg.BENCHMARK_NAME,
                       line=dict(width=self.cfg.BENCHMARK_LINE_WIDTH, color=self.cfg.BENCHMARK_COLOR, dash=self.cfg.BENCHMARK_LINE_DASH),
                       opacity=1.0, legendgroup="__sp__", showlegend=False),
            row=1, col=2,
        )
        fig.add_trace(
            go.Scatter(x=sp_ls.index, y=sp_ls.values, mode="lines", name=self.cfg.BENCHMARK_NAME,
                       line=dict(width=self.cfg.BENCHMARK_LINE_WIDTH, color=self.cfg.BENCHMARK_COLOR, dash=self.cfg.BENCHMARK_LINE_DASH),
                       opacity=1.0, legendgroup="__sp__", showlegend=False),
            row=1, col=3,
        )

        # NASDAQ lines
        fig.add_trace(
            go.Scatter(x=ndq_long.index, y=ndq_long.values, mode="lines", name=self.cfg.NASDAQ_NAME,
                       line=dict(width=self.cfg.NASDAQ_LINE_WIDTH, color=self.cfg.NASDAQ_COLOR, dash=self.cfg.NASDAQ_LINE_DASH),
                       opacity=1.0, legendgroup="__ndq__"),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=ndq_short.index, y=ndq_short.values, mode="lines", name=self.cfg.NASDAQ_NAME,
                       line=dict(width=self.cfg.NASDAQ_LINE_WIDTH, color=self.cfg.NASDAQ_COLOR, dash=self.cfg.NASDAQ_LINE_DASH),
                       opacity=1.0, legendgroup="__ndq__", showlegend=False),
            row=1, col=2,
        )
        fig.add_trace(
            go.Scatter(x=ndq_ls.index, y=ndq_ls.values, mode="lines", name=self.cfg.NASDAQ_NAME,
                       line=dict(width=self.cfg.NASDAQ_LINE_WIDTH, color=self.cfg.NASDAQ_COLOR, dash=self.cfg.NASDAQ_LINE_DASH),
                       opacity=1.0, legendgroup="__ndq__", showlegend=False),
            row=1, col=3,
        )

        # Label series at the right edge

        # Extend x range to make room for labels
        def extend_range(idx: pd.DatetimeIndex):
            if idx.shape[0] == 0:
                return None
            return [idx.min(), idx.max() + pd.Timedelta(days=28)]

        fig.update_xaxes(range=extend_range(long_plot.index), row=1, col=1)
        fig.update_xaxes(range=extend_range(short_plot.index), row=1, col=2)
        fig.update_xaxes(range=extend_range(ls_plot.index), row=1, col=3)

        # Add end labels for signals
        def add_labels(frame: pd.DataFrame, row: int, col: int, text_color: str = "#666"):
            last_points = self._last_valid_points(frame)
            for name, (xv, yv) in last_points.items():
                fig.add_trace(
                    go.Scatter(x=[xv], y=[yv], mode="text", text=[str(name)],
                               textposition="middle right", textfont=dict(size=10, color=text_color), showlegend=False),
                    row=row, col=col,
                )

        add_labels(long_plot, 1, 1, "#666")
        add_labels(short_plot, 1, 2, "#666")
        add_labels(ls_plot, 1, 3, "#666")

        # Add end labels for benchmarks
        def add_bench_label(series: pd.Series, name: str, color: str, row: int, col: int):
            if series.shape[0] == 0:
                return
            fig.add_trace(
                go.Scatter(x=[series.index[-1]], y=[float(series.iloc[-1])], mode="text", text=[name],
                           textposition="middle right", textfont=dict(size=12, color=color), showlegend=False),
                row=row, col=col,
            )

        add_bench_label(sp_long, self.cfg.BENCHMARK_NAME, self.cfg.BENCHMARK_COLOR, 1, 1)
        add_bench_label(sp_short, self.cfg.BENCHMARK_NAME, self.cfg.BENCHMARK_COLOR, 1, 2)
        add_bench_label(sp_ls, self.cfg.BENCHMARK_NAME, self.cfg.BENCHMARK_COLOR, 1, 3)

        add_bench_label(ndq_long, self.cfg.NASDAQ_NAME, self.cfg.NASDAQ_COLOR, 1, 1)
        add_bench_label(ndq_short, self.cfg.NASDAQ_NAME, self.cfg.NASDAQ_COLOR, 1, 2)
        add_bench_label(ndq_ls, self.cfg.NASDAQ_NAME, self.cfg.NASDAQ_COLOR, 1, 3)

        fig.update_layout(
            title="Signals overlapped: cumulative (additive) returns in %",
            template="plotly_white",
        )
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_xaxes(title_text="Date", row=1, col=3)
        fig.update_yaxes(title_text=ylabel, row=1, col=1)
        fig.update_yaxes(row=1, col=2)
        fig.update_yaxes(row=1, col=3)

        pio.write_html(fig, file=self.cfg.OUTPUT_PATH, include_plotlyjs="cdn", auto_open=self.cfg.AUTO_OPEN)
        print(f"Saved: {self.cfg.OUTPUT_PATH}")


if __name__ == "__main__":
    plotter = SignalPlotter(Config())
    plotter.plot()


