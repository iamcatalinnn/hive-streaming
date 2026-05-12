"""Streamlit dashboard for QoS analytics — Hive Streaming branded."""

import glob
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Hive Streaming — QoS Analytics",
    page_icon="🐝",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Hive Streaming Brand Colors ───────────────────────────────────────────────
# Extracted from hivestreaming.com
# Primary dark navy background, orange accent, clean whites

HIVE_ORANGE   = "#FF6B2B"       # primary accent
HIVE_NAVY     = "#0A0E1A"       # page background
HIVE_CARD     = "#131929"       # card background
HIVE_BORDER   = "#1E2A3A"       # card border
HIVE_TEXT     = "#FFFFFF"       # primary text
HIVE_MUTED    = "#8892A4"       # muted text
HIVE_BLUE     = "#1B8EF2"       # secondary accent

QOS_GREEN     = "#00C897"       # success green
QOS_YELLOW    = "#F5A623"       # warning amber
QOS_RED       = "#FF4757"       # error red

QOS_COLORS = {
    'green':  QOS_GREEN,
    'yellow': QOS_YELLOW,
    'red':    QOS_RED
}

QUALITY_ORDER = [
    '144p', '270p', '360p', '480p',
    '540p', '720p', '1080p', '1440p', '2160p'
]

# ── Global CSS ────────────────────────────────────────────────────────────────

st.markdown(f"""
<style>
    /* ── Base ── */
    .stApp {{
        background-color: {HIVE_NAVY};
        color: {HIVE_TEXT};
    }}

    /* ── Hide Streamlit chrome ── */
    #MainMenu, footer, header {{visibility: hidden;}}
    .block-container {{
        padding-top: 0rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }}

    /* ── Top nav bar ── */
    .hive-nav {{
        background-color: {HIVE_CARD};
        border-bottom: 1px solid {HIVE_BORDER};
        padding: 16px 32px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 32px;
        margin-left: -4rem;
        margin-right: -4rem;
    }}
    .hive-logo {{
        font-size: 20px;
        font-weight: 700;
        color: {HIVE_TEXT};
        letter-spacing: -0.5px;
    }}
    .hive-logo span {{
        color: {HIVE_ORANGE};
    }}
    .hive-nav-right {{
        font-size: 13px;
        color: {HIVE_MUTED};
    }}

    /* ── Section headers ── */
    .section-header {{
        font-size: 13px;
        font-weight: 600;
        color: {HIVE_ORANGE};
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-bottom: 4px;
        margin-top: 32px;
    }}
    .section-title {{
        font-size: 22px;
        font-weight: 700;
        color: {HIVE_TEXT};
        margin-bottom: 20px;
    }}

    /* ── KPI cards ── */
    .kpi-card {{
        background-color: {HIVE_CARD};
        border: 1px solid {HIVE_BORDER};
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }}
    .kpi-label {{
        font-size: 12px;
        color: {HIVE_MUTED};
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
    }}
    .kpi-value {{
        font-size: 32px;
        font-weight: 700;
        color: {HIVE_TEXT};
        line-height: 1;
    }}
    .kpi-sub {{
        font-size: 12px;
        color: {HIVE_MUTED};
        margin-top: 4px;
    }}
    .kpi-green  .kpi-value {{ color: {QOS_GREEN};  }}
    .kpi-yellow .kpi-value {{ color: {QOS_YELLOW}; }}
    .kpi-red    .kpi-value {{ color: {QOS_RED};    }}
    .kpi-orange .kpi-value {{ color: {HIVE_ORANGE};}}

    /* ── Chart cards ── */
    .chart-card {{
        background-color: {HIVE_CARD};
        border: 1px solid {HIVE_BORDER};
        border-radius: 12px;
        padding: 20px 24px;
        margin-bottom: 16px;
    }}
    .chart-title {{
        font-size: 14px;
        font-weight: 600;
        color: {HIVE_TEXT};
        margin-bottom: 4px;
    }}
    .chart-sub {{
        font-size: 12px;
        color: {HIVE_MUTED};
        margin-bottom: 16px;
    }}

    /* ── Divider ── */
    .hive-divider {{
        border: none;
        border-top: 1px solid {HIVE_BORDER};
        margin: 32px 0;
    }}

    /* ── Table ── */
    .dataframe {{
        background-color: {HIVE_CARD} !important;
        color: {HIVE_TEXT} !important;
    }}

    /* ── Plotly background match ── */
    .js-plotly-plot .plotly .bg {{
        fill: {HIVE_CARD} !important;
    }}
</style>
""", unsafe_allow_html=True)


# ── Plotly Theme ──────────────────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor=HIVE_CARD,
    plot_bgcolor=HIVE_CARD,
    font=dict(color=HIVE_TEXT, family="Inter, sans-serif", size=12),
    margin=dict(t=16, b=40, l=40, r=16),
    height=300,
    xaxis=dict(
        gridcolor=HIVE_BORDER,
        linecolor=HIVE_BORDER,
        tickcolor=HIVE_MUTED,
        tickfont=dict(color=HIVE_MUTED, size=10),
        title_font=dict(color=HIVE_MUTED)
    ),
    yaxis=dict(
        gridcolor=HIVE_BORDER,
        linecolor=HIVE_BORDER,
        tickcolor=HIVE_MUTED,
        tickfont=dict(color=HIVE_MUTED, size=10),
        title_font=dict(color=HIVE_MUTED)
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor=HIVE_BORDER,
        font=dict(color=HIVE_MUTED, size=11)
    )
)


# ── Load Data ─────────────────────────────────────────────────────────────────

def _latest_event_date(base: str = "output/gold") -> str:
    partitions = sorted(glob.glob(os.path.join(base, "eventDate=*")))
    if not partitions:
        st.error(f"No eventDate partitions found under {base}/. Run the pipeline first.")
        st.stop()
    return partitions[-1].split("eventDate=")[-1]

EVENT_DATE = _latest_event_date()

@st.cache_data
def load_gold():
    return pd.read_parquet(
        f'output/gold/eventDate={EVENT_DATE}/part-0.parquet'
    )

@st.cache_data
def load_silver_sessions():
    return pd.read_parquet(
        f'output/silver_sessions/eventDate={EVENT_DATE}/part-0.parquet'
    )

@st.cache_data
def load_silver_quality():
    return pd.read_parquet(
        f'output/silver_quality/eventDate={EVENT_DATE}/part-0.parquet'
    )

gold     = load_gold()
silver_s = load_silver_sessions()
silver_q = load_silver_quality()

# ── Derived KPIs ──────────────────────────────────────────────────────────────

total_viewers   = len(gold)
green_count     = (gold['qos_label'] == 'green').sum()
yellow_count    = (gold['qos_label'] == 'yellow').sum()
red_count       = (gold['qos_label'] == 'red').sum()
green_pct       = 100 * green_count  / total_viewers
yellow_pct      = 100 * yellow_count / total_viewers
red_pct         = 100 * red_count    / total_viewers
avg_buffering   = gold['buffering_ratio'].mean() * 100
avg_p2p         = gold['p2p_ratio'].mean() * 100
avg_qos         = gold['qos_score'].mean()
avg_session_min = gold['session_duration_min'].mean()

# ── Nav Bar ───────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="hive-nav">
    <div class="hive-logo">Hive <span>Streaming</span></div>
    <div class="hive-nav-right">
        QoS Analytics Dashboard &nbsp;·&nbsp; Event Date: {EVENT_DATE}
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPI Row ───────────────────────────────────────────────────────────────────

st.markdown('<div class="section-header">Event Overview</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Quality of Service Summary</div>', unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

def kpi(col, label, value, sub, extra_class=""):
    col.markdown(f"""
    <div class="kpi-card {extra_class}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>
    """, unsafe_allow_html=True)

kpi(c1, "Total Viewers",      total_viewers,        "this event")
kpi(c2, "🟢 Good QoS",        f"{green_pct:.0f}%",  f"{green_count} viewers",  "kpi-green")
kpi(c3, "🟡 Degraded QoS",    f"{yellow_pct:.0f}%", f"{yellow_count} viewers", "kpi-yellow")
kpi(c4, "🔴 Poor QoS",        f"{red_pct:.0f}%",    f"{red_count} viewers",    "kpi-red")
kpi(c5, "Avg Buffering",      f"{avg_buffering:.1f}%", "of session time")
kpi(c6, "P2P Offload",        f"{avg_p2p:.0f}%",    "avg traffic from peers",  "kpi-orange")

st.markdown('<hr class="hive-divider">', unsafe_allow_html=True)

# ── Row 1: QoS Distribution + Buffering ──────────────────────────────────────

st.markdown('<div class="section-header">Viewer Experience</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Buffering & QoS Distribution</div>', unsafe_allow_html=True)

col1, col2 = st.columns([1, 2])

with col1:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">QoS Label Breakdown</div>
        <div class="chart-sub">Viewer distribution by experience quality</div>
    </div>
    """, unsafe_allow_html=True)

    qos_counts = gold['qos_label'].value_counts().reset_index()
    qos_counts.columns = ['qos_label', 'count']
    qos_counts['order'] = qos_counts['qos_label'].map(
        {'green': 0, 'yellow': 1, 'red': 2}
    )
    qos_counts = qos_counts.sort_values('order')

    fig_pie = go.Figure(go.Pie(
        labels=qos_counts['qos_label'],
        values=qos_counts['count'],
        hole=0.6,
        marker=dict(
            colors=[QOS_COLORS[l] for l in qos_counts['qos_label']],
            line=dict(color=HIVE_CARD, width=3)
        ),
        textinfo='percent+label',
        textfont=dict(color=HIVE_TEXT, size=12),
        hovertemplate='<b>%{label}</b><br>%{value} viewers (%{percent})<extra></extra>'
    ))
    fig_pie.add_annotation(
        text=f"<b>{total_viewers}</b><br><span style='font-size:10px'>viewers</span>",
        x=0.5, y=0.5,
        font=dict(size=16, color=HIVE_TEXT),
        showarrow=False
    )
    fig_pie.update_layout(**{**PLOTLY_LAYOUT, 'height': 280, 'showlegend': False})
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">Buffering Ratio per Viewer</div>
        <div class="chart-sub">Percentage of session time spent buffering — coloured by QoS label</div>
    </div>
    """, unsafe_allow_html=True)

    gold_sorted = gold.sort_values('buffering_ratio', ascending=False).copy()
    gold_sorted['viewer_short'] = gold_sorted['client_id'].str[:8] + '...'

    fig_buf = go.Figure()
    for label in ['red', 'yellow', 'green']:
        subset = gold_sorted[gold_sorted['qos_label'] == label]
        fig_buf.add_trace(go.Bar(
            x=subset['viewer_short'],
            y=subset['buffering_ratio'],
            name=label.capitalize(),
            marker_color=QOS_COLORS[label],
            hovertemplate=(
                '<b>%{x}</b><br>'
                'Buffering: %{y:.1%}<extra></extra>'
            )
        ))

    fig_buf.add_hline(
        y=0.05, line_dash='dash', line_color=QOS_GREEN, line_width=1,
        annotation_text="Green threshold (5%)",
        annotation_font=dict(color=QOS_GREEN, size=10)
    )
    fig_buf.add_hline(
        y=0.35, line_dash='dash', line_color=QOS_RED, line_width=1,
        annotation_text="Red threshold (35%)",
        annotation_font=dict(color=QOS_RED, size=10)
    )

    fig_buf.update_layout(
        **{**PLOTLY_LAYOUT,
           'barmode': 'overlay',
           'showlegend': True,
           'yaxis': {**PLOTLY_LAYOUT['yaxis'], 'tickformat': ',.0%'},
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'tickangle': 35}
        }
    )
    st.plotly_chart(fig_buf, use_container_width=True)

st.markdown('<hr class="hive-divider">', unsafe_allow_html=True)

# ── Row 2: Quality + Traffic ──────────────────────────────────────────────────

st.markdown('<div class="section-header">Video Quality & Traffic</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Quality Consumption & P2P Delivery</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">Dominant Quality Distribution</div>
        <div class="chart-sub">Most consumed quality level per viewer session</div>
    </div>
    """, unsafe_allow_html=True)

    quality_counts = gold['dominant_quality'].value_counts().reset_index()
    quality_counts.columns = ['quality', 'count']
    quality_counts['order'] = quality_counts['quality'].apply(
        lambda q: QUALITY_ORDER.index(q) if q in QUALITY_ORDER else 99
    )
    quality_counts = quality_counts.sort_values('order')

    # orange gradient per quality level
    n = len(quality_counts)
    colors = [
        f"rgba(255, 107, 43, {0.4 + 0.6 * i / max(n-1, 1)})"
        for i in range(n)
    ]

    fig_qual = go.Figure(go.Bar(
        x=quality_counts['quality'],
        y=quality_counts['count'],
        marker=dict(
            color=colors,
            line=dict(color=HIVE_BORDER, width=1)
        ),
        hovertemplate='<b>%{x}</b><br>%{y} viewers<extra></extra>'
    ))
    fig_qual.update_layout(
        **{**PLOTLY_LAYOUT,
           'yaxis': {**PLOTLY_LAYOUT['yaxis'], 'title': 'Viewers'}
        }
    )
    st.plotly_chart(fig_qual, use_container_width=True)

with col2:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">P2P vs CDN Traffic per Viewer</div>
        <div class="chart-sub">Bytes received from peer network vs central CDN</div>
    </div>
    """, unsafe_allow_html=True)

    traffic = gold.copy()
    traffic['viewer_short'] = traffic['client_id'].str[:8] + '...'
    traffic['p2p_mb']    = traffic['total_p2p_received_bytes']    / 1e6
    traffic['source_mb'] = traffic['total_source_received_bytes'] / 1e6
    traffic = traffic.sort_values('p2p_mb', ascending=False)

    fig_traffic = go.Figure()
    fig_traffic.add_trace(go.Bar(
        name='P2P',
        x=traffic['viewer_short'],
        y=traffic['p2p_mb'],
        marker_color=HIVE_ORANGE,
        hovertemplate='P2P: %{y:.1f} MB<extra></extra>'
    ))
    fig_traffic.add_trace(go.Bar(
        name='CDN',
        x=traffic['viewer_short'],
        y=traffic['source_mb'],
        marker_color=HIVE_BLUE,
        hovertemplate='CDN: %{y:.1f} MB<extra></extra>'
    ))
    fig_traffic.update_layout(
        **{**PLOTLY_LAYOUT,
           'barmode': 'stack',
           'yaxis': {**PLOTLY_LAYOUT['yaxis'], 'title': 'MB'},
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'tickangle': 35},
           'legend': dict(
               orientation='h', y=1.05,
               bgcolor='rgba(0,0,0,0)',
               font=dict(color=HIVE_MUTED, size=11)
           )
        }
    )
    st.plotly_chart(fig_traffic, use_container_width=True)

st.markdown('<hr class="hive-divider">', unsafe_allow_html=True)

# ── Row 3: QoS Score + Scatter ────────────────────────────────────────────────

st.markdown('<div class="section-header">Session Analysis</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">QoS Score Distribution & Session Patterns</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">QoS Score Distribution</div>
        <div class="chart-sub">Composite score (0–1) across all viewer sessions</div>
    </div>
    """, unsafe_allow_html=True)

    fig_hist = go.Figure()
    for label in ['green', 'yellow', 'red']:
        subset = gold[gold['qos_label'] == label]
        fig_hist.add_trace(go.Histogram(
            x=subset['qos_score'],
            name=label.capitalize(),
            marker_color=QOS_COLORS[label],
            opacity=0.85,
            nbinsx=15,
            hovertemplate=f'Score: %{{x:.2f}}<br>Count: %{{y}}<extra></extra>'
        ))

    fig_hist.add_vline(
        x=0.75, line_dash='dash', line_color=QOS_GREEN, line_width=1,
        annotation_text="Green (0.75)",
        annotation_font=dict(color=QOS_GREEN, size=10)
    )
    fig_hist.add_vline(
        x=0.50, line_dash='dash', line_color=QOS_YELLOW, line_width=1,
        annotation_text="Yellow (0.50)",
        annotation_font=dict(color=QOS_YELLOW, size=10)
    )
    fig_hist.update_layout(
        **{**PLOTLY_LAYOUT,
           'barmode': 'overlay',
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'title': 'QoS Score'},
           'yaxis': {**PLOTLY_LAYOUT['yaxis'], 'title': 'Viewers'},
        }
    )
    st.plotly_chart(fig_hist, use_container_width=True)

with col2:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">Session Duration vs Buffering</div>
        <div class="chart-sub">Bubble size = quality switches. Hover for viewer detail.</div>
    </div>
    """, unsafe_allow_html=True)

    gold_plot = gold.copy()
    gold_plot['viewer_short'] = gold_plot['client_id'].str[:8] + '...'

    fig_scatter = go.Figure()
    for label in ['green', 'yellow', 'red']:
        subset = gold_plot[gold_plot['qos_label'] == label]
        fig_scatter.add_trace(go.Scatter(
            x=subset['session_duration_min'],
            y=subset['buffering_ratio'],
            mode='markers',
            name=label.capitalize(),
            marker=dict(
                color=QOS_COLORS[label],
                size=subset['quality_switches'] * 1.5 + 8,
                opacity=0.8,
                line=dict(color=HIVE_BORDER, width=1)
            ),
            text=subset['viewer_short'],
            hovertemplate=(
                '<b>%{text}</b><br>'
                'Duration: %{x:.1f} min<br>'
                'Buffering: %{y:.1%}<br>'
                'Quality: ' + subset['dominant_quality'].astype(str) + '<extra></extra>'
            )
        ))

    fig_scatter.update_layout(
        **{**PLOTLY_LAYOUT,
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'title': 'Session Duration (min)'},
           'yaxis': {**PLOTLY_LAYOUT['yaxis'],
                     'title': 'Buffering Ratio', 'tickformat': ',.0%'},
        }
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown('<hr class="hive-divider">', unsafe_allow_html=True)

# ── Row 4: Stability + Delivery ───────────────────────────────────────────────

st.markdown('<div class="section-header">Stability & Delivery</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Quality Switches & Request Fulfilment</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">Quality Switches per Viewer</div>
        <div class="chart-sub">Cross-window quality transitions — higher = more unstable</div>
    </div>
    """, unsafe_allow_html=True)

    sw_df = gold.copy()
    sw_df['viewer_short'] = sw_df['client_id'].str[:8] + '...'
    sw_df = sw_df.sort_values('quality_switches', ascending=False)

    fig_sw = go.Figure(go.Bar(
        x=sw_df['viewer_short'],
        y=sw_df['quality_switches'],
        marker=dict(
            color=sw_df['qos_label'].map(QOS_COLORS),
            line=dict(color=HIVE_BORDER, width=1)
        ),
        hovertemplate='<b>%{x}</b><br>Switches: %{y}<extra></extra>'
    ))
    fig_sw.update_layout(
        **{**PLOTLY_LAYOUT,
           'yaxis': {**PLOTLY_LAYOUT['yaxis'], 'title': 'Quality Switches'},
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'tickangle': 35}
        }
    )
    st.plotly_chart(fig_sw, use_container_width=True)

with col2:
    st.markdown("""
    <div class="chart-card">
        <div class="chart-title">Delivery Rate per Viewer</div>
        <div class="chart-sub">Ratio of received vs requested bytes — below 100% = dropped chunks</div>
    </div>
    """, unsafe_allow_html=True)

    del_df = gold.copy()
    del_df['viewer_short'] = del_df['client_id'].str[:8] + '...'
    del_df = del_df.sort_values('delivery_rate')

    fig_del = go.Figure(go.Bar(
        x=del_df['viewer_short'],
        y=del_df['delivery_rate'],
        marker=dict(
            color=[
                QOS_GREEN if v >= 0.99 else QOS_YELLOW if v >= 0.95 else QOS_RED
                for v in del_df['delivery_rate']
            ],
            line=dict(color=HIVE_BORDER, width=1)
        ),
        hovertemplate='<b>%{x}</b><br>Delivery: %{y:.1%}<extra></extra>'
    ))
    fig_del.add_hline(
        y=1.0, line_dash='dash', line_color=QOS_GREEN, line_width=1,
        annotation_text="Perfect (100%)",
        annotation_font=dict(color=QOS_GREEN, size=10)
    )
    fig_del.update_layout(
        **{**PLOTLY_LAYOUT,
           'yaxis': {
               **PLOTLY_LAYOUT['yaxis'],
               'title': 'Delivery Rate',
               'tickformat': ',.0%',
               'range': [0.9, 1.05]
           },
           'xaxis': {**PLOTLY_LAYOUT['xaxis'], 'tickangle': 35}
        }
    )
    st.plotly_chart(fig_del, use_container_width=True)

st.markdown('<hr class="hive-divider">', unsafe_allow_html=True)

# ── Viewer Detail Table ───────────────────────────────────────────────────────

st.markdown('<div class="section-header">Viewer Detail</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Full Session Metrics</div>', unsafe_allow_html=True)

display_cols = [
    'client_id', 'session_duration_min', 'total_bufferings',
    'buffering_ratio', 'dominant_quality', 'quality_switches',
    'p2p_ratio', 'delivery_rate', 'qos_score', 'qos_label'
]

display_df = gold[display_cols].copy()
display_df.columns = [
    'Viewer ID', 'Duration (min)', 'Bufferings',
    'Buffering %', 'Dominant Quality', 'Quality Switches',
    'P2P Ratio', 'Delivery Rate', 'QoS Score', 'QoS Label'
]
display_df['Duration (min)'] = display_df['Duration (min)'].map('{:.1f}'.format)
display_df['Buffering %']    = display_df['Buffering %'].map('{:.1%}'.format)
display_df['P2P Ratio']      = display_df['P2P Ratio'].map('{:.1%}'.format)
display_df['Delivery Rate']  = display_df['Delivery Rate'].map('{:.1%}'.format)
display_df['QoS Score']      = display_df['QoS Score'].map('{:.3f}'.format)
display_df['Viewer ID']      = display_df['Viewer ID'].str[:16] + '...'

def style_qos_label(val):
    colors = {
        'green':  f'background-color: {QOS_GREEN}22; color: {QOS_GREEN}; font-weight: 600',
        'yellow': f'background-color: {QOS_YELLOW}22; color: {QOS_YELLOW}; font-weight: 600',
        'red':    f'background-color: {QOS_RED}22; color: {QOS_RED}; font-weight: 600',
    }
    return colors.get(val, '')

def style_table(df):
    styles = pd.DataFrame('', index=df.index, columns=df.columns)
    styles['QoS Label'] = df['QoS Label'].map(
        lambda v: style_qos_label(v)
    )
    return styles

styled = display_df.style\
    .apply(style_table, axis=None)\
    .set_properties(**{
        'background-color': HIVE_CARD,
        'color': HIVE_TEXT,
        'border-color': HIVE_BORDER,
        'font-size': '13px'
    })\
    .set_table_styles([{
        'selector': 'th',
        'props': [
            ('background-color', HIVE_BORDER),
            ('color', HIVE_MUTED),
            ('font-size', '11px'),
            ('text-transform', 'uppercase'),
            ('letter-spacing', '0.5px'),
            ('padding', '10px 12px'),
        ]
    }, {
        'selector': 'td',
        'props': [('padding', '10px 12px')]
    }])

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<hr class="hive-divider">
<div style="text-align: center; color: {HIVE_MUTED}; font-size: 12px; padding: 16px 0 32px;">
    Hive Streaming &nbsp;·&nbsp; QoS Analytics Pipeline &nbsp;·&nbsp;
    Built with Pandas · Pydantic · Streamlit · Plotly
</div>
""", unsafe_allow_html=True)
