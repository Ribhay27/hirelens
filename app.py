"""
HireLens Dashboard
Streamlit app for exploring job market intelligence.
Run with: streamlit run src/dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import io

from src.dashboard.queries import (
    get_kpis,
    get_role_distribution,
    get_seniority_distribution,
    get_top_skills,
    get_skill_by_role,
    get_top_hiring_companies,
    get_salary_by_role,
    get_location_distribution,
    get_postings_over_time,
    get_job_listings,
)
from src.database import check_connection

# ── Page Config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="HireLens – Job Market Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme / CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
}

/* KPI Cards */
.kpi-card {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 20px 24px;
    margin: 4px 0;
}
.kpi-value {
    font-size: 2.2rem;
    font-weight: 700;
    color: #38bdf8;
    line-height: 1.1;
}
.kpi-label {
    font-size: 0.8rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 4px;
}
.kpi-delta {
    font-size: 0.85rem;
    color: #4ade80;
    margin-top: 6px;
}

/* Section headers */
.section-header {
    font-size: 1.1rem;
    font-weight: 600;
    color: #e2e8f0;
    border-left: 3px solid #38bdf8;
    padding-left: 12px;
    margin: 8px 0 16px 0;
}

/* Skill badges */
.skill-badge {
    display: inline-block;
    background: #1e3a5f;
    color: #7dd3fc;
    border: 1px solid #2563eb;
    border-radius: 6px;
    padding: 2px 8px;
    font-size: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    margin: 2px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #0f172a;
    border-right: 1px solid #1e293b;
}

/* Main background */
.main { background: #0b1120; }

/* Remove streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

PALETTE = {
    "primary":   "#38bdf8",
    "secondary": "#818cf8",
    "success":   "#4ade80",
    "warning":   "#fb923c",
    "danger":    "#f87171",
    "bg":        "#0f172a",
    "surface":   "#1e293b",
    "border":    "#334155",
    "text":      "#e2e8f0",
    "muted":     "#94a3b8",
}

PLOTLY_TEMPLATE = dict(
    layout=dict(
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        font=dict(family="Space Grotesk", color="#e2e8f0"),
        xaxis=dict(gridcolor="#1e293b", linecolor="#334155"),
        yaxis=dict(gridcolor="#1e293b", linecolor="#334155"),
        legend=dict(bgcolor="#1e293b", bordercolor="#334155"),
        colorway=[
            "#38bdf8", "#818cf8", "#4ade80", "#fb923c",
            "#f87171", "#a78bfa", "#34d399", "#fbbf24",
        ],
    )
)

def _plotly_layout(fig, title="", height=350):
    fig.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        title=dict(text=title, font=dict(size=14, color="#94a3b8")),
        height=height,
        margin=dict(l=16, r=16, t=40, b=16),
    )
    return fig


def _fmt_num(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def _kpi(value, label, delta=""):
    delta_html = f'<div class="kpi-delta">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>
    """


# ── DB Check ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=10)
def _check_db():
    return check_connection()


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 HireLens")
    st.markdown("<small style='color:#64748b'>Job Market Intelligence</small>", unsafe_allow_html=True)
    st.divider()

    page = st.radio(
        "Navigate",
        ["📊 Overview", "🛠 Skills", "🏢 Companies", "💰 Salaries", "📋 Browse Jobs"],
        label_visibility="collapsed",
    )

    st.divider()

    # Role filter (global)
    @st.cache_data(ttl=60)
    def _get_roles():
        df = get_role_distribution()
        return ["All"] + list(df["role_category"].dropna())

    role_options = _get_roles()
    selected_role = st.selectbox("Filter by Role", role_options)

    st.divider()

    # Pipeline trigger
    st.markdown("**⚙ Pipeline**")
    if st.button("▶ Run Pipeline Now", use_container_width=True):
        with st.spinner("Running pipeline… this may take a few minutes."):
            try:
                from src.pipeline import HireLensPipeline
                pipeline = HireLensPipeline()
                run = pipeline.run()
                st.success(f"Done! {run.jobs_scraped} scraped, {run.jobs_processed} processed.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Pipeline error: {e}")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    db_ok = _check_db()
    status_color = "#4ade80" if db_ok else "#f87171"
    status_text  = "Connected" if db_ok else "Disconnected"
    st.markdown(
        f"<small style='color:{status_color}'>● DB: {status_text}</small>",
        unsafe_allow_html=True,
    )


# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="font-size:2rem; font-weight:700; color:#f1f5f9; margin:0;">
        HireLens
        <span style="font-size:1rem; font-weight:400; color:#64748b; margin-left:12px;">
            Job Market Intelligence
        </span>
    </h1>
</div>
""", unsafe_allow_html=True)

if not db_ok:
    st.error("⚠️ Cannot connect to PostgreSQL. Check your DATABASE_URL in .env and run `python -m src.database` to initialise.")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Overview
# ══════════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":

    @st.cache_data(ttl=120)
    def load_overview():
        return (
            get_kpis(),
            get_role_distribution(),
            get_seniority_distribution(selected_role),
            get_postings_over_time(30),
            get_location_distribution(),
        )

    kpis, role_df, seniority_df, trend_df, loc_df = load_overview()

    # KPI Row
    cols = st.columns(5)
    kpi_data = [
        (_fmt_num(kpis["total_jobs"]),       "Total Jobs",        ""),
        (_fmt_num(kpis["unique_companies"]),  "Companies Hiring",  ""),
        (f"{kpis['remote_pct']}%",           "Remote Roles",      ""),
        (_fmt_num(kpis["unique_skills"]),     "Unique Skills",     ""),
        (_fmt_num(kpis["processed_jobs"]),    "Analysed",          ""),
    ]
    for col, (val, label, delta) in zip(cols, kpi_data):
        col.markdown(_kpi(val, label, delta), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Row 2: Role distribution + Seniority
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown('<div class="section-header">Role Distribution</div>', unsafe_allow_html=True)
        if not role_df.empty:
            fig = px.bar(
                role_df.head(10),
                x="count", y="role_category",
                orientation="h",
                color="count",
                color_continuous_scale=["#1e3a5f", "#38bdf8"],
            )
            fig.update_coloraxes(showscale=False)
            fig.update_yaxis(categoryorder="total ascending")
            fig = _plotly_layout(fig, height=380)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet — run the pipeline to get started.")

    with col2:
        st.markdown('<div class="section-header">Seniority Breakdown</div>', unsafe_allow_html=True)
        if not seniority_df.empty:
            fig = px.pie(
                seniority_df,
                values="count", names="seniority",
                hole=0.55,
                color_discrete_sequence=["#38bdf8", "#818cf8", "#4ade80", "#fb923c", "#f87171", "#fbbf24"],
            )
            fig.update_traces(textposition="outside", textinfo="label+percent")
            fig = _plotly_layout(fig, height=380)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No seniority data yet.")

    # Row 3: Postings over time + Location
    col3, col4 = st.columns([3, 2])

    with col3:
        st.markdown('<div class="section-header">Postings Collected (Last 30 Days)</div>', unsafe_allow_html=True)
        if not trend_df.empty:
            fig = px.area(
                trend_df, x="date", y="count",
                line_shape="spline",
                color_discrete_sequence=["#38bdf8"],
            )
            fig.update_traces(fill="tozeroy", fillcolor="rgba(56,189,248,0.1)")
            fig = _plotly_layout(fig, height=280)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data yet.")

    with col4:
        st.markdown('<div class="section-header">Top Locations</div>', unsafe_allow_html=True)
        if not loc_df.empty:
            fig = px.bar(
                loc_df.head(8),
                x="count", y="location", orientation="h",
                color_discrete_sequence=["#818cf8"],
            )
            fig.update_yaxis(categoryorder="total ascending")
            fig = _plotly_layout(fig, height=280)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No location data.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Skills
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🛠 Skills":

    @st.cache_data(ttl=120)
    def load_skills(role):
        return (
            get_top_skills(category=role, top_n=25),
            get_top_skills(category="All", top_n=100),
            get_skill_by_role(),
        )

    top_skills_df, all_skills_df, skill_role_df = load_skills(selected_role)

    col_a, col_b = st.columns([2, 3])

    with col_a:
        st.markdown(f'<div class="section-header">Top 25 Skills — {selected_role}</div>', unsafe_allow_html=True)
        if not top_skills_df.empty:
            fig = px.bar(
                top_skills_df,
                x="count", y="skill",
                orientation="h",
                color="count",
                color_continuous_scale=["#1e3a5f", "#0ea5e9", "#38bdf8"],
            )
            fig.update_coloraxes(showscale=False)
            fig.update_yaxis(categoryorder="total ascending")
            fig = _plotly_layout(fig, height=560)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No skill data yet.")

    with col_b:
        st.markdown('<div class="section-header">Skill Word Cloud</div>', unsafe_allow_html=True)
        if not all_skills_df.empty:
            freq = dict(zip(all_skills_df["skill"], all_skills_df["count"]))
            wc = WordCloud(
                width=700, height=340,
                background_color="#0f172a",
                colormap="cool",
                prefer_horizontal=0.85,
                max_words=80,
            ).generate_from_frequencies(freq)
            fig_wc, ax = plt.subplots(figsize=(7, 3.4))
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            fig_wc.patch.set_facecolor("#0f172a")
            buf = io.BytesIO()
            fig_wc.savefig(buf, format="png", bbox_inches="tight", facecolor="#0f172a")
            buf.seek(0)
            st.image(buf, use_column_width=True)
            plt.close()
        else:
            st.info("No skill data for word cloud.")

        # Heatmap: Skills × Roles
        st.markdown('<div class="section-header">Skill Demand by Role (Heatmap)</div>', unsafe_allow_html=True)
        if not skill_role_df.empty:
            top_skills = skill_role_df.groupby("skill")["count"].sum().nlargest(15).index.tolist()
            heat_df = skill_role_df[skill_role_df["skill"].isin(top_skills)]
            pivot = heat_df.pivot_table(
                index="skill", columns="role_category", values="count", fill_value=0
            )
            fig = px.imshow(
                pivot,
                color_continuous_scale=["#0f172a", "#1e3a5f", "#0ea5e9", "#38bdf8"],
                aspect="auto",
            )
            fig = _plotly_layout(fig, height=380)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data for heatmap yet.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Companies
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🏢 Companies":

    @st.cache_data(ttl=120)
    def load_companies():
        return get_top_hiring_companies(top_n=20)

    companies_df = load_companies()

    st.markdown('<div class="section-header">Top Hiring Companies</div>', unsafe_allow_html=True)

    if not companies_df.empty:
        col1, col2 = st.columns([3, 2])

        with col1:
            fig = px.bar(
                companies_df.head(15),
                x="count", y="company",
                orientation="h",
                color="count",
                color_continuous_scale=["#1e3a5f", "#818cf8"],
            )
            fig.update_coloraxes(showscale=False)
            fig.update_yaxis(categoryorder="total ascending")
            fig = _plotly_layout(fig, height=480)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.treemap(
                companies_df.head(20),
                path=["company"],
                values="count",
                color="count",
                color_continuous_scale=["#1e293b", "#818cf8"],
            )
            fig.update_traces(textinfo="label+value")
            fig = _plotly_layout(fig, height=480)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No company data yet.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Salaries
# ══════════════════════════════════════════════════════════════════════════════

elif page == "💰 Salaries":

    @st.cache_data(ttl=120)
    def load_salaries():
        return get_salary_by_role()

    sal_df = load_salaries()

    st.markdown('<div class="section-header">Average Salary Range by Role</div>', unsafe_allow_html=True)

    if not sal_df.empty:
        fig = go.Figure()
        for _, row in sal_df.iterrows():
            fig.add_trace(go.Bar(
                name=row["role_category"],
                x=[row["role_category"]],
                y=[row["avg_max"] - row["avg_min"]],
                base=[row["avg_min"]],
                marker_color=PALETTE["primary"],
                opacity=0.8,
                text=f"${row['avg_min']:,.0f} – ${row['avg_max']:,.0f}",
                textposition="outside",
            ))
        fig.update_layout(
            **PLOTLY_TEMPLATE["layout"],
            barmode="stack",
            showlegend=False,
            yaxis_title="Annual Salary (USD)",
            height=440,
            margin=dict(l=16, r=16, t=40, b=80),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Salary Data Table</div>', unsafe_allow_html=True)
        sal_display = sal_df.copy()
        sal_display["avg_min"] = sal_display["avg_min"].apply(lambda x: f"${x:,.0f}")
        sal_display["avg_max"] = sal_display["avg_max"].apply(lambda x: f"${x:,.0f}")
        sal_display.columns = ["Role", "Avg Min Salary", "Avg Max Salary", "Jobs w/ Salary"]
        st.dataframe(sal_display, use_container_width=True, hide_index=True)
    else:
        st.info("No salary data available yet. Salary info is optional on Indeed listings — run more scrapes to accumulate data.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Browse Jobs
# ══════════════════════════════════════════════════════════════════════════════

elif page == "📋 Browse Jobs":

    st.markdown('<div class="section-header">Browse & Filter Job Listings</div>', unsafe_allow_html=True)

    filt_col1, filt_col2, filt_col3 = st.columns(3)

    with filt_col1:
        @st.cache_data(ttl=60)
        def _roles_list():
            df = get_role_distribution()
            return ["All"] + list(df["role_category"].dropna())
        f_role = st.selectbox("Role Category", _roles_list(), key="browse_role")

    with filt_col2:
        f_seniority = st.selectbox("Seniority", ["All", "Intern", "Junior", "Mid", "Senior", "Lead", "Manager"])

    with filt_col3:
        f_remote = st.selectbox("Work Type", ["All", "Remote Only", "On-site Only"])

    remote_filter = None
    if f_remote == "Remote Only":
        remote_filter = True
    elif f_remote == "On-site Only":
        remote_filter = False

    @st.cache_data(ttl=60)
    def load_listings(role, seniority, remote):
        return get_job_listings(
            role_category=role,
            seniority=seniority,
            is_remote=remote,
            limit=300,
        )

    listings_df = load_listings(f_role, f_seniority, remote_filter)

    st.markdown(f"<small style='color:#64748b'>{len(listings_df)} jobs found</small>", unsafe_allow_html=True)

    if not listings_df.empty:
        # Display cards for top 5
        st.markdown("**Latest Listings**")
        for _, row in listings_df.head(5).iterrows():
            remote_badge = "🌐 Remote" if row.get("is_remote") else f"📍 {row.get('location','')}"
            salary_str = ""
            if row.get("salary_min") and row.get("salary_max"):
                salary_str = f"💰 ${row['salary_min']:,.0f}–${row['salary_max']:,.0f}"

            skills_html = " ".join([
                f'<span class="skill-badge">{s}</span>'
                for s in (list(row.get("skills") or []) + list(row.get("tools") or []))[:8]
            ])

            st.markdown(f"""
            <div style="background:#1e293b; border:1px solid #334155; border-radius:10px;
                        padding:16px 20px; margin-bottom:10px;">
                <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                    <div>
                        <div style="font-size:1rem; font-weight:600; color:#f1f5f9;">{row['title']}</div>
                        <div style="color:#94a3b8; font-size:0.875rem; margin-top:2px;">
                            {row.get('company','Unknown')} &nbsp;·&nbsp; {remote_badge}
                            {'&nbsp;·&nbsp; ' + salary_str if salary_str else ''}
                        </div>
                    </div>
                    <div style="text-align:right;">
                        <span style="background:#0f172a; border:1px solid #38bdf8; color:#38bdf8;
                                     border-radius:6px; padding:3px 10px; font-size:0.75rem;">
                            {row.get('role_category','—')}
                        </span>
                        <br><span style="color:#64748b; font-size:0.75rem;">{row.get('seniority','')}</span>
                    </div>
                </div>
                <div style="margin-top:10px;">{skills_html}</div>
                {f'<div style="margin-top:8px;"><a href="{row["url"]}" target="_blank" style="color:#38bdf8; font-size:0.8rem;">View on Indeed →</a></div>' if row.get('url') else ''}
            </div>
            """, unsafe_allow_html=True)

        # Full table
        with st.expander("📊 View Full Table"):
            display_df = listings_df[[
                "title", "company", "location", "role_category",
                "seniority", "is_remote", "salary_min", "salary_max", "posted_date"
            ]].copy()
            display_df.columns = [
                "Title", "Company", "Location", "Role", "Seniority",
                "Remote", "Min Salary", "Max Salary", "Posted"
            ]
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No jobs match the current filters.")
