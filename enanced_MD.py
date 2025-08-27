import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
import numpy as np
import json

# -------------------------------
# Ladder Color Scheme
# -------------------------------
LADDER_COLORS = {
    'navy': '#011D70',
    'orange': "#FF6E00",
    'yellow': '#F7CB15',
    'white': '#FFFFFF',
    'green': '#00BF2F',
    'purple': '#6F02CE',
    'light_gray': '#F8F9FA',
    'dark_gray': '#6C757D',
    'blue': '#3498DB',
    'light_blue': '#5DADE2'
}

# Feature color mapping
FEATURE_COLORS = {
    'spending': 'blue',
    'lady_ai': 'orange',
    'savings': 'green',
    'investment': 'purple'
}

# -------------------------------
# Load credentials
# -------------------------------
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def get_database_connection():
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None
# -------------------------------
# Get FFP data
# -------------------------------
@st.cache_data
def load_ffp_data():
    """Load FFP data from PostgreSQL"""
    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        ffp_df = pd.read_sql("SELECT * FROM financial_simulator_v2", engine)
        feedback_df = pd.read_sql("SELECT * FROM financial_simulator_reviews", engine)
        return ffp_df, feedback_df
    except Exception as e:
        st.error(f"Failed to load FFP data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def parse_ffp_metadata(metadata_str):
    """Parse FFP metadata JSON"""
    try:
        parsed = json.loads(metadata_str)
        if isinstance(parsed, dict) and "plan" in parsed:
            return {item['question']: item['answer'] for item in parsed['plan'] if isinstance(item, dict)}
    except Exception as e:
        return {}
    return {}

# -------------------------------
# Enhanced Styling Functions
# -------------------------------
def create_metric_card(title, value, help_text, color_key='navy', icon='📊', change_value=None, change_direction="up"):
    color = LADDER_COLORS[color_key]
    
    # Change indicator
    change_html = ""
    if change_value is not None:
        change_color = "#2ECC71" if change_direction == "up" else "#E74C3C"
        change_arrow = "↗️" if change_direction == "up" else "↘️"
        change_html = f'<div style="font-size: 12px; color: {change_color}; margin-top: 5px;">{change_arrow} {change_value}</div>'
    
    return f"""
    <div style="
        background: linear-gradient(135deg, {color}ee, {color});
        padding: 20px;
        border-radius: 15px;
        text-align: center;
        color: white;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        margin: 10px 0;
        border-left: 5px solid {LADDER_COLORS['yellow']};
        transition: transform 0.2s ease;
    ">
        <div style="font-size: 24px; margin-bottom: 5px;">{icon}</div>
        <div style="font-size: 14px; opacity: 0.9; margin-bottom: 8px;">{title}</div>
        <div style="font-size: 28px; font-weight: bold; margin-bottom: 5px;">{value}</div>
        <div style="font-size: 11px; opacity: 0.8;">{help_text}</div>
        {change_html}
    </div>
    """

def create_insight_card(title, insight, recommendation, icon="💡"):
    return f"""
    <div style="
        background: linear-gradient(135deg, {LADDER_COLORS['white']}, {LADDER_COLORS['light_gray']});
        padding: 20px;
        border-radius: 15px;
        margin: 15px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border-left: 5px solid {LADDER_COLORS['orange']};
    ">
        <div style="display: flex; align-items: center; margin-bottom: 10px;">
            <span style="font-size: 24px; margin-right: 10px;">{icon}</span>
            <h4 style="color: {LADDER_COLORS['navy']}; margin: 0;">{title}</h4>
        </div>
        <p style="color: {LADDER_COLORS['dark_gray']}; margin-bottom: 10px; font-size: 14px;">{insight}</p>
        <p style="color: {LADDER_COLORS['orange']}; margin: 0; font-weight: bold; font-size: 13px;">💡 Recommendation: {recommendation}</p>
    </div>
    """

def apply_custom_css():
    st.markdown(f"""
    <style>
    .main {{
        background: linear-gradient(135deg, {LADDER_COLORS['light_gray']}, {LADDER_COLORS['white']});
    }}
    
    .sidebar .sidebar-content {{
        background: linear-gradient(180deg, {LADDER_COLORS['navy']}, {LADDER_COLORS['purple']});
        color: white;
    }}
    
    .metric-container:hover {{
        transform: translateY(-2px);
    }}
    
    h1, h2, h3 {{
        color: {LADDER_COLORS['navy']};
    }}
    
    .feature-section {{
        background: white;
        padding: 20px;
        border-radius: 15px;
        margin: 15px 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border-left: 4px solid {LADDER_COLORS['orange']};
    }}
    
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        background-color: {LADDER_COLORS['light_gray']};
        border-radius: 10px;
        color: {LADDER_COLORS['navy']};
        font-weight: bold;
    }}
    
    .stTabs [aria-selected="true"] {{
        background: linear-gradient(90deg, {LADDER_COLORS['orange']}, {LADDER_COLORS['yellow']});
        color: white;
    }}
    </style>
    """, unsafe_allow_html=True)

# -------------------------------
# Enhanced Query Functions
# -------------------------------
@st.cache_data(ttl=300)
def fetch_comprehensive_metrics(start_date, end_date):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    WITH users_filtered AS (
        SELECT id::TEXT AS user_id, DATE(created_at) AS signup_date
        FROM users
        WHERE DATE(created_at) BETWEEN %s AND %s
          AND restricted = false
    ),
    
    all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
        UNION
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
    ),
    
    user_first_activity AS (
        SELECT user_id, MIN(activity_date) AS first_activity_date
        FROM all_feature_usage
        GROUP BY user_id
    ),
    
    first_time_users AS (
        SELECT uf.user_id
        FROM users_filtered uf
        JOIN user_first_activity ufa ON uf.user_id = ufa.user_id
        WHERE ufa.first_activity_date >= uf.signup_date
    ),
    
    recurring_users AS (
        SELECT user_id
        FROM all_feature_usage
        GROUP BY user_id
        HAVING COUNT(DISTINCT activity_date) > 1
    )
    
    SELECT
        -- Overall metrics
        (SELECT COUNT(*) FROM users_filtered) AS total_signups,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage) AS total_active_users,
        (SELECT COUNT(*) FROM first_time_users) AS first_time_users,
        (SELECT COUNT(*) FROM recurring_users) AS recurring_users,
        
        -- Feature-specific active users
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'spending') AS spending_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'savings') AS savings_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'investment') AS investment_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'lady_ai') AS lady_ai_users;
    """

    params = [start_date, end_date] * 6
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@st.cache_data(ttl=300)
def fetch_feature_specific_metrics(start_date, end_date, feature):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    feature_queries = {
        'spending': """
            SELECT user_id::TEXT, DATE(created_at) AS activity_date
            FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT user_id::TEXT, DATE(created_at)
            FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
        """,
        'savings': """
            SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
        """,
        'investment': """
            SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
            FROM transactions t
            JOIN investment_plans ip ON ip.id = t.investment_plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
        """,
        'lady_ai': """
            SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
            FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
        """
    }

    if feature not in feature_queries:
        return pd.DataFrame()

    base_query = feature_queries[feature]
    
    query = f"""
    WITH users_filtered AS (
        SELECT id::TEXT AS user_id, DATE(created_at) AS signup_date
        FROM users
        WHERE DATE(created_at) BETWEEN %s AND %s
          AND restricted = false
    ),
    
    feature_usage AS (
        {base_query}
    ),
    
    user_first_activity AS (
        SELECT user_id, MIN(activity_date) AS first_activity_date
        FROM feature_usage
        GROUP BY user_id
    ),
    
    first_time_users AS (
        SELECT uf.user_id
        FROM users_filtered uf
        JOIN user_first_activity ufa ON uf.user_id = ufa.user_id
        WHERE ufa.first_activity_date >= uf.signup_date
    ),
    
    recurring_users AS (
        SELECT user_id
        FROM feature_usage
        GROUP BY user_id
        HAVING COUNT(DISTINCT activity_date) > 1
    ),
    
    dau AS (
        SELECT activity_date, COUNT(DISTINCT user_id) AS dau
        FROM feature_usage
        GROUP BY activity_date
    ),
    
    wau AS (
        SELECT DATE_TRUNC('week', activity_date)::DATE AS week, COUNT(DISTINCT user_id) AS wau
        FROM feature_usage
        GROUP BY 1
    ),
    
    mau AS (
        SELECT DATE_TRUNC('month', activity_date)::DATE AS month, COUNT(DISTINCT user_id) AS mau
        FROM feature_usage
        GROUP BY 1
    )
    
    SELECT
        (SELECT COUNT(DISTINCT user_id) FROM feature_usage) AS total_active_users,
        (SELECT COUNT(*) FROM first_time_users) AS first_time_users,
        (SELECT COUNT(*) FROM recurring_users) AS recurring_users,
        (SELECT AVG(dau) FROM dau) AS avg_dau,
        (SELECT AVG(wau) FROM wau) AS avg_wau,
        (SELECT AVG(mau) FROM mau) AS avg_mau;
    """

    params_count = 2 if feature in ['savings', 'investment', 'lady_ai'] else 4
    params = [start_date, end_date] + [start_date, end_date] * (params_count // 2)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@st.cache_data(ttl=300)
def fetch_retention_metrics(start_date, end_date, feature=None):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    if feature:
        feature_queries = {
            'spending': """
                SELECT user_id::TEXT, DATE(created_at) AS activity_date
                FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
                UNION
                SELECT user_id::TEXT, DATE(created_at)
                FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            """,
            'savings': """
                SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            'investment': """
                SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN investment_plans ip ON ip.id = t.investment_plan_id
                WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            'lady_ai': """
                SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
                FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
            """
        }
        activity_query = feature_queries[feature]
    else:
        activity_query = """
            SELECT user_id::TEXT, DATE(created_at) AS activity_date
            FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT user_id::TEXT, DATE(created_at)
            FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT ip.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN investment_plans ip ON ip.id = t.investment_plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT p.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT "user"::TEXT, DATE(created_at)
            FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
        """

    query = f"""
    WITH signups AS (
        SELECT id::TEXT AS user_id, DATE(created_at) AS signup_date
        FROM users
        WHERE DATE(created_at) BETWEEN %s AND %s
          AND restricted = false
    ),
    activity AS (
        {activity_query}
    ),
    joined AS (
        SELECT s.user_id, s.signup_date, a.activity_date
        FROM signups s
        LEFT JOIN activity a ON s.user_id = a.user_id
    )
    SELECT
        COUNT(DISTINCT user_id) AS total_signups,
        
        -- Day 1 Retention
        COUNT(DISTINCT user_id) FILTER (WHERE activity_date = signup_date + INTERVAL '1 day')
        ::FLOAT / NULLIF(COUNT(DISTINCT user_id), 0) AS day1_retention,
        
        -- Week 1 Retention
        COUNT(DISTINCT user_id) FILTER (WHERE activity_date BETWEEN signup_date + INTERVAL '1 day' AND signup_date + INTERVAL '7 days')
        ::FLOAT / NULLIF(COUNT(DISTINCT user_id), 0) AS week1_retention,
        
        -- Month 1 Retention
        COUNT(DISTINCT user_id) FILTER (WHERE activity_date BETWEEN signup_date + INTERVAL '1 day' AND signup_date + INTERVAL '30 days')
        ::FLOAT / NULLIF(COUNT(DISTINCT user_id), 0) AS month1_retention
    FROM joined;
    """

    if feature:
        params_count = 2 if feature in ['savings', 'investment', 'lady_ai'] else 4
        params = [start_date, end_date] + [start_date, end_date] * (params_count // 2)
    else:
        params = [start_date, end_date] * 6

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@st.cache_data(ttl=300)
def fetch_trend_data(start_date, end_date, feature=None):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if feature:
        feature_queries = {
            'spending': """
                SELECT user_id::TEXT, DATE(created_at) AS activity_date
                FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
                UNION
                SELECT user_id::TEXT, DATE(created_at)
                FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            """,
            'savings': """
                SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            'investment': """
                SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN investment_plans ip ON ip.id = t.investment_plan_id
                WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            'lady_ai': """
                SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
                FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
            """
        }
        base_query = feature_queries[feature]
    else:
        base_query = """
            SELECT user_id::TEXT, DATE(created_at) AS activity_date
            FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT user_id::TEXT, DATE(created_at)
            FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT ip.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN investment_plans ip ON ip.id = t.investment_plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT p.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT "user"::TEXT, DATE(created_at)
            FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
        """

    dau_query = f"""
        SELECT activity_date, COUNT(DISTINCT user_id) AS dau
        FROM ({base_query}) fu
        GROUP BY activity_date
        ORDER BY activity_date;
    """

    wau_query = f"""
        SELECT DATE_TRUNC('week', activity_date)::DATE AS week, COUNT(DISTINCT user_id) AS wau
        FROM ({base_query}) fu
        GROUP BY 1
        ORDER BY 1;
    """

    mau_query = f"""
        SELECT DATE_TRUNC('month', activity_date)::DATE AS month, COUNT(DISTINCT user_id) AS mau
        FROM ({base_query}) fu
        GROUP BY 1
        ORDER BY 1;
    """

    if feature:
        params_count = 2 if feature in ['savings', 'investment', 'lady_ai'] else 4
        params = [start_date, end_date] * (params_count // 2)
    else:
        params = [start_date, end_date] * 5

    dau_df = pd.read_sql_query(dau_query, conn, params=params)
    wau_df = pd.read_sql_query(wau_query, conn, params=params)
    mau_df = pd.read_sql_query(mau_query, conn, params=params)
    conn.close()

    return dau_df, wau_df, mau_df

def generate_insights(metrics_df, retention_df, feature=None):
    """Generate actionable insights based on the data"""
    insights = []
    
    if not metrics_df.empty:
        # Engagement insights
        total_signups = metrics_df['total_signups'].iloc[0] if 'total_signups' in metrics_df.columns else 0
        total_active = metrics_df['total_active_users'].iloc[0] if 'total_active_users' in metrics_df.columns else 0
        
        if total_signups > 0:
            activation_rate = (total_active / total_signups) * 100
            
            if activation_rate < 30:
                insights.append({
                    'title': 'Low Activation Rate Alert',
                    'insight': f'Only {activation_rate:.1f}% of signups become active users.',
                    'recommendation': 'Improve onboarding flow and feature discovery.',
                    'icon': '⚠️'
                })
            elif activation_rate > 70:
                insights.append({
                    'title': 'Excellent Activation Rate',
                    'insight': f'{activation_rate:.1f}% activation rate shows strong product-market fit.',
                    'recommendation': 'Scale marketing efforts and maintain current onboarding quality.',
                    'icon': '🎉'
                })
    
    if not retention_df.empty and 'day1_retention' in retention_df.columns:
        day1_retention = retention_df['day1_retention'].iloc[0] * 100 if retention_df['day1_retention'].iloc[0] else 0
        week1_retention = retention_df['week1_retention'].iloc[0] * 100 if retention_df['week1_retention'].iloc[0] else 0
        
        if day1_retention < 20:
            insights.append({
                'title': 'Day 1 Retention Needs Attention',
                'insight': f'Only {day1_retention:.1f}% of users return the next day.',
                'recommendation': 'Implement push notifications and email sequences for new users.',
                'icon': '📱'
            })
        
        if week1_retention > 50:
            insights.append({
                'title': 'Strong Week 1 Retention',
                'insight': f'{week1_retention:.1f}% of users return within a week.',
                'recommendation': 'Focus on converting these engaged users to power users.',
                'icon': '💪'
            })
    
    return insights

# -------------------------------
# Streamlit App Configuration
# -------------------------------
st.set_page_config(
    page_title="📊 Ladder Marketing Analytics", 
    layout="wide",
    initial_sidebar_state="expanded"
)

apply_custom_css()

# Header
st.markdown(f"""
<div style="text-align: center; padding: 20px; background: linear-gradient(90deg, {LADDER_COLORS['navy']}, {LADDER_COLORS['purple']}); border-radius: 15px; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0; font-size: 2.5rem;">📊 Ladder Marketing Analytics Hub</h1>
    <p style="color: white; opacity: 0.9; margin: 10px 0 0 0;">Comprehensive growth, retention, and feature engagement analytics</p>
</div>
""", unsafe_allow_html=True)

# -------------------------------
# Sidebar Configuration
# -------------------------------
st.sidebar.markdown(f"""
<div style="background: linear-gradient(180deg, {LADDER_COLORS['orange']}, {LADDER_COLORS['yellow']}); 
            padding: 15px; border-radius: 10px; margin-bottom: 20px; text-align: center;">
    <h3 style="color: white; margin: 0;">🎯 Analytics Control Center</h3>
</div>
""", unsafe_allow_html=True)

# Date Range Selection
today = datetime.today().date()
options = {
    "Last 7 Days": (today - timedelta(days=7), today),
    "Last 30 Days": (today - timedelta(days=30), today),
    "This Month": (today.replace(day=1), today),
    "Custom": None
}

range_choice = st.sidebar.selectbox("📅 Date Range", options.keys(), index=1)

if range_choice == "Custom":
    start_date = st.sidebar.date_input("Start", today - timedelta(days=30))
    end_date = st.sidebar.date_input("End", today)
else:
    start_date, end_date = options[range_choice]

# -------------------------------
# Fetch Absolute Metrics
# -------------------------------
conn = get_database_connection()
absolute_metrics = {}
if conn:
    absolute_query = """
    WITH feature_usage AS (
        SELECT user_id::TEXT, DATE(created_at) AS activity_date
        FROM budgets WHERE DATE(created_at) >= '2024-06-24'
        UNION
        SELECT user_id::TEXT, DATE(created_at)
        FROM manual_and_external_transactions WHERE DATE(created_at) >= '2024-06-24'
        UNION
        SELECT ip.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND DATE(t.updated_at) >= '2024-06-24'
        UNION
        SELECT p.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND DATE(t.updated_at) >= '2024-06-24'
        UNION
        SELECT "user"::TEXT, DATE(created_at)
        FROM slack_message_dump WHERE DATE(created_at) >= '2024-06-24'
    )
    SELECT 
        (SELECT COUNT(*) FROM users WHERE DATE(created_at) >= '2024-06-24') AS absolute_total_signups,
        (SELECT COUNT(DISTINCT user_id) FROM feature_usage) AS absolute_total_active_users;
    """
    abs_df = pd.read_sql_query(absolute_query, conn)
    if not abs_df.empty:
        absolute_metrics = abs_df.iloc[0].to_dict()
    conn.close()

# Fetch comprehensive metrics
comprehensive_df = fetch_comprehensive_metrics(start_date, end_date)

# -------------------------------
# Main Dashboard - Overview Tab System
# -------------------------------
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Overview", "💰 Spending Analytics", "🤖 Lady AI Analytics", "🏦 Savings Analytics", "📈 Investment Analytics", "📋 FFP Engagement"])

with tab1:
    st.markdown('<div class="feature-section">', unsafe_allow_html=True)
    st.subheader("🎯 Key Performance Indicators")
    
    # Absolute Metrics Row
    if absolute_metrics:
        col_abs1, col_abs2 = st.columns(2)
        with col_abs1:
            st.markdown(
                create_metric_card(
                    "Absolute Total Signups", 
                    f"{absolute_metrics['absolute_total_signups']:,}",
                    "Total signups since June 24, 2024",
                    'blue',
                    '🚀'
                ),
                unsafe_allow_html=True
            )
        with col_abs2:
            st.markdown(
                        create_metric_card(
                            "Absolute Active Users",
                            f"{absolute_metrics['absolute_total_active_users']:,}",
                            "Total active users since June 24, 2024",
                            'blue',
                            '⚡'
                        ),
                        unsafe_allow_html=True
                    )

        st.subheader("📈 Comprehensive Metrics Overview")
         # Period-Specific Metrics
        if not comprehensive_df.empty:
            # Row 1: Core Metrics
            col1, col2, col3, col4 = st.columns(4)
                
            with col1:
                st.markdown(
                    create_metric_card(
                        "Total Signups",
                        f"{comprehensive_df['total_signups'][0]:,}",
                        f"New signups from {start_date} to {end_date}",
                        'navy',
                        '👥'
                    ),
                    unsafe_allow_html=True
                )
                
            with col2:
                st.markdown(
                    create_metric_card(
                        "Total Active Users",
                        f"{comprehensive_df['total_active_users'][0]:,}",
                        f"Users active from {start_date} to {end_date}",
                        'navy',
                        '🎯'
                    ),
                    unsafe_allow_html=True
                )

            with col3:
                st.markdown(
                    create_metric_card(
                        "First Time Users",
                        f"{comprehensive_df['first_time_users'][0]:,}",
                        "Registered users who used at least one feature",
                        'navy',
                            '🌟'
                    ),
                    unsafe_allow_html=True
                )
            with col4:
                st.markdown(
                    create_metric_card(
                        "Recurring Users",
                        f"{comprehensive_df['recurring_users'][0]:,}",
                        "Users with multiple active days",
                        'navy',
                        '🔄'
                    ),
                    unsafe_allow_html=True
                )
    
        # # Core Metrics Row
        # col1, col2 = st.columns(2)
        # with col1:
        #     st.markdown(
        #         create_metric_card(
        #             "First Time Users",
        #             f"{comprehensive_df['first_time_users'][0]:,}",
        #             "Registered users who used at least one feature",
        #             'blue',
        #             '🌟'
        #         ),
        #         unsafe_allow_html=True
        #     )
        
        # with col2:
        #     st.markdown(
        #         create_metric_card(
        #             "Recurring Users",
        #             f"{comprehensive_df['recurring_users'][0]:,}",
        #             "Users with multiple active days",
        #             'blue',
        #             '🔄'
        #         ),
        #         unsafe_allow_html=True
        #     )
        
        # Row 2: Feature-Specific Metrics with Color Coding
        st.subheader("🎪 Feature Engagement")
        col3, col4, col5, col6 = st.columns(4)
        
        with col3:
            st.markdown(
                create_metric_card(
                    "Spending Users",
                    f"{comprehensive_df['spending_users'][0]:,}",
                    "Users active in spending features",
                    'blue',  # Blue for spending
                    '💰'
                ),
                unsafe_allow_html=True
            )
        
        with col4:
            st.markdown(
                create_metric_card(
                    "Lady AI Users",
                    f"{comprehensive_df['lady_ai_users'][0]:,}",
                    "Users engaging with Lady AI",
                    'orange',  # Orange for Lady AI
                    '🤖'
                ),
                unsafe_allow_html=True
            )
        
        with col5:
            st.markdown(
                create_metric_card(
                    "Savings Users",
                    f"{comprehensive_df['savings_users'][0]:,}",
                    "Users active in savings",
                    'green',
                    '🏦'
                ),
                unsafe_allow_html=True
            )
        
        with col6:
            st.markdown(
                create_metric_card(
                    "Investment Users",
                    f"{comprehensive_df['investment_users'][0]:,}",
                    "Users active in investments",
                    'purple',
                    '📈'
                ),
                unsafe_allow_html=True
            )
    
    # Generate and display insights
    retention_df = fetch_retention_metrics(start_date, end_date)
    insights = generate_insights(comprehensive_df, retention_df)
    
    if insights:
        st.subheader("💡 Retention Insights")
        for insight in insights:
            st.markdown(
                create_insight_card(
                    insight['title'],
                    insight['insight'],
                    insight['recommendation'],
                    insight['icon']
                ),
                unsafe_allow_html=True
            )

    # Additional Analytics Section
    st.markdown('<div class="feature-section">', unsafe_allow_html=True)
    st.subheader("📊 Cross-Feature Comparison")
    
    if not comprehensive_df.empty:
        # Create comparison chart
        feature_data = pd.DataFrame({
            'Feature': ['Spending', 'Lady AI', 'Savings', 'Investment'],
            'Active Users': [
                comprehensive_df['spending_users'][0],
                comprehensive_df['lady_ai_users'][0],
                comprehensive_df['savings_users'][0],
                comprehensive_df['investment_users'][0]
            ],
            'Colors': [LADDER_COLORS['blue'], LADDER_COLORS['orange'], LADDER_COLORS['green'], LADDER_COLORS['purple']]
        })
        
        fig_comparison = px.bar(
            feature_data,
            x='Feature',
            y='Active Users',
            title="Feature Adoption Comparison",
            color='Feature',
            color_discrete_sequence=feature_data['Colors']
        )
        fig_comparison.update_layout(
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_comparison, use_container_width=True)
        
        # Feature penetration analysis
        total_active = comprehensive_df['total_active_users'][0]
        if total_active > 0:
            penetration_data = pd.DataFrame({
                'Feature': ['Spending', 'Lady AI', 'Savings', 'Investment'],
                'Penetration %': [
                    (comprehensive_df['spending_users'][0] / total_active) * 100,
                    (comprehensive_df['lady_ai_users'][0] / total_active) * 100,
                    (comprehensive_df['savings_users'][0] / total_active) * 100,
                    (comprehensive_df['investment_users'][0] / total_active) * 100
                ]
            })
            
            fig_penetration = px.pie(
                penetration_data,
                values='Penetration %',
                names='Feature',
                title="Feature Penetration Among Active Users",
                color_discrete_sequence=[LADDER_COLORS['blue'], LADDER_COLORS['orange'], LADDER_COLORS['green'], LADDER_COLORS['purple']]
            )
            fig_penetration.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_penetration, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
# Feature-specific tabs
for tab, feature, feature_name in [(tab2, 'spending', 'Spending'), (tab3, 'lady_ai', 'Lady AI'), (tab4, 'savings', 'Savings'), (tab5, 'investment', 'Investment')]:
    with tab:
        st.markdown('<div class="feature-section">', unsafe_allow_html=True)
        st.subheader(f"{feature_name} Deep Dive Analytics")
        
        # Fetch feature-specific metrics
        feature_metrics = fetch_feature_specific_metrics(start_date, end_date, feature)
        feature_retention = fetch_retention_metrics(start_date, end_date, feature)
        
        if not feature_metrics.empty:
            # Calculate stickiness
            avg_dau = feature_metrics['avg_dau'][0] if not pd.isna(feature_metrics['avg_dau'][0]) else 0
            avg_wau = feature_metrics['avg_wau'][0] if not pd.isna(feature_metrics['avg_wau'][0]) else 0
            avg_mau = feature_metrics['avg_mau'][0] if not pd.isna(feature_metrics['avg_mau'][0]) else 0
            stickiness_ratio = avg_dau / avg_mau if avg_mau > 0 else 0
            
            # Get feature color
            feature_color = FEATURE_COLORS.get(feature, 'navy')
            
            # Core metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(
                    create_metric_card(
                        f"{feature_name} Active Users",
                        f"{feature_metrics['total_active_users'][0]:,}",
                        f"Total users active in {feature_name.lower()}",
                        feature_color,
                        '👥'
                    ),
                    unsafe_allow_html=True
                )
            
            with col2:
                st.markdown(
                    create_metric_card(
                        "First Time Users",
                        f"{feature_metrics['first_time_users'][0]:,}",
                        f"New users to {feature_name.lower()}",
                        feature_color,
                        '🌟'
                    ),
                    unsafe_allow_html=True
                )
            
            with col3:
                st.markdown(
                    create_metric_card(
                        "Recurring Users",
                        f"{feature_metrics['recurring_users'][0]:,}",
                        f"Multi-day {feature_name.lower()} users",
                        feature_color,
                        '🔄'
                    ),
                    unsafe_allow_html=True
                )
            
            with col4:
                st.markdown(
                    create_metric_card(
                        "Stickiness Ratio",
                        f"{stickiness_ratio:.2f}",
                        "DAU/MAU engagement ratio",
                        feature_color,
                        '🎯'
                    ),
                    unsafe_allow_html=True
                )
            
            # Engagement metrics row
            st.subheader(f"📊 {feature_name} Engagement Metrics")
            col5, col6, col7 = st.columns(3)
            
            with col5:
                st.markdown(
                    create_metric_card(
                        "Average DAU",
                        f"{avg_dau:,.0f}",
                        f"Daily active {feature_name.lower()} users",
                        feature_color,
                        '📅'
                    ),
                    unsafe_allow_html=True
                )
            
            with col6:
                st.markdown(
                    create_metric_card(
                        "Average WAU",
                        f"{avg_wau:,.0f}",
                        f"Weekly active {feature_name.lower()} users",
                        feature_color,
                        '📆'
                    ),
                    unsafe_allow_html=True
                )
            
            with col7:
                st.markdown(
                    create_metric_card(
                        "Average MAU",
                        f"{avg_mau:,.0f}",
                        f"Monthly active {feature_name.lower()} users",
                        feature_color,
                        '🗓️'
                    ),
                    unsafe_allow_html=True
                )
            
            # Retention metrics
            if not feature_retention.empty:
                st.subheader(f"🔄 {feature_name} Retention Analysis")
                col8, col9, col10 = st.columns(3)
                
                day1_ret = feature_retention['day1_retention'][0] * 100 if feature_retention['day1_retention'][0] else 0
                week1_ret = feature_retention['week1_retention'][0] * 100 if feature_retention['week1_retention'][0] else 0
                month1_ret = feature_retention['month1_retention'][0] * 100 if feature_retention['month1_retention'][0] else 0
                
                with col8:
                    st.markdown(
                        create_metric_card(
                            "Day 1 Retention",
                            f"{day1_ret:.1f}%",
                            "Users returning next day",
                            feature_color,
                            '📱'
                        ),
                        unsafe_allow_html=True
                    )
                
                with col9:
                    st.markdown(
                        create_metric_card(
                            "Week 1 Retention",
                            f"{week1_ret:.1f}%",
                            "Users returning within a week",
                            feature_color,
                            '🗓️'
                        ),
                        unsafe_allow_html=True
                    )
                
                with col10:
                    st.markdown(
                        create_metric_card(
                            "Month 1 Retention",
                            f"{month1_ret:.1f}%",
                            "Users returning within a month",
                            feature_color,
                            '📊'
                        ),
                        unsafe_allow_html=True
                    )
            
            # Trend Charts
            st.subheader(f"📈 {feature_name} Usage Trends")
            dau_df, wau_df, mau_df = fetch_trend_data(start_date, end_date, feature)
            
            if not dau_df.empty:
                # Daily trend
                fig_dau = px.line(
                    dau_df, 
                    x='activity_date', 
                    y='dau',
                    title=f'{feature_name} Daily Active Users',
                    color_discrete_sequence=[LADDER_COLORS[feature_color]]
                )
                fig_dau.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Daily Active Users",
                    hovermode='x unified',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_dau, use_container_width=True)
            
            if not wau_df.empty:
                # Weekly trend
                fig_wau = px.bar(
                    wau_df, 
                    x='week', 
                    y='wau',
                    title=f'{feature_name} Weekly Active Users',
                    color_discrete_sequence=[LADDER_COLORS[feature_color]]
                )
                fig_wau.update_layout(
                    xaxis_title="Week",
                    yaxis_title="Weekly Active Users",
                    showlegend=False,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_wau, use_container_width=True)
            
            if not mau_df.empty and len(mau_df) > 1:
                # Monthly trend
                fig_mau = px.line(
                    mau_df, 
                    x='month', 
                    y='mau',
                    title=f'{feature_name} Monthly Active Users',
                    color_discrete_sequence=[LADDER_COLORS[feature_color]],
                    markers=True
                )
                fig_mau.update_layout(
                    xaxis_title="Month",
                    yaxis_title="Monthly Active Users",
                    hovermode='x unified',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_mau, use_container_width=True)
        
        # Feature-specific insights
        feature_insights = generate_insights(feature_metrics, feature_retention, feature)
        if feature_insights:
            st.subheader(f"💡 {feature_name} Insights")
            for insight in feature_insights:
                st.markdown(
                    create_insight_card(
                        insight['title'],
                        insight['insight'],
                        insight['recommendation'],
                        insight['icon']
                    ),
                    unsafe_allow_html=True
                )
        
        st.markdown('</div>', unsafe_allow_html=True)

# FFP Engagement Dashboard Tab
with tab6:
    st.markdown('<div class="feature-section">', unsafe_allow_html=True)
    st.subheader("📋 Free Financial Plan (FFP) Engagement Dashboard")
    st.markdown("Gain actionable insights into how users interact with the Free Financial Plan experience.")
    
    # Load FFP data
    ffp_df, feedback_df = load_ffp_data()
    
    if not ffp_df.empty:
        # Convert dates
        ffp_df['created_at'] = pd.to_datetime(ffp_df['created_at'])
        if not feedback_df.empty:
            feedback_df['created_at'] = pd.to_datetime(feedback_df['created_at'])
        
        # Apply date filter
        filtered_ffp = ffp_df[(ffp_df['created_at'].dt.date >= start_date) & (ffp_df['created_at'].dt.date <= end_date)]
        filtered_feedback = feedback_df[(feedback_df['created_at'].dt.date >= start_date) & (feedback_df['created_at'].dt.date <= end_date)] if not feedback_df.empty else feedback_df
        
        # FFP Metrics
        col1, col2 = st.columns(2)
        with col1:
            parsed_metadata = filtered_ffp['metadata'].apply(parse_ffp_metadata)
            total_completed = parsed_metadata.apply(lambda x: len([v for v in x.values() if v not in (None, '', [], {})]))
            completed_surveys = (total_completed == total_completed.max()).sum()
            st.markdown(
                create_metric_card(
                    "✅ Completed Surveys",
                    f"{completed_surveys:,}",
                    f"All questions completed ({start_date} to {end_date})",
                    'navy',
                    '📋'
                ),
                unsafe_allow_html=True
            )
        
        with col2:
            st.markdown(
                create_metric_card(
                    "📥 Total Submissions",
                    f"{len(filtered_ffp):,}",
                    f"Total FFP submissions ({start_date} to {end_date})",
                    'navy',
                    '📊'
                ),
                unsafe_allow_html=True
            )
        
        # Engagement Trends
        st.subheader("📊 Engagement Over Time and User Feedback")
        trend_df = filtered_ffp.groupby(filtered_ffp['created_at'].dt.date).size().reset_index(name='Submissions')
        trend_df = trend_df.rename(columns={"created_at": "Date"})
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("### Daily Submissions")
            st.line_chart(trend_df.set_index("Date"))
        
        if not filtered_feedback.empty:
            reaction_counts = filtered_feedback['reaction'].value_counts()
            with col2:
                st.subheader("💬 User Reactions")
                st.bar_chart(reaction_counts)
            
            # User Comments
            st.subheader("💭 User Feedback")
            for _, row in filtered_feedback.iterrows():
                st.markdown(f"- **{row['reaction'].capitalize()}** — {row['comment']} *(on {row['created_at'].date()})*")
        else:
            st.info("No feedback data available for the selected period.")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Footer with summary stats
st.markdown(f"""
<div style="text-align: center; padding: 20px; margin-top: 30px; 
            background: linear-gradient(90deg, {LADDER_COLORS['navy']}, {LADDER_COLORS['purple']}); 
            border-radius: 15px;">
    <h4 style="color: white; margin-bottom: 15px;">📊 Analytics Summary</h4>
    <div style="display: flex; justify-content: space-around; flex-wrap: wrap;">
        <div style="color: white; text-align: center; margin: 5px;">
            <div style="font-size: 18px; font-weight: bold;">Period Analyzed</div>
            <div style="opacity: 0.8;">{start_date} to {end_date}</div>
        </div>
        <div style="color: white; text-align: center; margin: 5px;">
            <div style="font-size: 18px; font-weight: bold;">Last Updated</div>
            <div style="opacity: 0.8;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
        </div>
        <div style="color: white; text-align: center; margin: 5px;">
            <div style="font-size: 18px; font-weight: bold;">Data Source</div>
            <div style="opacity: 0.8;">Ladder Production Database</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


    # Period-Specific Metrics
if not comprehensive_df.empty:
    # Row 1: Core Metrics
    col1, col2, col3, col4 = st.columns(4)
        
    with col1:
        st.markdown(
            create_metric_card(
                "Total Signups",
                f"{comprehensive_df['total_signups'][0]:,}",
                f"New signups from {start_date} to {end_date}",
                'navy',
                '👥'
            ),
            unsafe_allow_html=True
        )
        
    with col2:
        st.markdown(
            create_metric_card(
                "Total Active Users",
                f"{comprehensive_df['total_active_users'][0]:,}",
                f"Users active from {start_date} to {end_date}",
                'green',
                '🎯'
            ),
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            create_metric_card(
                "First Time Users",
                f"{comprehensive_df['first_time_users'][0]:,}",
                "Registered users who used at least one feature",
                'purple',
                    '🌟'
            ),
            unsafe_allow_html=True
        )
    with col4:
        st.markdown(
            create_metric_card(
                "Recurring Users",
                f"{comprehensive_df['recurring_users'][0]:,}",
                "Users with multiple active days",
                'yellow',
                '🔄'
            ),
            unsafe_allow_html=True
        )
        




