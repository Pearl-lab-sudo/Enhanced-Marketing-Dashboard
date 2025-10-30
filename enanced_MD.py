import os
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
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
    "navy": "#011D70",
    "orange": "#FF6E00",
    "yellow": "#F7CB15",
    "white": "#FFFFFF",
    "green": "#00BF2F",
    "purple": "#6F02CE",
    "light_gray": "#F8F9FA",
    "dark_gray": "#6C757D",
    "blue": "#5C9DD8",
    "light_blue": "#5DADE2",
    "red": "#991821",
    "azure": "#0161F3"
}

# Feature color mapping
FEATURE_COLORS = {
    "spending": "blue",
    "lady_ai": "orange",
    "savings": "green",
    "investment": "purple",
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

db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_database_connection():
    try:
        conn = create_engine(db_url).connect()
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
            return {
                item["question"]: item["answer"]
                for item in parsed["plan"]
                if isinstance(item, dict)
            }
    except Exception as e:
        return {}
    return {}


# -------------------------------
# Enhanced Styling Functions
# -------------------------------
def create_metric_card(
    title,
    value,
    help_text,
    color_key="navy",
    icon="📊",
    change_value=None,
    change_direction="up",
    alert_level=None,
    additional_insight=None,
):
    color = LADDER_COLORS[color_key]

    # Change indicator
    change_html = ""
    if change_value is not None:
        change_color = "#2ECC71" if change_direction == "up" else "#E74C3C"
        change_arrow = "↗️" if change_direction == "up" else "↘️"
        change_html = f'<div style="font-size: 12px; color: {change_color}; margin-top: 5px;">{change_arrow} {change_value}</div>'
    
    # Alert level indicator
    alert_html = ""
    if alert_level:
        alert_colors = {
            "high": "#E74C3C",    # Red for high attention needed
            "medium": "#F39C12",  # Orange for medium attention
            "low": "#2ECC71",     # Green for good performance
            "info": "#3498DB"     # Blue for informational
        }
        alert_icons = {
            "high": "🚨",
            "medium": "⚠️", 
            "low": "✅",
            "info": "ℹ️"
        }
        alert_color = alert_colors.get(alert_level, "#3498DB")
        alert_icon = alert_icons.get(alert_level, "ℹ️")
        alert_html = f'<div style="font-size: 14px; color: {alert_color}; margin-top: 3px;">{alert_icon}</div>'
    
    # Additional insight
    insight_html = ""
    if additional_insight:
        insight_html = f'<div style="font-size: 10px; opacity: 0.9; margin-top: 3px; font-style: italic;">💡 {additional_insight}</div>'

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
        {alert_html}
        {insight_html}
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
    st.markdown(
        f"""
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
    """,
        unsafe_allow_html=True,
    )


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
    WHERE restricted = false
    ),

    all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets
        UNION
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump
    ),

    -- Step 1: Each user’s earliest feature usage ever (across all time)
    first_ever_activity AS (
        SELECT user_id, MIN(activity_date) AS first_ever_activity_date
        FROM all_feature_usage
        GROUP BY user_id
    ),

    -- Step 2: Each user’s activity summary within the selected period
    user_activity_summary AS (
        SELECT 
            user_id,
            COUNT(DISTINCT activity_date) AS active_days_in_period,
            MIN(activity_date) AS first_activity_in_period,
            MAX(activity_date) AS last_activity_in_period
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
        GROUP BY user_id
    ),

    -- Step 3: Combine and classify
    user_classification AS (
        SELECT 
            uas.user_id,
            uf.signup_date,
            fea.first_ever_activity_date,
            uas.first_activity_in_period,
            uas.active_days_in_period,
            CASE
                WHEN uas.active_days_in_period = 1 THEN 'one_time_usage'
                WHEN uas.active_days_in_period >= 2 THEN 'recurring'
                ELSE 'inactive'
            END AS usage_type,
            CASE
                WHEN fea.first_ever_activity_date BETWEEN %s AND %s THEN TRUE
                ELSE FALSE
            END AS is_first_time_user
        FROM user_activity_summary uas
        JOIN users_filtered uf ON uf.user_id = uas.user_id
        LEFT JOIN first_ever_activity fea ON fea.user_id = uas.user_id
    ),

    -- Step 4: DAU, WAU, MAU metrics
    dau_metrics AS (
        SELECT AVG(daily_count) AS avg_dau
        FROM (
            SELECT activity_date, COUNT(DISTINCT user_id) AS daily_count
            FROM all_feature_usage
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY activity_date
        ) d
    ),
    wau_metrics AS (
        SELECT AVG(weekly_count) AS avg_wau
        FROM (
            SELECT DATE_TRUNC('week', activity_date)::DATE AS week, COUNT(DISTINCT user_id) AS weekly_count
            FROM all_feature_usage
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY 1
        ) w
    ),
    mau_metrics AS (
        SELECT AVG(monthly_count) AS avg_mau
        FROM (
            SELECT DATE_TRUNC('month', activity_date)::DATE AS month, COUNT(DISTINCT user_id) AS monthly_count
            FROM all_feature_usage
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY 1
        ) m
    )
    SELECT
    -- Core user metrics
        (SELECT COUNT(*) FROM users WHERE restricted = false AND DATE(created_at) BETWEEN %s AND %s) AS total_signups,
        (SELECT COUNT(DISTINCT user_id) FROM user_classification) AS total_active_users,
        (SELECT COUNT(*) FROM user_classification WHERE usage_type = 'one_time_usage') AS one_time_usage_users,
        (SELECT COUNT(*) FROM user_classification WHERE usage_type = 'recurring') AS recurring_users,
        (SELECT COUNT(*) FROM user_classification WHERE is_first_time_user = TRUE) AS first_time_users,
        
        -- Engagement
        (SELECT avg_dau FROM dau_metrics) AS avg_dau,
        (SELECT avg_wau FROM wau_metrics) AS avg_wau,
        (SELECT avg_mau FROM mau_metrics) AS avg_mau,
 
        -- Feature-level usage
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'spending' AND activity_date BETWEEN %s AND %s) AS spending_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'savings' AND activity_date BETWEEN %s AND %s) AS savings_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'investment' AND activity_date BETWEEN %s AND %s) AS investment_users,
        (SELECT COUNT(DISTINCT user_id) FROM all_feature_usage WHERE feature = 'lady_ai' AND activity_date BETWEEN %s AND %s) AS lady_ai_users,
        (SELECT COUNT(*) FROM (
            SELECT user_id, COUNT(DISTINCT feature) as feature_count
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) = 1
        ) single_feature) AS single_feature_users,
        (SELECT COUNT(*) FROM (
            SELECT user_id, COUNT(DISTINCT feature) as feature_count
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) > 1
        ) multi_feature) AS multiple_feature_users,
        
        -- Single feature users (users who use only one specific feature)
        (SELECT COUNT(*) FROM (
            SELECT user_id
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) = 1 AND MAX(feature) = 'spending'
        ) single_spending) AS only_spending_users,
        (SELECT COUNT(*) FROM (
            SELECT user_id
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) = 1 AND MAX(feature) = 'savings'
        ) single_savings) AS only_savings_users,
        (SELECT COUNT(*) FROM (
            SELECT user_id
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) = 1 AND MAX(feature) = 'investment'
        ) single_investment) AS only_investment_users,
        (SELECT COUNT(*) FROM (
            SELECT user_id
            FROM all_feature_usage 
            WHERE activity_date BETWEEN %s AND %s
            GROUP BY user_id
            HAVING COUNT(DISTINCT feature) = 1 AND MAX(feature) = 'lady_ai'
        ) single_lady_ai) AS only_lady_ai_users;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        params = [start_date, end_date] * 16  # Added 4 more for single feature specific users
        df = pd.read_sql_query(query, connection, params=tuple(params))
        return df


@st.cache_data(ttl=300)
def fetch_feature_combinations(start_date, end_date):
    """Fetch feature combinations for multiple feature users"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    WITH all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets
        UNION
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump
    ),
    
    user_features AS (
        SELECT 
            user_id,
            STRING_AGG(DISTINCT feature, ' + ' ORDER BY feature) AS feature_combination,
            COUNT(DISTINCT feature) AS feature_count
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
        GROUP BY user_id
        HAVING COUNT(DISTINCT feature) > 1
    )
    
    SELECT 
        feature_combination,
        COUNT(*) AS user_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
    FROM user_features
    GROUP BY feature_combination
    ORDER BY user_count DESC;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=(start_date, end_date))
        return df


@st.cache_data(ttl=300)
def fetch_feature_specific_metrics(start_date, end_date, feature):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    feature_queries = {
        "spending": """
            SELECT user_id::TEXT, DATE(created_at) AS activity_date
            FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
            UNION
            SELECT user_id::TEXT, DATE(created_at)
            FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
        """,
        "savings": """
            SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
        """,
        "investment": """
            SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
            FROM transactions t
            JOIN investment_plans ip ON ip.id = t.investment_plan_id
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
        """,
        "lady_ai": """
            SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
            FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
        """,
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

    params_count = 2 if feature in ["savings", "investment", "lady_ai"] else 4
    params = [start_date, end_date] + [start_date, end_date] * (params_count // 2)

    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=tuple(params))
    return df


@st.cache_data(ttl=300)
def fetch_retention_metrics(start_date, end_date, feature=None):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    if feature:
        feature_queries = {
            "spending": """
                SELECT user_id::TEXT, DATE(created_at) AS activity_date
                FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
                UNION
                SELECT user_id::TEXT, DATE(created_at)
                FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            """,
            "savings": """
                SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            "investment": """
                SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN investment_plans ip ON ip.id = t.investment_plan_id
                WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            "lady_ai": """
                SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
                FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
            """,
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
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT p.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
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
        params_count = 2 if feature in ["savings", "investment", "lady_ai"] else 4
        params = [start_date, end_date] + [start_date, end_date] * (params_count // 2)
    else:
        params = [start_date, end_date] * 6

    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=tuple(params))
    return df


@st.cache_data(ttl=300)
def fetch_trend_data(start_date, end_date, feature=None):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if feature:
        feature_queries = {
            "spending": """
                SELECT user_id::TEXT, DATE(created_at) AS activity_date
                FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
                UNION
                SELECT user_id::TEXT, DATE(created_at)
                FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
            """,
            "savings": """
                SELECT p.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            "investment": """
                SELECT ip.user_id::TEXT, DATE(t.updated_at) AS activity_date
                FROM transactions t
                JOIN investment_plans ip ON ip.id = t.investment_plan_id
                WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            """,
            "lady_ai": """
                SELECT "user"::TEXT AS user_id, DATE(created_at) AS activity_date
                FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
            """,
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
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
            UNION
            SELECT p.user_id::TEXT, DATE(t.updated_at)
            FROM transactions t
            JOIN plans p ON p.id = t.plan_id
            WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
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
        params_count = 2 if feature in ["savings", "investment", "lady_ai"] else 4
        params = [start_date, end_date] * (params_count // 2)
    else:
        params = [start_date, end_date] * 5

    engine = create_engine(db_url)
    with engine.connect() as connection:
        dau_df = pd.read_sql_query(dau_query, connection, params=tuple(params))
        wau_df = pd.read_sql_query(wau_query, connection, params=tuple(params))
        mau_df = pd.read_sql_query(mau_query, connection, params=tuple(params))

    return dau_df, wau_df, mau_df


# -------------------------------
# Overview Trend and Churn Helpers
# -------------------------------
@st.cache_data(ttl=300)
def fetch_overview_trend(start_date, end_date, aggregation='day'):
    """
    Trend for signups, active users, Lady AI users, and spending users within the selected period.
    Also includes absolute (cumulative from inception) metrics.
    
    Args:
        start_date: Start date for the period
        end_date: End date for the period
        aggregation: 'day', 'week', or 'month' - determines the granularity of the trend
    """
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    # Determine date truncation based on aggregation
    if aggregation == 'month':
        date_trunc = "DATE_TRUNC('month', {})::date"
        interval = "interval '1 month'"
    elif aggregation == 'week':
        date_trunc = "DATE_TRUNC('week', {})::date"
        interval = "interval '1 week'"
    else:  # day
        date_trunc = "{}"
        interval = "interval '1 day'"

    query = f"""
    WITH days AS (
        SELECT generate_series(%s::date, %s::date, {interval})::date AS dt
    ),
    signups AS (
        SELECT {date_trunc.format('DATE(created_at)')} AS dt, COUNT(*) AS signups
        FROM users
        WHERE restricted = false AND DATE(created_at) BETWEEN %s AND %s
        GROUP BY 1
    ),
    -- Absolute signups: cumulative from inception to each date
    absolute_signups AS (
        SELECT {date_trunc.format('DATE(created_at)')} AS dt, COUNT(*) AS absolute_signups
        FROM users
        WHERE restricted = false AND DATE(created_at) <= %s
        GROUP BY 1
    ),
    absolute_signups_cumulative AS (
        SELECT 
            dt,
            SUM(absolute_signups) OVER (ORDER BY dt) AS absolute_signups
        FROM absolute_signups
    ),
    all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT AS user_id, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets WHERE DATE(created_at) BETWEEN %s AND %s
        UNION ALL
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions WHERE DATE(created_at) BETWEEN %s AND %s
        UNION ALL
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
        UNION ALL
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) BETWEEN %s AND %s
        UNION ALL
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump WHERE DATE(created_at) BETWEEN %s AND %s
    ),
    -- All feature usage from inception for absolute active users
    all_feature_usage_absolute AS (
        -- Spending
        SELECT user_id::TEXT AS user_id, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets WHERE DATE(created_at) <= %s
        UNION ALL
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions WHERE DATE(created_at) <= %s
        UNION ALL
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) <= %s
        UNION ALL
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) <= %s
        UNION ALL
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump WHERE DATE(created_at) <= %s
    ),
    aggregated_usage AS (
        SELECT 
            {date_trunc.format('activity_date')} AS dt,
            user_id,
            feature
        FROM all_feature_usage
        GROUP BY 1, 2, 3
    ),
    -- Absolute active users: count distinct users who have been active up to each date
    aggregated_usage_absolute AS (
        SELECT 
            {date_trunc.format('activity_date')} AS dt,
            user_id,
            feature
        FROM all_feature_usage_absolute
        GROUP BY 1, 2, 3
    ),
    absolute_active AS (
        SELECT 
            dt,
            COUNT(DISTINCT user_id) AS absolute_active_users
        FROM (
            SELECT dt, user_id
            FROM aggregated_usage_absolute
            GROUP BY dt, user_id
        ) subq
        GROUP BY dt
    ),
    absolute_active_cumulative AS (
        SELECT 
            dt,
            (SELECT COUNT(DISTINCT user_id) 
             FROM aggregated_usage_absolute aua 
             WHERE aua.dt <= aa.dt) AS absolute_active_users
        FROM absolute_active aa
    ),
    active AS (
        SELECT dt, COUNT(DISTINCT user_id) AS active_users
        FROM aggregated_usage
        GROUP BY 1
    ),
    lady AS (
        SELECT dt, COUNT(DISTINCT user_id) AS lady_users
        FROM aggregated_usage
        WHERE feature = 'lady_ai'
        GROUP BY 1
    ),
    spending AS (
        SELECT dt, COUNT(DISTINCT user_id) AS spending_users
        FROM aggregated_usage
        WHERE feature = 'spending'
        GROUP BY 1
    ),
    savings AS (
        SELECT dt, COUNT(DISTINCT user_id) AS savings_users
        FROM aggregated_usage
        WHERE feature = 'savings'
        GROUP BY 1
    ),
    investment AS (
        SELECT dt, COUNT(DISTINCT user_id) AS investment_users
        FROM aggregated_usage
        WHERE feature = 'investment'
        GROUP BY 1
    )
    SELECT 
        d.dt AS activity_date,
        COALESCE(s.signups, 0) AS signups,
        COALESCE(a.active_users, 0) AS active_users,
        COALESCE(l.lady_users, 0) AS lady_users,
        COALESCE(sp.spending_users, 0) AS spending_users,
        COALESCE(sv.savings_users, 0) AS savings_users,
        COALESCE(iv.investment_users, 0) AS investment_users,
        COALESCE(abs_s.absolute_signups, 0) AS absolute_signups,
        COALESCE(abs_a.absolute_active_users, 0) AS absolute_active_users
    FROM days d
    LEFT JOIN signups s ON s.dt = d.dt
    LEFT JOIN active a ON a.dt = d.dt
    LEFT JOIN lady l ON l.dt = d.dt
    LEFT JOIN spending sp ON sp.dt = d.dt
    LEFT JOIN savings sv ON sv.dt = d.dt
    LEFT JOIN investment iv ON iv.dt = d.dt
    LEFT JOIN absolute_signups_cumulative abs_s ON abs_s.dt = d.dt
    LEFT JOIN absolute_active_cumulative abs_a ON abs_a.dt = d.dt
    ORDER BY d.dt;
    """

    params = (
        start_date, end_date,
        start_date, end_date,
        end_date,  # for absolute signups
        start_date, end_date,
        start_date, end_date,
        start_date, end_date,
        start_date, end_date,
        start_date, end_date,
        end_date,  # for absolute active users (spending)
        end_date,  # for absolute active users (manual transactions)
        end_date,  # for absolute active users (investment)
        end_date,  # for absolute active users (savings)
        end_date,  # for absolute active users (lady ai)
    )

    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=params)
    return df

@st.cache_data(ttl=300)
def fetch_churn_count(start_date, end_date):
    """Number of customers who used any feature before end_date but did not use any feature in [start_date, end_date]."""
    conn = get_database_connection()
    if conn is None:
        return 0

    query = """
    WITH all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT AS user_id, DATE(created_at) AS activity_date
        FROM budgets
        UNION
        SELECT user_id::TEXT, DATE(created_at)
        FROM manual_and_external_transactions
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at)
        FROM slack_message_dump
    ), users_ever AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE activity_date <= %s
    ), users_in_period AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
    )
    SELECT COUNT(*) AS churn_count
    FROM users_ever e
    LEFT JOIN users_in_period p ON e.user_id = p.user_id
    WHERE p.user_id IS NULL;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=(end_date, start_date, end_date))
        return int(df.iloc[0]["churn_count"]) if not df.empty else 0


# -------------------------------
# Customer Feature Analysis Functions
# -------------------------------
@st.cache_data(ttl=300)
def fetch_dormant_users_analysis(start_date, end_date, dormant_period_days):
    """Fetch analysis of dormant users - users who used features before but not in the specified period"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    # Calculate the historical period (before the current analysis period)
    analysis_start = pd.to_datetime(start_date)
    historical_start = analysis_start - timedelta(days=dormant_period_days)
    
    query = """
    WITH users_filtered AS (
        SELECT id::TEXT AS user_id, DATE(created_at) AS signup_date
        FROM users
        WHERE restricted = false
    ),

    all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets
        UNION
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump
    ),

    -- Users who had activity in historical period
    historical_users AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
    ),

    -- Users who had activity in current analysis period
    current_users AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
    ),

    -- Overall dormant users (used features historically but not in current period)
    overall_dormant AS (
        SELECT h.user_id
        FROM historical_users h
        LEFT JOIN current_users c ON h.user_id = c.user_id
        WHERE c.user_id IS NULL
    ),

    -- Feature-specific dormant users
    spending_historical AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'spending' AND activity_date BETWEEN %s AND %s
    ),
    spending_current AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'spending' AND activity_date BETWEEN %s AND %s
    ),
    spending_dormant AS (
        SELECT h.user_id
        FROM spending_historical h
        LEFT JOIN spending_current c ON h.user_id = c.user_id
        WHERE c.user_id IS NULL
    ),

    savings_historical AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'savings' AND activity_date BETWEEN %s AND %s
    ),
    savings_current AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'savings' AND activity_date BETWEEN %s AND %s
    ),
    savings_dormant AS (
        SELECT h.user_id
        FROM savings_historical h
        LEFT JOIN savings_current c ON h.user_id = c.user_id
        WHERE c.user_id IS NULL
    ),

    investment_historical AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'investment' AND activity_date BETWEEN %s AND %s
    ),
    investment_current AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'investment' AND activity_date BETWEEN %s AND %s
    ),
    investment_dormant AS (
        SELECT h.user_id
        FROM investment_historical h
        LEFT JOIN investment_current c ON h.user_id = c.user_id
        WHERE c.user_id IS NULL
    ),

    lady_ai_historical AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'lady_ai' AND activity_date BETWEEN %s AND %s
    ),
    lady_ai_current AS (
        SELECT DISTINCT user_id
        FROM all_feature_usage
        WHERE feature = 'lady_ai' AND activity_date BETWEEN %s AND %s
    ),
    lady_ai_dormant AS (
        SELECT h.user_id
        FROM lady_ai_historical h
        LEFT JOIN lady_ai_current c ON h.user_id = c.user_id
        WHERE c.user_id IS NULL
    )

    SELECT
        (SELECT COUNT(*) FROM overall_dormant) AS overall_dormant_users,
        (SELECT COUNT(*) FROM spending_dormant) AS spending_dormant_users,
        (SELECT COUNT(*) FROM savings_dormant) AS savings_dormant_users,
        (SELECT COUNT(*) FROM investment_dormant) AS investment_dormant_users,
        (SELECT COUNT(*) FROM lady_ai_dormant) AS lady_ai_dormant_users,
        (SELECT COUNT(*) FROM historical_users) AS total_historical_users,
        (SELECT COUNT(*) FROM current_users) AS total_current_users;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        params = [
            historical_start.date(), analysis_start.date(),  # Historical period
            start_date, end_date,  # Current analysis period
            historical_start.date(), analysis_start.date(),  # Spending historical
            start_date, end_date,  # Spending current
            historical_start.date(), analysis_start.date(),  # Savings historical
            start_date, end_date,  # Savings current
            historical_start.date(), analysis_start.date(),  # Investment historical
            start_date, end_date,  # Investment current
            historical_start.date(), analysis_start.date(),  # Lady AI historical
            start_date, end_date,  # Lady AI current
        ]
        df = pd.read_sql_query(query, connection, params=tuple(params))
        return df


@st.cache_data(ttl=300)
def fetch_dormant_users_trend(start_date, end_date, dormant_period_days):
    """Fetch trend data for dormant users over time"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    # Calculate the historical period
    analysis_start = pd.to_datetime(start_date)
    historical_start = analysis_start - timedelta(days=dormant_period_days)
    
    query = """
    WITH all_feature_usage AS (
        -- Spending
        SELECT user_id::TEXT, DATE(created_at) AS activity_date, 'spending' AS feature
        FROM budgets
        UNION
        SELECT user_id::TEXT, DATE(created_at), 'spending'
        FROM manual_and_external_transactions
        UNION
        -- Investment
        SELECT ip.user_id::TEXT, DATE(t.updated_at), 'investment'
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Savings
        SELECT p.user_id::TEXT, DATE(t.updated_at), 'savings'
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar'
        UNION
        -- Lady AI
        SELECT "user"::TEXT, DATE(created_at), 'lady_ai'
        FROM slack_message_dump
    ),

    -- Get all unique dates in the analysis period
    analysis_dates AS (
        SELECT DISTINCT activity_date
        FROM all_feature_usage
        WHERE activity_date BETWEEN %s AND %s
        ORDER BY activity_date
    ),

    -- For each date, calculate dormant users
    daily_dormant AS (
        SELECT 
            ad.activity_date,
            COUNT(DISTINCT h.user_id) AS dormant_count
        FROM analysis_dates ad
        CROSS JOIN (
            SELECT DISTINCT user_id
            FROM all_feature_usage
            WHERE activity_date BETWEEN %s AND %s
        ) h
        LEFT JOIN (
            SELECT DISTINCT user_id, activity_date
            FROM all_feature_usage
            WHERE activity_date BETWEEN %s AND %s
        ) c ON h.user_id = c.user_id AND c.activity_date = ad.activity_date
        WHERE c.user_id IS NULL
        GROUP BY ad.activity_date
        ORDER BY ad.activity_date
    )

    SELECT * FROM daily_dormant;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        params = [
            start_date, end_date,  # Analysis period for dates
            historical_start.date(), analysis_start.date(),  # Historical period for users
            start_date, end_date,  # Current period for comparison
        ]
        df = pd.read_sql_query(query, connection, params=tuple(params))
        return df


def analyze_feature_usage_patterns(comprehensive_df):
    """Analyze feature usage patterns and provide insights"""
    insights = []
    
    if comprehensive_df.empty:
        return insights
    
    # Get feature usage counts
    spending_users = comprehensive_df['spending_users'][0]
    savings_users = comprehensive_df['savings_users'][0] 
    investment_users = comprehensive_df['investment_users'][0]
    lady_ai_users = comprehensive_df['lady_ai_users'][0]
    
    # Find most and least used features
    feature_usage = {
        'Spending': spending_users,
        'Savings': savings_users,
        'Investment': investment_users,
        'Lady AI': lady_ai_users
    }
    
    most_used = max(feature_usage, key=feature_usage.get)
    least_used = min(feature_usage, key=feature_usage.get)
    
    # Calculate feature usage percentages
    total_active = comprehensive_df['total_active_users'][0]
    if total_active > 0:
        most_used_pct = (feature_usage[most_used] / total_active) * 100
        least_used_pct = (feature_usage[least_used] / total_active) * 100
        
        insights.append({
            'most_used_feature': most_used,
            'most_used_count': feature_usage[most_used],
            'most_used_pct': most_used_pct,
            'least_used_feature': least_used,
            'least_used_count': feature_usage[least_used],
            'least_used_pct': least_used_pct
        })
    
    return insights


def generate_insights(metrics_df, retention_df, feature=None):
    """Generate actionable insights based on the data"""
    insights = []

    if not metrics_df.empty:
        # Engagement insights
        total_signups = (
            metrics_df["total_signups"].iloc[0]
            if "total_signups" in metrics_df.columns
            else 0
        )
        total_active = (
            metrics_df["total_active_users"].iloc[0]
            if "total_active_users" in metrics_df.columns
            else 0
        )

        if total_signups > 0:
            activation_rate = (total_active / total_signups) * 100

            if activation_rate < 30:
                insights.append(
                    {
                        "title": "Low Activation Rate Alert",
                        "insight": f"Only {activation_rate:.1f}% of signups become active users.",
                        "recommendation": "Improve onboarding flow and feature discovery.",
                        "icon": "⚠️",
                    }
                )
            elif activation_rate > 70:
                insights.append(
                    {
                        "title": "Excellent Activation Rate",
                        "insight": f"{activation_rate:.1f}% activation rate shows strong product-market fit.",
                        "recommendation": "Scale marketing efforts and maintain current onboarding quality.",
                        "icon": "🎉",
                    }
                )

    if not retention_df.empty and "day1_retention" in retention_df.columns:
        day1_retention = (
            retention_df["day1_retention"].iloc[0] * 100
            if retention_df["day1_retention"].iloc[0]
            else 0
        )
        week1_retention = (
            retention_df["week1_retention"].iloc[0] * 100
            if retention_df["week1_retention"].iloc[0]
            else 0
        )

        if day1_retention < 20:
            insights.append(
                {
                    "title": "Day 1 Retention Needs Attention",
                    "insight": f"Only {day1_retention:.1f}% of users return the next day.",
                    "recommendation": "Implement push notifications and email sequences for new users.",
                    "icon": "📱",
                }
            )

        if week1_retention > 50:
            insights.append(
                {
                    "title": "Strong Week 1 Retention",
                    "insight": f"{week1_retention:.1f}% of users return within a week.",
                    "recommendation": "Focus on converting these engaged users to power users.",
                    "icon": "💪",
                }
            )

    return insights


# -------------------------------
# Streamlit App Configuration
# -------------------------------
st.set_page_config(
    page_title="📊 Ladder Marketing Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_custom_css()

# Header
st.markdown(
    f"""
<div style="text-align: center; padding: 20px; background: linear-gradient(90deg, {LADDER_COLORS['navy']}, {LADDER_COLORS['purple']}); border-radius: 15px; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0; font-size: 2.5rem;">📊 Ladder Marketing Analytics Hub</h1>
    <p style="color: white; opacity: 0.9; margin: 10px 0 0 0;">Comprehensive growth, retention, and feature engagement analytics</p>
</div>
""",
    unsafe_allow_html=True,
)

# -------------------------------
# Sidebar Configuration
# -------------------------------
st.sidebar.markdown(
    f"""
<div style="background: linear-gradient(180deg, {LADDER_COLORS['orange']}, {LADDER_COLORS['yellow']}); 
            padding: 15px; border-radius: 10px; margin-bottom: 20px; text-align: center;">
    <h3 style="color: white; margin: 0;">🎯 Analytics Control Center</h3>
</div>
""",
    unsafe_allow_html=True,
)

# Date Range Selection
today = datetime.today().date()
options = {
    "Last 7 Days": (today - timedelta(days=7), today),
    "Last 30 Days": (today - timedelta(days=30), today),
    "This Month": (today.replace(day=1), today),
    "Custom": None,
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
@st.cache_data(ttl=300)
def fetch_absolute_metrics(end_date):
    """
    Fetch absolute metrics from inception to the end date.
    Returns total signups and active users since inception.
    """
    conn = get_database_connection()
    if conn is None:
        return {}

    absolute_query = """
    WITH feature_usage AS (
        SELECT user_id::TEXT, DATE(created_at) AS activity_date
        FROM budgets WHERE DATE(created_at) <= %s
        UNION
        SELECT user_id::TEXT, DATE(created_at)
        FROM manual_and_external_transactions WHERE DATE(created_at) <= %s
        UNION
        SELECT ip.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN investment_plans ip ON ip.id = t.investment_plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) <= %s
        UNION
        SELECT p.user_id::TEXT, DATE(t.updated_at)
        FROM transactions t
        JOIN plans p ON p.id = t.plan_id
        WHERE t.status = 'success' AND t.provider_number != 'Flex Dollar' AND DATE(t.updated_at) <= %s
        UNION
        SELECT "user"::TEXT, DATE(created_at)
        FROM slack_message_dump WHERE DATE(created_at) <= %s
    )
    SELECT 
        (SELECT COUNT(*) FROM users WHERE DATE(created_at) <= %s AND restricted = false) AS absolute_total_signups,
        (SELECT COUNT(DISTINCT user_id) FROM feature_usage) AS absolute_total_active_users;
    """

    engine = create_engine(db_url)
    with engine.connect() as connection:
        abs_df = pd.read_sql_query(absolute_query, connection, params=(end_date, end_date, end_date, end_date, end_date, end_date))
        if not abs_df.empty:
            return abs_df.iloc[0].to_dict()
    return {}

# Fetch comprehensive metrics
comprehensive_df = fetch_comprehensive_metrics(start_date, end_date)

# Fetch absolute metrics HERE (before using them in tabs)
absolute_metrics = fetch_absolute_metrics(end_date)

# -------------------------------
# Main Dashboard - Overview Tab System
# -------------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    [
        "📊 Overview",
        "💰 Spending Analytics",
        "🤖 Lady AI Analytics",
        "🏦 Savings Analytics",
        "📈 Investment Analytics",
        "📋 FFP Engagement",
        "🔍 Customer Feature Analysis",
    ]
)

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
                    f"Total signups from inception to {end_date}",
                    "navy",
                    "🚀",
                ),
                unsafe_allow_html=True,
            )
        with col_abs2:
            st.markdown(
                create_metric_card(
                    "Absolute Active Users",
                    f"{absolute_metrics['absolute_total_active_users']:,}",
                    f"Total active users from inception to {end_date}",
                    "navy",
                    "⚡",
                ),
                unsafe_allow_html=True,
            )

        st.subheader("📈 Comprehensive Metrics Overview")
        # Period-Specific Metrics
        if not comprehensive_df.empty:
            # Row 1: Core Metrics
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                st.markdown(
                    create_metric_card(
                        "Total Signups",
                        f"{comprehensive_df['total_signups'][0]:,}",
                        f"New signups from {start_date} to {end_date}",
                        "azure",
                        "👥",
                    ),
                    unsafe_allow_html=True,
                )

            with col2:
                st.markdown(
                    create_metric_card(
                        "Total Active Users",
                        f"{comprehensive_df['total_active_users'][0]:,}",
                        f"Users active from {start_date} to {end_date}",
                        "azure",
                        "🎯",
                    ),
                    unsafe_allow_html=True,
                )

            with col3:
                st.markdown(
                    create_metric_card(
                        "First Time Users",
                        f"{comprehensive_df['first_time_users'][0]:,}",
                        "Users who used features for the first time in this period",
                        "azure",
                        "🌟",
                    ),
                    unsafe_allow_html=True,
                )
            with col4:
                st.markdown(
                    create_metric_card(
                        "One Time Usage Users",
                        f"{comprehensive_df['one_time_usage_users'][0]:,}",
                        "Users active on exactly one day",
                        "azure",
                        "📅",
                    ),
                    unsafe_allow_html=True,
                )

            with col5:
                churn_kpi = fetch_churn_count(start_date, end_date)
                st.markdown(
                    create_metric_card(
                        "User Churn",
                        f"{churn_kpi:,}",
                        f"No activity between {start_date} and {end_date}",
                        "red",
                        "📉",
                    ),
                    unsafe_allow_html=True,
                )

        # Additional metrics row
        if not comprehensive_df.empty:
            col_add1, col_add2, col_add3 = st.columns(3)
            
            with col_add1:
                st.markdown(
                    create_metric_card(
                        "Recurring Users",
                        f"{comprehensive_df['recurring_users'][0]:,}",
                        "Users with multiple active days",
                        "azure",
                        "🔄",
                    ),
                    unsafe_allow_html=True,
                )
            
            with col_add2:
                # Calculate Signup Conversion Rate with insights
                total_signups = comprehensive_df['total_signups'][0]
                first_time_users = comprehensive_df['first_time_users'][0]
                conversion_rate = (first_time_users / total_signups * 100) if total_signups > 0 else 0
                
                # Determine alert level and insight
                conversion_alert = "high" if conversion_rate < 20 else "medium" if conversion_rate < 40 else "low"
                conversion_insight = "Critical: Improve onboarding flow" if conversion_rate < 20 else "Good: Focus on activation campaigns" if conversion_rate < 40 else "Excellent: Maintain current strategy"
                
                # Create the metric card HTML
                metric_html = create_metric_card(
                    "Signup Conversion Rate",
                    f"{conversion_rate:.1f}%",
                    f"{first_time_users:,} of {total_signups:,} signups activated",
                    "green" if conversion_rate > 40 else "orange" if conversion_rate > 20 else "red",
                    "📈",
                    alert_level=conversion_alert,
                    additional_insight=conversion_insight
                )
                # Use components.html for better HTML rendering
                components.html(metric_html, height=200)
            
            with col_add3:
                # Calculate feature adoption rate with insights
                total_active = comprehensive_df['total_active_users'][0]
                first_time_users = comprehensive_df['first_time_users'][0]
                feature_adoption = min((first_time_users / total_active * 100), 100.0) if total_active > 0 else 0
                
                # Determine alert level and insight
                adoption_alert = "high" if feature_adoption > 80 else "medium" if feature_adoption > 50 else "low"
                adoption_insight = "High new user acquisition" if feature_adoption > 80 else "Balanced user growth" if feature_adoption > 50 else "Focus on user retention"
                
                # Create the metric card HTML
                adoption_html = create_metric_card(
                    "Feature Adoption Rate",
                    f"{feature_adoption:.1f}%",
                    f"{first_time_users:,} new feature users",
                    "green" if feature_adoption > 70 else "orange" if feature_adoption > 40 else "red",
                    "🎯",
                    alert_level=adoption_alert,
                    additional_insight=adoption_insight
                )
                # Use components.html for better HTML rendering
                components.html(adoption_html, height=200)

        # Row 4: Enhanced Feature-Specific Metrics with Insights
        st.subheader("🎪 Feature Engagement with Insights")
        col3, col4, col5, col6 = st.columns(4)

        # Calculate feature usage percentages for insights
        total_active = comprehensive_df['total_active_users'][0]
        spending_pct = (comprehensive_df['spending_users'][0] / total_active * 100) if total_active > 0 else 0
        lady_ai_pct = (comprehensive_df['lady_ai_users'][0] / total_active * 100) if total_active > 0 else 0
        savings_pct = (comprehensive_df['savings_users'][0] / total_active * 100) if total_active > 0 else 0
        investment_pct = (comprehensive_df['investment_users'][0] / total_active * 100) if total_active > 0 else 0

        with col3:
            # Determine alert level and insight for spending
            spending_alert = "low" if spending_pct > 40 else "medium" if spending_pct < 20 else "info"
            spending_insight = "Strong spending engagement" if spending_pct > 40 else "Consider spending feature promotion" if spending_pct < 20 else "Normal spending usage"
            
            spending_html = create_metric_card(
                "Spending Users",
                f"{comprehensive_df['spending_users'][0]:,}",
                f"{spending_pct:.1f}% of active users",
                "blue",  # Blue for spending
                "💰",
                alert_level=spending_alert,
                additional_insight=spending_insight
            )
            components.html(spending_html, height=200)

        with col4:
            # Determine alert level and insight for Lady AI
            lady_ai_alert = "low" if lady_ai_pct > 30 else "medium" if lady_ai_pct < 10 else "info"
            lady_ai_insight = "High AI engagement" if lady_ai_pct > 30 else "Boost Lady AI adoption" if lady_ai_pct < 10 else "Steady AI usage"
            
            lady_ai_html = create_metric_card(
                "Lady AI Users",
                f"{comprehensive_df['lady_ai_users'][0]:,}",
                f"{lady_ai_pct:.1f}% of active users",
                "orange",  # Orange for Lady AI
                "🤖",
                alert_level=lady_ai_alert,
                additional_insight=lady_ai_insight
            )
            components.html(lady_ai_html, height=200)

        with col5:
            # Determine alert level and insight for savings
            savings_alert = "low" if savings_pct > 25 else "medium" if savings_pct < 15 else "info"
            savings_insight = "Strong savings culture" if savings_pct > 25 else "Promote savings features" if savings_pct < 15 else "Healthy savings usage"
            
            savings_html = create_metric_card(
                "Savings Users",
                f"{comprehensive_df['savings_users'][0]:,}",
                f"{savings_pct:.1f}% of active users",
                "green",
                "🏦",
                alert_level=savings_alert,
                additional_insight=savings_insight
            )
            components.html(savings_html, height=200)

        with col6:
            # Determine alert level and insight for investment
            investment_alert = "low" if investment_pct > 20 else "medium" if investment_pct < 8 else "info"
            investment_insight = "High investment engagement" if investment_pct > 20 else "Focus on investment adoption" if investment_pct < 8 else "Growing investment usage"
            
            investment_html = create_metric_card(
                "Investment Users",
                f"{comprehensive_df['investment_users'][0]:,}",
                f"{investment_pct:.1f}% of active users",
                "purple",
                "📈",
                alert_level=investment_alert,
                additional_insight=investment_insight
            )
            components.html(investment_html, height=200)

        # Row 2: Overall Engagement Metrics
        st.subheader("📊 Overall Engagement Metrics")
        col_eng1, col_eng2, col_eng3 = st.columns(3)
        
        with col_eng1:
            st.markdown(
                create_metric_card(
                    "Average DAU",
                    f"{comprehensive_df['avg_dau'][0]:,.0f}",
                    "Average daily active users across all features",
                    "azure",
                    "📅",
                ),
                unsafe_allow_html=True,
            )
        
        with col_eng2:
            st.markdown(
                create_metric_card(
                    "Average WAU",
                    f"{comprehensive_df['avg_wau'][0]:,.0f}",
                    "Average weekly active users across all features",
                    "azure",
                    "📆",
                ),
                unsafe_allow_html=True,
            )
        
        with col_eng3:
            st.markdown(
                create_metric_card(
                    "Average MAU",
                    f"{comprehensive_df['avg_mau'][0]:,.0f}",
                    "Average monthly active users across all features",
                    "azure",
                    "🗓️",
                ),
                unsafe_allow_html=True,
            )

        # Daily Trend: configurable and polished
        st.subheader("📈 Trend Analysis")

        # Determine default aggregation based on date range
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        start_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
        end_dt = datetime.strptime(str(end_date), '%Y-%m-%d')
        months_diff = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month)

        # Auto-select month aggregation if 2+ months, otherwise day
        default_aggregation = 'month' if months_diff >= 2 else 'day'

        # Let user override the aggregation
        aggregation_options = {
            'Day-to-Day': 'day',
            'Week-to-Week': 'week',
            'Month-to-Month': 'month'
        }

        col_agg, col_metrics = st.columns([1, 2])

        with col_agg:
            selected_agg_display = st.selectbox(
                "Trend Granularity",
                options=list(aggregation_options.keys()),
                index=list(aggregation_options.values()).index(default_aggregation),
                help=f"{'Auto-selected Month-to-Month (2+ months in period). ' if default_aggregation == 'month' else ''}Choose how to aggregate the trend data"
            )
            aggregation = aggregation_options[selected_agg_display]

        with col_metrics:
            metric_display_to_col = {
                "Signups": "signups",
                "Active Users": "active_users",
                "Absolute Signups": "absolute_signups",
                "Absolute Active Users": "absolute_active_users",
                "Lady Users": "lady_users",
                "Spending Users": "spending_users",
                "Savings Users": "savings_users",
                "Investment Users": "investment_users",
            }
            default_metrics = ["Signups", "Active Users"]
            selected = st.multiselect(
                "Select trend lines",
                options=list(metric_display_to_col.keys()),
                default=default_metrics,
                help="Period metrics show activity within selected dates. Absolute metrics show cumulative totals from inception.",
            )

        # Fetch trend data with selected aggregation
        trend_df = fetch_overview_trend(start_date, end_date, aggregation)

        if not trend_df.empty and selected:
            selected_cols = [metric_display_to_col[m] for m in selected]
            base_cols = ["activity_date"] + selected_cols
            df_plot = trend_df[base_cols].copy()

            # Optional smoothing (only show for day and week aggregations)
            smooth = False
            if aggregation in ['day', 'week']:
                smooth = st.checkbox(
                    f"Smooth ({7 if aggregation == 'day' else 4}-{'day' if aggregation == 'day' else 'week'} rolling average)", 
                    value=False
                )

            plot_df = df_plot.melt(
                id_vars=["activity_date"],
                var_name="metric",
                value_name="count",
            )
            metric_name_map = {v: k for k, v in metric_display_to_col.items()}
            plot_df["metric"] = plot_df["metric"].map(metric_name_map)
            plot_df = plot_df.sort_values(["metric", "activity_date"])
            
            if smooth:
                window_size = 7 if aggregation == 'day' else 4
                plot_df["count"] = (
                    plot_df.groupby("metric")["count"].transform(
                        lambda s: s.rolling(window_size, min_periods=1).mean()
                    )
                )

            color_map = {
                "Signups": LADDER_COLORS["navy"],
                "Active Users": LADDER_COLORS["azure"],
                "Absolute Signups": LADDER_COLORS["purple"],
                "Absolute Active Users": LADDER_COLORS["orange"],
                "Lady Users": LADDER_COLORS["orange"],
                "Spending Users": LADDER_COLORS["blue"],
                "Savings Users": LADDER_COLORS["green"],
                "Investment Users": LADDER_COLORS["purple"],
            }

            fig_overview_trend = px.line(
                plot_df,
                x="activity_date",
                y="count",
                color="metric",
                color_discrete_map=color_map,
                markers=True,
                labels={"activity_date": "Date", "count": "Users", "metric": ""},
            )
            fig_overview_trend.update_traces(line=dict(width=3), marker=dict(size=6, symbol="circle"))
            fig_overview_trend.update_layout(
                hovermode="x unified",
                margin=dict(l=0, r=0, t=10, b=0),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                xaxis_showgrid=False,
                yaxis_showgrid=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_overview_trend, use_container_width=True)
        else:
            if not selected:
                st.info("Please select at least one metric to display the trend.")


        # Row 3: Overall Retention Metrics
        st.subheader("🔄 Overall Retention Metrics")
        col_ret1, col_ret2, col_ret3 = st.columns(3)
        
        # Fetch overall retention data
        overall_retention = fetch_retention_metrics(start_date, end_date)
        
        if not overall_retention.empty:
            day1_ret = overall_retention['day1_retention'][0] * 100 if overall_retention['day1_retention'][0] else 0
            week1_ret = overall_retention['week1_retention'][0] * 100 if overall_retention['week1_retention'][0] else 0
            month1_ret = overall_retention['month1_retention'][0] * 100 if overall_retention['month1_retention'][0] else 0
            
            with col_ret1:
                st.markdown(
                    create_metric_card(
                        "Day 1 Retention",
                        f"{day1_ret:.1f}%",
                        "Users returning the next day",
                        "azure",
                        "📱",
                    ),
                    unsafe_allow_html=True,
                )
            
            with col_ret2:
                st.markdown(
                    create_metric_card(
                        "Week 1 Retention",
                        f"{week1_ret:.1f}%",
                        "Users returning within a week",
                        "azure",
                        "🗓️",
                    ),
                    unsafe_allow_html=True,
                )
            
            with col_ret3:
                st.markdown(
                    create_metric_card(
                        "Month 1 Retention",
                        f"{month1_ret:.1f}%",
                        "Users returning within a month",
                        "azure",
                        "📊",
                    ),
                    unsafe_allow_html=True,
                )

        # Row 5: Enhanced Feature Usage Patterns & Analytics
        st.subheader("🎯 Feature Usage Patterns & Analytics")
        
        # Get feature usage analysis
        feature_analysis = analyze_feature_usage_patterns(comprehensive_df)
        
        # Single vs Multiple Feature Users
        col_pattern1, col_pattern2 = st.columns(2)

        with col_pattern1:
            # Calculate single feature users with insights
            single_feature_users = comprehensive_df['single_feature_users'][0] if 'single_feature_users' in comprehensive_df.columns else 0
            total_active_for_patterns = comprehensive_df['total_active_users'][0]
            single_feature_pct = (single_feature_users / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            # Determine alert level for single feature users
            alert_level = "medium" if single_feature_pct > 60 else "low" if single_feature_pct < 40 else "info"
            additional_insight = f"Opportunity to cross-sell other features" if single_feature_pct > 50 else "Good feature adoption balance"
            
            single_html = create_metric_card(
                "Single Feature Users",
                f"{single_feature_users:,}",
                f"{single_feature_pct:.1f}% of active users use only one feature",
                "azure",
                "🎯",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(single_html, height=200)

        with col_pattern2:
            # Calculate multiple feature users with insights
            multiple_feature_users = comprehensive_df['multiple_feature_users'][0] if 'multiple_feature_users' in comprehensive_df.columns else 0
            multiple_feature_pct = (multiple_feature_users / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            # Determine alert level for multiple feature users
            alert_level = "low" if multiple_feature_pct > 40 else "medium" if multiple_feature_pct < 20 else "info"
            additional_insight = f"High engagement users - focus on retention" if multiple_feature_pct > 40 else "Potential to increase feature adoption"
            
            multiple_html = create_metric_card(
                "Multiple Feature Users",
                f"{multiple_feature_users:,}",
                f"{multiple_feature_pct:.1f}% of active users use multiple features",
                "azure",
                "🔄",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(multiple_html, height=200)
        
        # Row 6: Most/Least Used Features
        st.subheader("📊 Feature Usage Rankings")
        col_rank1, col_rank2 = st.columns(2)
        
        if feature_analysis:
            analysis = feature_analysis[0]
            
            with col_rank1:
                # Most used feature
                most_used = analysis['most_used_feature']
                most_used_count = analysis['most_used_count']
                most_used_pct = analysis['most_used_pct']
                
                # Determine alert level
                alert_level = "low" if most_used_pct > 60 else "info"
                additional_insight = f"Leading feature - leverage for marketing campaigns"
                
                most_used_html = create_metric_card(
                    f"Most Used Feature: {most_used}",
                    f"{most_used_count:,}",
                    f"{most_used_pct:.1f}% of active users",
                    "green",
                    "🏆",
                    alert_level=alert_level,
                    additional_insight=additional_insight
                )
                components.html(most_used_html, height=200)
            
            with col_rank2:
                # Least used feature
                least_used = analysis['least_used_feature']
                least_used_count = analysis['least_used_count']
                least_used_pct = analysis['least_used_pct']
                
                # Determine alert level
                alert_level = "high" if least_used_pct < 15 else "medium" if least_used_pct < 25 else "info"
                additional_insight = f"Focus on improving {least_used.lower()} adoption" if least_used_pct < 20 else "Consider feature enhancement"
                
                least_used_html = create_metric_card(
                    f"Least Used Feature: {least_used}",
                    f"{least_used_count:,}",
                    f"{least_used_pct:.1f}% of active users",
                    "red",
                    "📉",
                    alert_level=alert_level,
                    additional_insight=additional_insight
                )
                components.html(least_used_html, height=200)
        
        # Row 7: Single Feature Specific Users
        st.subheader("🎯 Single Feature Specific Users")
        col_single1, col_single2, col_single3, col_single4 = st.columns(4)
        
        # Only Spending Users
        with col_single1:
            only_spending = comprehensive_df['only_spending_users'][0] if 'only_spending_users' in comprehensive_df.columns else 0
            spending_pct = (only_spending / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            alert_level = "info" if spending_pct > 10 else "medium"
            additional_insight = "Cross-sell savings/investment features" if spending_pct > 15 else "Normal spending-only user segment"
            
            only_spending_html = create_metric_card(
                "Only Spending Users",
                f"{only_spending:,}",
                f"{spending_pct:.1f}% of active users",
                "blue",
                "💰",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(only_spending_html, height=200)
        
        # Only Lady AI Users
        with col_single2:
            only_lady_ai = comprehensive_df['only_lady_ai_users'][0] if 'only_lady_ai_users' in comprehensive_df.columns else 0
            lady_ai_pct = (only_lady_ai / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            alert_level = "info" if lady_ai_pct > 5 else "medium"
            additional_insight = "Engage with financial planning features" if lady_ai_pct > 10 else "AI-only users segment"
            
            only_lady_ai_html = create_metric_card(
                "Only Lady AI Users",
                f"{only_lady_ai:,}",
                f"{lady_ai_pct:.1f}% of active users",
                "orange",
                "🤖",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(only_lady_ai_html, height=200)
        
        # Only Savings Users
        with col_single3:
            only_savings = comprehensive_df['only_savings_users'][0] if 'only_savings_users' in comprehensive_df.columns else 0
            savings_pct = (only_savings / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            alert_level = "info" if savings_pct > 8 else "medium"
            additional_insight = "Introduce spending tracking" if savings_pct > 12 else "Savings-focused user segment"
            
            only_savings_html = create_metric_card(
                "Only Savings Users",
                f"{only_savings:,}",
                f"{savings_pct:.1f}% of active users",
                "green",
                "🏦",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(only_savings_html, height=200)
        
        # Only Investment Users
        with col_single4:
            only_investment = comprehensive_df['only_investment_users'][0] if 'only_investment_users' in comprehensive_df.columns else 0
            investment_pct = (only_investment / total_active_for_patterns * 100) if total_active_for_patterns > 0 else 0
            
            alert_level = "info" if investment_pct > 3 else "medium"
            additional_insight = "High-value users - premium features" if investment_pct > 5 else "Investment-focused segment"
            
            only_investment_html = create_metric_card(
                "Only Investment Users",
                f"{only_investment:,}",
                f"{investment_pct:.1f}% of active users",
                "purple",
                "📈",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(only_investment_html, height=200)
        
        # Row 8: Feature Combinations Analysis
        st.subheader("🔄 Multiple Feature Combinations")
        
        # Fetch feature combinations data
        feature_combinations_df = fetch_feature_combinations(start_date, end_date)
        
        if not feature_combinations_df.empty:
            # Most popular combination metric card
            most_popular_combo = feature_combinations_df.iloc[0]
            combo_name = most_popular_combo['feature_combination']
            combo_count = most_popular_combo['user_count']
            combo_pct = most_popular_combo['percentage']
            
            col_combo1, col_combo2 = st.columns(2)
            
            with col_combo1:
                most_popular_html = create_metric_card(
                    f"Most Popular Combination",
                    f"{combo_count:,}",
                    f"{combo_name} ({combo_pct:.1f}% of multi-feature users)",
                    "purple",
                    "🔥",
                    alert_level="low",
                    additional_insight="Focus marketing on this combination"
                )
                components.html(most_popular_html, height=200)
            
            with col_combo2:
                # Show total combinations count
                total_combinations = len(feature_combinations_df)
                total_combinations_html = create_metric_card(
                    "Total Combinations",
                    f"{total_combinations}",
                    "Different feature combinations used",
                    "blue",
                    "🎭",
                    alert_level="info",
                    additional_insight="Shows user behavior diversity"
                )
                components.html(total_combinations_html, height=200)
            
            # Expandable table for feature combinations
            with st.expander("📋 View All Feature Combinations", expanded=False):
                st.markdown("**Feature Combination Breakdown for Multiple Feature Users**")
                
                # Style the dataframe
                styled_df = feature_combinations_df.copy()
                styled_df['feature_combination'] = styled_df['feature_combination'].str.replace(' + ', ' + ', regex=False)
                styled_df = styled_df.rename(columns={
                    'feature_combination': 'Feature Combination',
                    'user_count': 'Users',
                    'percentage': '% of Multi-Feature Users'
                })
                
                # Add color coding based on usage
                def highlight_rows(row):
                    if row.name == 0:  # Most popular
                        return ['background-color: #E8F5E8'] * len(row)
                    elif row['% of Multi-Feature Users'] > 15:  # High usage
                        return ['background-color: #F0F8FF'] * len(row)
                    else:
                        return [''] * len(row)
                
                styled_df = styled_df.style.apply(highlight_rows, axis=1)
                st.dataframe(styled_df, use_container_width=True)
                
                # Add insights about combinations
                if len(feature_combinations_df) > 1:
                    second_popular = feature_combinations_df.iloc[1]
                    st.info(f"💡 **Insight**: The top 2 combinations ({combo_name} and {second_popular['feature_combination']}) represent {combo_pct + second_popular['percentage']:.1f}% of all multi-feature users.")
        else:
            st.info("No multiple feature combinations found for the selected period.")
    # Generate and display insights
    retention_df = fetch_retention_metrics(start_date, end_date)
    insights = generate_insights(comprehensive_df, retention_df)

    if insights:
        st.subheader("💡 Retention Insights")
        for insight in insights:
            st.markdown(
                create_insight_card(
                    insight["title"],
                    insight["insight"],
                    insight["recommendation"],
                    insight["icon"],
                ),
                unsafe_allow_html=True,
            )

    # Cross-Feature Comparison removed per request
# Feature-specific tabs
for tab, feature, feature_name in [
    (tab2, "spending", "Spending"),
    (tab3, "lady_ai", "Lady AI"),
    (tab4, "savings", "Savings"),
    (tab5, "investment", "Investment"),
]:
    with tab:
        st.markdown('<div class="feature-section">', unsafe_allow_html=True)
        st.subheader(f"{feature_name} Deep Dive Analytics")

        # Fetch feature-specific metrics
        feature_metrics = fetch_feature_specific_metrics(start_date, end_date, feature)
        feature_retention = fetch_retention_metrics(start_date, end_date, feature)

        if not feature_metrics.empty:
            # Calculate stickiness
            avg_dau = (
                feature_metrics["avg_dau"][0]
                if not pd.isna(feature_metrics["avg_dau"][0])
                else 0
            )
            avg_wau = (
                feature_metrics["avg_wau"][0]
                if not pd.isna(feature_metrics["avg_wau"][0])
                else 0
            )
            avg_mau = (
                feature_metrics["avg_mau"][0]
                if not pd.isna(feature_metrics["avg_mau"][0])
                else 0
            )
            stickiness_ratio = avg_dau / avg_mau if avg_mau > 0 else 0

            # Get feature color
            feature_color = FEATURE_COLORS.get(feature, "navy")

            # Core metrics row
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown(
                    create_metric_card(
                        f"{feature_name} Active Users",
                        f"{feature_metrics['total_active_users'][0]:,}",
                        f"Total users active in {feature_name.lower()}",
                        feature_color,
                        "👥",
                    ),
                    unsafe_allow_html=True,
                )

            with col2:
                st.markdown(
                    create_metric_card(
                        "One Time Usage Users",
                        f"{feature_metrics['first_time_users'][0]:,}",
                        f"Users active on exactly one day in {feature_name.lower()}",
                        feature_color,
                        "📅",
                    ),
                    unsafe_allow_html=True,
                )

            with col3:
                st.markdown(
                    create_metric_card(
                        "Recurring Users",
                        f"{feature_metrics['recurring_users'][0]:,}",
                        f"Multi-day {feature_name.lower()} users",
                        feature_color,
                        "🔄",
                    ),
                    unsafe_allow_html=True,
                )

            with col4:
                st.markdown(
                    create_metric_card(
                        "Stickiness Ratio",
                        f"{stickiness_ratio:.2f}",
                        "DAU/MAU engagement ratio",
                        feature_color,
                        "🎯",
                    ),
                    unsafe_allow_html=True,
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
                        "📅",
                    ),
                    unsafe_allow_html=True,
                )

            with col6:
                st.markdown(
                    create_metric_card(
                        "Average WAU",
                        f"{avg_wau:,.0f}",
                        f"Weekly active {feature_name.lower()} users",
                        feature_color,
                        "📆",
                    ),
                    unsafe_allow_html=True,
                )

            with col7:
                st.markdown(
                    create_metric_card(
                        "Average MAU",
                        f"{avg_mau:,.0f}",
                        f"Monthly active {feature_name.lower()} users",
                        feature_color,
                        "🗓️",
                    ),
                    unsafe_allow_html=True,
                )

            # Retention metrics
            if not feature_retention.empty:
                st.subheader(f"🔄 {feature_name} Retention Analysis")
                col8, col9, col10 = st.columns(3)

                day1_ret = (
                    feature_retention["day1_retention"][0] * 100
                    if feature_retention["day1_retention"][0]
                    else 0
                )
                week1_ret = (
                    feature_retention["week1_retention"][0] * 100
                    if feature_retention["week1_retention"][0]
                    else 0
                )
                month1_ret = (
                    feature_retention["month1_retention"][0] * 100
                    if feature_retention["month1_retention"][0]
                    else 0
                )

                with col8:
                    st.markdown(
                        create_metric_card(
                            "Day 1 Retention",
                            f"{day1_ret:.1f}%",
                            "Users returning next day",
                            feature_color,
                            "📱",
                        ),
                        unsafe_allow_html=True,
                    )

                with col9:
                    st.markdown(
                        create_metric_card(
                            "Week 1 Retention",
                            f"{week1_ret:.1f}%",
                            "Users returning within a week",
                            feature_color,
                            "🗓️",
                        ),
                        unsafe_allow_html=True,
                    )

                with col10:
                    st.markdown(
                        create_metric_card(
                            "Month 1 Retention",
                            f"{month1_ret:.1f}%",
                            "Users returning within a month",
                            feature_color,
                            "📊",
                        ),
                        unsafe_allow_html=True,
                    )

            # Trend Charts
            st.subheader(f"📈 {feature_name} Usage Trends")
            dau_df, wau_df, mau_df = fetch_trend_data(start_date, end_date, feature)

            if not dau_df.empty:
                # Daily trend
                fig_dau = px.line(
                    dau_df,
                    x="activity_date",
                    y="dau",
                    title=f"{feature_name} Daily Active Users",
                    color_discrete_sequence=[LADDER_COLORS[feature_color]],
                )
                fig_dau.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Daily Active Users",
                    hovermode="x unified",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_dau, use_container_width=True)

            if not wau_df.empty:
                # Weekly trend
                fig_wau = px.bar(
                    wau_df,
                    x="week",
                    y="wau",
                    title=f"{feature_name} Weekly Active Users",
                    color_discrete_sequence=[LADDER_COLORS[feature_color]],
                )
                fig_wau.update_layout(
                    xaxis_title="Week",
                    yaxis_title="Weekly Active Users",
                    showlegend=False,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_wau, use_container_width=True)

            if not mau_df.empty and len(mau_df) > 1:
                # Monthly trend
                fig_mau = px.line(
                    mau_df,
                    x="month",
                    y="mau",
                    title=f"{feature_name} Monthly Active Users",
                    color_discrete_sequence=[LADDER_COLORS[feature_color]],
                    markers=True,
                )
                fig_mau.update_layout(
                    xaxis_title="Month",
                    yaxis_title="Monthly Active Users",
                    hovermode="x unified",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_mau, use_container_width=True)

        # Feature-specific insights
        feature_insights = generate_insights(
            feature_metrics, feature_retention, feature
        )
        if feature_insights:
            st.subheader(f"💡 {feature_name} Insights")
            for insight in feature_insights:
                st.markdown(
                    create_insight_card(
                        insight["title"],
                        insight["insight"],
                        insight["recommendation"],
                        insight["icon"],
                    ),
                    unsafe_allow_html=True,
                )

        st.markdown("</div>", unsafe_allow_html=True)

# FFP Engagement Dashboard Tab
with tab6:
    st.markdown('<div class="feature-section">', unsafe_allow_html=True)
    st.subheader("📋 Free Financial Plan (FFP) Engagement Dashboard")
    st.markdown(
        "Gain actionable insights into how users interact with the Free Financial Plan experience."
    )

    # Load FFP data
    ffp_df, feedback_df = load_ffp_data()

    if not ffp_df.empty:
        # Convert dates
        ffp_df["created_at"] = pd.to_datetime(ffp_df["created_at"])
        if not feedback_df.empty:
            feedback_df["created_at"] = pd.to_datetime(feedback_df["created_at"])

        # Apply date filter
        filtered_ffp = ffp_df[
            (ffp_df["created_at"].dt.date >= start_date)
            & (ffp_df["created_at"].dt.date <= end_date)
        ]
        filtered_feedback = (
            feedback_df[
                (feedback_df["created_at"].dt.date >= start_date)
                & (feedback_df["created_at"].dt.date <= end_date)
            ]
            if not feedback_df.empty
            else feedback_df
        )

        # FFP Metrics
        col1, col2 = st.columns(2)
        with col1:
            parsed_metadata = filtered_ffp["metadata"].apply(parse_ffp_metadata)
            total_completed = parsed_metadata.apply(
                lambda x: len([v for v in x.values() if v not in (None, "", [], {})])
            )
            completed_surveys = (total_completed == total_completed.max()).sum()
            st.markdown(
                create_metric_card(
                    "✅ Completed Surveys",
                    f"{completed_surveys:,}",
                    f"All questions completed ({start_date} to {end_date})",
                    "navy",
                    "📋",
                ),
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                create_metric_card(
                    "📥 Total Submissions",
                    f"{len(filtered_ffp):,}",
                    f"Total FFP submissions ({start_date} to {end_date})",
                    "navy",
                    "📊",
                ),
                unsafe_allow_html=True,
            )

        # Engagement Trends
        st.subheader("📊 Engagement Over Time and User Feedback")
        trend_df = (
            filtered_ffp.groupby(filtered_ffp["created_at"].dt.date)
            .size()
            .reset_index(name="Submissions")
        )
        trend_df = trend_df.rename(columns={"created_at": "Date"})

        col1, col2 = st.columns(2)
        with col1:
            st.write("### Daily Submissions")
            st.line_chart(trend_df.set_index("Date"))

        if not filtered_feedback.empty:
            reaction_counts = filtered_feedback["reaction"].value_counts()
            with col2:
                st.subheader("💬 User Reactions")
                st.bar_chart(reaction_counts)

            # User Comments
            st.subheader("💭 User Feedback")
            for _, row in filtered_feedback.iterrows():
                st.markdown(
                    f"- **{row['reaction'].capitalize()}** — {row['comment']} *(on {row['created_at'].date()})*"
                )
        else:
            st.info("No feedback data available for the selected period.")

    st.markdown("</div>", unsafe_allow_html=True)

# Customer Feature Analysis Tab
with tab7:
    st.markdown('<div class="feature-section">', unsafe_allow_html=True)
    st.subheader("🔍 Customer Feature Analysis Dashboard")
    st.markdown(
        "Identify and analyze users who have used features before but haven't been active recently. "
        "This helps in understanding user churn patterns and opportunities for re-engagement."
    )

    # Dormant Period Selection
    st.subheader("📅 Dormant Period Configuration")
    col_config1, col_config2 = st.columns(2)
    
    with col_config1:
        dormant_period_options = {
            "1 Week": 7,
            "2 Weeks": 14,
            "1 Month": 30,
            "2 Months": 60,
            "3 Months": 90,
            "Custom": None
        }
        
        dormant_choice = st.selectbox(
            "⏰ Dormant Period", 
            list(dormant_period_options.keys()), 
            index=2,  # Default to 1 Month
            help="Users who used features before this period but not during the analysis period"
        )
    
    with col_config2:
        if dormant_choice == "Custom":
            dormant_period_days = st.number_input(
                "Custom Dormant Period (Days)", 
                min_value=1, 
                max_value=365, 
                value=30,
                help="Number of days to look back for historical usage"
            )
        else:
            dormant_period_days = dormant_period_options[dormant_choice]

    # Fetch dormant users analysis
    dormant_analysis = fetch_dormant_users_analysis(start_date, end_date, dormant_period_days)
    
    if not dormant_analysis.empty:
        # Overall Dormant Users Section
        st.subheader("📊 Overall Dormant Users Analysis")
        
        col_overall1, col_overall2, col_overall3 = st.columns(3)
        
        with col_overall1:
            overall_dormant = dormant_analysis['overall_dormant_users'][0]
            total_historical = dormant_analysis['total_historical_users'][0]
            dormant_percentage = (overall_dormant / total_historical * 100) if total_historical > 0 else 0
            
            # Determine alert level
            alert_level = "high" if dormant_percentage > 40 else "medium" if dormant_percentage > 20 else "low"
            additional_insight = f"Critical: {dormant_percentage:.1f}% of historical users are dormant" if dormant_percentage > 40 else f"Moderate: {dormant_percentage:.1f}% dormant users" if dormant_percentage > 20 else f"Good: Only {dormant_percentage:.1f}% dormant users"
            
            overall_html = create_metric_card(
                "Overall Dormant Users",
                f"{overall_dormant:,}",
                f"{dormant_percentage:.1f}% of {total_historical:,} historical users",
                "red" if dormant_percentage > 40 else "orange" if dormant_percentage > 20 else "green",
                "😴",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(overall_html, height=200)
        
        with col_overall2:
            total_current = dormant_analysis['total_current_users'][0]
            reactivation_rate = ((total_current - overall_dormant) / total_historical * 100) if total_historical > 0 else 0
            
            alert_level = "low" if reactivation_rate > 60 else "medium" if reactivation_rate > 40 else "high"
            additional_insight = f"Strong user retention" if reactivation_rate > 60 else f"Moderate retention" if reactivation_rate > 40 else f"Focus on re-engagement"
            
            reactivation_html = create_metric_card(
                "User Reactivation Rate",
                f"{reactivation_rate:.1f}%",
                f"Active users from historical base",
                "green" if reactivation_rate > 60 else "orange" if reactivation_rate > 40 else "red",
                "🔄",
                alert_level=alert_level,
                additional_insight=additional_insight
            )
            components.html(reactivation_html, height=200)
        
        with col_overall3:
            # Churn count based on selected date range
            churn_count = fetch_churn_count(start_date, end_date)
            churn_html = create_metric_card(
                "User Churn",
                f"{churn_count:,}",
                f"Users with no activity between {start_date} and {end_date}",
                "red",
                "📉"
            )
            components.html(churn_html, height=200)

        # Feature-Specific Dormant Users Section
        st.subheader("🎯 Feature-Specific Dormant Users")
        
        # Create feature-specific metric cards
        col_feature1, col_feature2, col_feature3, col_feature4 = st.columns(4)
        
        features_data = [
            ("spending", "Spending", "blue", "💰"),
            ("savings", "Savings", "green", "🏦"),
            ("investment", "Investment", "purple", "📈"),
            ("lady_ai", "Lady AI", "orange", "🤖")
        ]
        
        for i, (feature_key, feature_name, color, icon) in enumerate(features_data):
            with [col_feature1, col_feature2, col_feature3, col_feature4][i]:
                dormant_count = dormant_analysis[f'{feature_key}_dormant_users'][0]
                
                # Calculate percentage of total dormant users
                dormant_pct = (dormant_count / overall_dormant * 100) if overall_dormant > 0 else 0
                
                alert_level = "high" if dormant_pct > 30 else "medium" if dormant_pct > 15 else "low"
                additional_insight = f"High {feature_name.lower()} churn" if dormant_pct > 30 else f"Moderate {feature_name.lower()} churn" if dormant_pct > 15 else f"Low {feature_name.lower()} churn"
                
                feature_html = create_metric_card(
                    f"{feature_name} Dormant Users",
                    f"{dormant_count:,}",
                    f"{dormant_pct:.1f}% of total dormant users",
                    color,
                    icon,
                    alert_level=alert_level,
                    additional_insight=additional_insight
                )
                components.html(feature_html, height=200)

        # Dormant Users Trend Analysis
        st.subheader("📈 Dormant Users Trend Over Time")
        
        # Fetch trend data
        dormant_trend = fetch_dormant_users_trend(start_date, end_date, dormant_period_days)
        
        if not dormant_trend.empty:
            # Create trend chart
            fig_trend = px.line(
                dormant_trend,
                x="activity_date",
                y="dormant_count",
                title=f"Daily Dormant Users Count ({dormant_period_days}-day lookback)",
                color_discrete_sequence=[LADDER_COLORS["red"]],
                markers=True
            )
            fig_trend.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Dormant Users",
                hovermode="x unified",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_trend, use_container_width=True)
            
            # Trend insights
            if len(dormant_trend) > 1:
                trend_change = dormant_trend['dormant_count'].iloc[-1] - dormant_trend['dormant_count'].iloc[0]
                trend_direction = "increasing" if trend_change > 0 else "decreasing" if trend_change < 0 else "stable"
                
                st.info(f"📊 **Trend Insight**: Dormant users are {trend_direction} by {abs(trend_change):,} users over the analysis period.")
        else:
            st.info("No trend data available for the selected period.")

        # Recommendations Section
        st.subheader("💡 Re-engagement Recommendations")
        
        # Generate recommendations based on the data
        recommendations = []
        
        if dormant_percentage > 30:
            recommendations.append({
                "title": "High Dormant User Rate",
                "insight": f"{dormant_percentage:.1f}% of historical users are dormant",
                "recommendation": "Implement aggressive re-engagement campaigns including email sequences, push notifications, and personalized offers",
                "icon": "🚨"
            })
        
        # Feature-specific recommendations
        for feature_key, feature_name, _, _ in features_data:
            dormant_count = dormant_analysis[f'{feature_key}_dormant_users'][0]
            dormant_pct = (dormant_count / overall_dormant * 100) if overall_dormant > 0 else 0
            
            if dormant_pct > 25:
                recommendations.append({
                    "title": f"High {feature_name} Churn",
                    "insight": f"{dormant_pct:.1f}% of dormant users were {feature_name} users",
                    "recommendation": f"Focus on {feature_name} feature improvements and targeted re-engagement for {feature_name} users",
                    "icon": "🎯"
                })
        
        if not recommendations:
            recommendations.append({
                "title": "Good User Retention",
                "insight": "Dormant user rates are within acceptable ranges",
                "recommendation": "Continue current retention strategies and monitor for any changes",
                "icon": "✅"
            })
        
        # Display recommendations
        for rec in recommendations:
            st.markdown(
                create_insight_card(
                    rec["title"],
                    rec["insight"],
                    rec["recommendation"],
                    rec["icon"]
                ),
                unsafe_allow_html=True
            )

        # Detailed Analysis Table
        with st.expander("📋 Detailed Dormant Users Breakdown", expanded=False):
            st.markdown("**Comprehensive Dormant Users Analysis**")
            
            # Create detailed breakdown table
            breakdown_data = {
                'Feature': ['Overall', 'Spending', 'Savings', 'Investment', 'Lady AI'],
                'Dormant Users': [
                    dormant_analysis['overall_dormant_users'][0],
                    dormant_analysis['spending_dormant_users'][0],
                    dormant_analysis['savings_dormant_users'][0],
                    dormant_analysis['investment_dormant_users'][0],
                    dormant_analysis['lady_ai_dormant_users'][0]
                ],
                'Percentage of Total Dormant': [
                    100.0,
                    (dormant_analysis['spending_dormant_users'][0] / dormant_analysis['overall_dormant_users'][0] * 100) if dormant_analysis['overall_dormant_users'][0] > 0 else 0,
                    (dormant_analysis['savings_dormant_users'][0] / dormant_analysis['overall_dormant_users'][0] * 100) if dormant_analysis['overall_dormant_users'][0] > 0 else 0,
                    (dormant_analysis['investment_dormant_users'][0] / dormant_analysis['overall_dormant_users'][0] * 100) if dormant_analysis['overall_dormant_users'][0] > 0 else 0,
                    (dormant_analysis['lady_ai_dormant_users'][0] / dormant_analysis['overall_dormant_users'][0] * 100) if dormant_analysis['overall_dormant_users'][0] > 0 else 0
                ]
            }
            
            breakdown_df = pd.DataFrame(breakdown_data)
            breakdown_df['Percentage of Total Dormant'] = breakdown_df['Percentage of Total Dormant'].round(1)
            
            # Style the dataframe
            def highlight_dormant_rows(row):
                if row['Dormant Users'] == max(breakdown_df['Dormant Users']):
                    return ['background-color: #FFE6E6'] * len(row)
                elif row['Dormant Users'] > breakdown_df['Dormant Users'].mean():
                    return ['background-color: #FFF2E6'] * len(row)
                else:
                    return [''] * len(row)
            
            styled_breakdown = breakdown_df.style.apply(highlight_dormant_rows, axis=1)
            st.dataframe(styled_breakdown, use_container_width=True)
            
            # Summary insights
            st.info(f"📊 **Analysis Summary**: {dormant_analysis['overall_dormant_users'][0]:,} users became dormant during the analysis period. "
                   f"The feature with the highest dormant user count is {breakdown_df.loc[breakdown_df['Dormant Users'].idxmax(), 'Feature']}.")

    else:
        st.warning("No dormant users data available for the selected period and configuration.")

    st.markdown("</div>", unsafe_allow_html=True)

# Footer with summary stats
st.markdown(
    f"""
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
""",
    unsafe_allow_html=True,
)

