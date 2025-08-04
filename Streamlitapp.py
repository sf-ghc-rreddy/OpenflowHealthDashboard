import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

# --- Page Configuration & Custom Theme ---
st.set_page_config(
    page_title="Openflow Health Dashboard",
    layout="wide",
)

# A custom, professional light theme.
custom_theme = """
<style>
    /* Main app background */
    .stApp {
        background-color: #F0F2F6;
    }
    /* Main title styling */
    h1 {
        color: #1E293B;
    }
    /* Sub-header styling */
    h2 {
        color: #334155;
    }
    /* Primary text color for readability */
    p, .st-emotion-cache-1y4p8pa, .st-emotion-cache-q82iup, .st-emotion-cache-16idsys p {
        color: #475569;
    }
    /* Container/Card styling */
    .st-emotion-cache-1r4qj8v {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 0.75rem;
        padding: 1.5rem !important;
        box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
    }
    /* Sidebar styling */
    .st-emotion-cache-1629p8f {
        background-color: #FFFFFF;
        border-right: 1px solid #E2E8F0;
    }
    /* Selected tab styling */
    .st-emotion-cache-1ht1bi .st-emotion-cache-12fmjuu {
        background-color: #F8FAFC;
    }
</style>
"""
st.markdown(custom_theme, unsafe_allow_html=True)
alt.themes.enable('default')

# --- App Header ---
SNOWFLAKE_LOGO_URL = "https://i.imgur.com/h31nI6I.png"
st.markdown(f"""
<div style="display: flex; justify-content: center; align-items: center; text-align: center; margin-bottom: 20px;">
    <div>
        <h1 style='margin-bottom: 0;'>Openflow Health Dashboard</h1>
        <p style='margin-top: 0; color:#475569;'>Your mission control for monitoring the health, performance, and operational status of your Openflow pipelines.</p>
    </div>
</div>
""", unsafe_allow_html=True)


# --- Snowflake Connection & Caching ---
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Could not get Snowflake session. Error: {e}")
    st.stop()
    
EVENTS_TABLE = 'openflow.telemetry.events'

@st.cache_data(ttl=60)
def run_query(query: str) -> pd.DataFrame:
    try:
        df = session.sql(query).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        print(f"Error running query: {query}\nException: {e}")
        return pd.DataFrame()

if st.sidebar.button("Clear Cache"):
    st.cache_data.clear()
        
# --- Sidebar Filters ---
with st.sidebar:
    st.header("⚙️ Dashboard Filters")
    st.caption("Use these filters to control the scope of the entire dashboard.")
    time_window_options = {"Last Hour": 1, "Last 6 Hours": 6, "Last 24 Hours": 24, "Last 7 Days": 168}
    selected_time_label = st.selectbox("Select Time Window", options=list(time_window_options.keys()), index=2)
    time_window_hours = time_window_options[selected_time_label]

    # CORRECTED QUERY: Changed alias to RUNTIME for consistency and made the WHERE clause more robust.
    runtime_query = f"""
        SELECT DISTINCT resource_attributes:"k8s.namespace.name"::STRING AS RUNTIME
        FROM {EVENTS_TABLE}
        WHERE RUNTIME IS NOT NULL AND timestamp > DATEADD(hour, -24, SYSDATE())
    """
    all_runtimes_df_for_filter = run_query(runtime_query)

    show_internal_runtimes = st.toggle("Show internal runtimes", value=False)
    
    # Check if the DataFrame and column exist before processing
    if not all_runtimes_df_for_filter.empty and 'RUNTIME' in all_runtimes_df_for_filter.columns:
        if show_internal_runtimes:
            # CORRECTED ACCESS: Use the 'RUNTIME' column
            runtimes_to_show = all_runtimes_df_for_filter['RUNTIME'].tolist()
        else:
            # CORRECTED ACCESS: Use the 'RUNTIME' column
            runtimes_to_show = [r for r in all_runtimes_df_for_filter['RUNTIME'].tolist() if r and r.startswith('runtime-')]
        
        all_runtimes_list = ['All Runtimes'] + sorted(runtimes_to_show)
        selected_runtimes = st.multiselect("Filter by Runtime(s)", options=all_runtimes_list, default=['All Runtimes'])
    else:
        # Fallback if the query returns no data
        st.warning("No runtimes found to filter by.")
        selected_runtimes = []

    st.divider()

    with st.expander("🧭 How to Use This Dashboard", expanded=True):
        st.write("This dashboard guides you from a high-level overview to the root cause of an issue. **Follow the tabs from left to right** for a typical investigation workflow.")
        st.markdown("""
        - **👋 Mission Control:** Start here for an automated summary of critical issues that need your immediate attention.
        - **❤️‍🩹 Pipeline Health Pulse:** Get a pulse on error and performance trends for your user-created runtimes.
        - **🔍 Root Cause Analysis:** If you see errors, come here to find which runtime and which processor is failing.
        - **🚀 Performance Bottleneck Analyzer:** Investigate slow pipelines by looking for data pile-ups (backpressure) and wait times.
        - **🗓️ Anomaly Tracker:** Discover if your errors are happening at specific, recurring times.
        - **🌳 Runtime Status:** Get a complete inventory of all your runtimes and see if they are online and active.
        """)

    runtime_filter_clause = ""
    if 'All Runtimes' not in selected_runtimes and selected_runtimes:
        runtime_list_str = "', '".join(selected_runtimes)
        runtime_filter_clause = f"AND resource_attributes:\"k8s.namespace.name\"::STRING IN ('{runtime_list_str}')"


# --- Main Dashboard Tabs ---
tab_list = ["👋 Mission Control", "❤️‍🩹 Pipeline Health Pulse", "🔍 Root Cause Analysis", "🚀 Performance Bottleneck Analyzer", "🗓️ Anomaly Tracker", "🌳 Runtime Status"]
tabs = st.tabs(tab_list)

#==============================================================================
# TAB 1: Mission Control
#==============================================================================
with tabs[0]:
    st.markdown("<h2 style='text-align: center;'>Mission Control</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This page provides a proactive summary of your environment's health. If any immediate action is needed, it will be listed below.")
        
        findings = []
        hf_error_query = f"SELECT resource_attributes:\"k8s.namespace.name\"::STRING as RUNTIME, COUNT(*) as ERROR_COUNT FROM {EVENTS_TABLE} WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(minute, -30, CURRENT_TIMESTAMP()) {runtime_filter_clause} GROUP BY 1 HAVING ERROR_COUNT > 5;"
        hf_error_df = run_query(hf_error_query)
        for _, row in hf_error_df.iterrows():
            findings.append({"type": "error", "icon": "🔥", "title": "High Error Rate Detected", "text": f"Runtime **{row['RUNTIME']}** has produced **{row['ERROR_COUNT']} errors** in the last 30 minutes.", "action": "Navigate to the **Root Cause Analysis** tab and select this runtime to investigate.", "urgency": "High (Investigate Now)"})
        stopped_processor_query = f"""WITH latest_status AS (SELECT resource_attributes:"k8s.namespace.name"::STRING as RUNTIME, record_attributes:name::STRING as PROCESSOR, value as status, ROW_NUMBER() OVER (PARTITION BY RUNTIME, PROCESSOR ORDER BY timestamp DESC) as rn FROM {EVENTS_TABLE} WHERE record:metric:name = 'processor.run.status.running' AND timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP()) {runtime_filter_clause}) SELECT RUNTIME, COUNT(*) as STOPPED_COUNT FROM latest_status WHERE rn = 1 AND status = 0 GROUP BY RUNTIME;"""
        stopped_processor_df = run_query(stopped_processor_query)
        for _, row in stopped_processor_df.iterrows():
            findings.append({"type": "warning", "icon": "🛑", "title": "Stopped Processors Found", "text": f"Runtime **{row['RUNTIME']}** has **{row['STOPPED_COUNT']} processor(s)** in a stopped state.", "action": "Verify if this is intentional. If not, check the runtime's flow configuration to restart the processor(s).", "urgency": "Medium (Review Soon)"})
        
        if not findings:
            st.success("✅ All Systems Nominal. No critical issues detected in the last 30 minutes.", icon="✅")
        else:
            st.error(f"🚨 {len(findings)} Urgent Action Item(s) Detected!", icon="🚨")
            st.markdown("---")
            num_findings = len(findings)
            if num_findings > 0:
                num_columns = min(num_findings, 3)
                cols = st.columns(num_columns)
                for i, finding in enumerate(findings):
                    with cols[i % num_columns]:
                        message = f"""
                        **{i+1}. {finding['title']}**
                        
                        {finding['text']}
                        
                        **Urgency:** {finding['urgency']}
                        
                        **Action:** {finding['action']}
                        """
                        if finding['type'] == 'error':
                            st.error(message, icon=finding['icon'])
                        else:
                            st.warning(message, icon=finding['icon'])

#==============================================================================
# TAB 2: Pipeline Health Pulse
#==============================================================================
with tabs[1]:
    st.markdown("<h2 style='text-align: center;'>Pipeline Health Pulse</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This tab shows the overall trend of errors and performance delays for your **user-created runtimes**. Each line on the chart represents a different runtime, making it easy to compare them.")
        time_bucket = 'hour' if time_window_hours <= 48 else 'day'
        kpi_query = f"""
        SELECT DATE_TRUNC('{time_bucket}', timestamp) as TIME_BUCKET, resource_attributes:"k8s.namespace.name"::STRING as RUNTIME,
            COUNT_IF(TRY_PARSE_JSON(value):level = 'ERROR') AS TOTAL_ERRORS,
            MAX(IFF(record:metric:name = 'connection.queued.duration.max', value / 60000, 0)) AS MAX_QUEUE_MINUTES
        FROM {EVENTS_TABLE} WHERE record_type IN ('LOG', 'METRIC')
          AND resource_attributes:"k8s.namespace.name"::STRING LIKE 'runtime-%'
          AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {runtime_filter_clause}
        GROUP BY 1, 2 ORDER BY 1;"""
        kpi_df = run_query(kpi_query)
        if not kpi_df.empty:
            kpi_df['TIME_BUCKET'] = pd.to_datetime(kpi_df['TIME_BUCKET'])
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.subheader("Error Trends by Runtime")
                st.caption("For a data engineer, this chart helps you instantly see if a recent code deployment or configuration change in a specific runtime has introduced new errors.")
                error_chart = alt.Chart(kpi_df).mark_line(point=True).encode(x=alt.X('TIME_BUCKET:T', title=f"Time ({time_bucket.capitalize()})"), y=alt.Y('TOTAL_ERRORS:Q', title='Total Errors'), color=alt.Color('RUNTIME:N', title='Runtime'), tooltip=['TIME_BUCKET', 'RUNTIME', 'TOTAL_ERRORS']).properties(height=300).interactive()
                st.altair_chart(error_chart, use_container_width=True)
            with chart_col2:
                st.subheader("Queue Time Trends by Runtime")
                st.caption("This chart shows processing delays. A rising line indicates a runtime's downstream systems (like databases or APIs) are slowing down, which could impact data freshness SLAs.")
                queue_chart = alt.Chart(kpi_df).mark_line(point=True).encode(x=alt.X('TIME_BUCKET:T', title=f"Time ({time_bucket.capitalize()})"), y=alt.Y('MAX_QUEUE_MINUTES:Q', title='Max Queue Time (Min)'), color=alt.Color('RUNTIME:N', title='Runtime'), tooltip=['TIME_BUCKET', 'RUNTIME', 'MAX_QUEUE_MINUTES']).properties(height=300).interactive()
                st.altair_chart(queue_chart, use_container_width=True)
        else:
            st.info("No data found for user-created runtimes in the selected time window.")
        with st.expander("💡 Recommended Actions"):
            st.markdown("- **Analyze Spikes:** If you see a runtime with a high spike, **note the time and runtime name**.\n- **Next Step:** For error spikes, go to **Root Cause Analysis**. For queue time spikes, go to **Performance Bottleneck Analyzer**.")

#==============================================================================
# TAB 3: Root Cause Analysis
#==============================================================================
with tabs[2]:
    st.markdown("<h2 style='text-align: center;'>Root Cause Analysis</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This tab helps you find the source of errors. Start by identifying the top failing runtime, then drill down to the specific processor and error message.")
        st.subheader("Top 10 Error-Producing Runtimes")
        st.caption("This bar chart is your 'hit list' for debugging. Start with the runtime at the top to make the biggest impact on overall pipeline stability.")
        error_runtime_query = f"""SELECT resource_attributes:"k8s.namespace.name"::STRING AS "RUNTIME", COUNT(*) AS "ERROR_COUNT"
        FROM {EVENTS_TABLE} WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {runtime_filter_clause}
        GROUP BY 1 HAVING "ERROR_COUNT" > 0 ORDER BY "ERROR_COUNT" DESC LIMIT 10;"""
        error_runtime_df = run_query(error_runtime_query)
        if not error_runtime_df.empty:
            chart = alt.Chart(error_runtime_df).mark_bar().encode(x=alt.X('ERROR_COUNT:Q', title='Error Count'), y=alt.Y('RUNTIME:N', title='Runtime', sort='-x', axis=alt.Axis(labelLimit=200)), tooltip=['RUNTIME', 'ERROR_COUNT'])
            st.altair_chart(chart, use_container_width=True)
            st.divider()
            
            st.subheader("Drill-Down Analysis Workflow")
            dcol1, dcol2 = st.columns([1, 1], gap="large")
            with dcol1:
                st.caption("Follow these steps to find the root cause of an error.")
                selected_runtime = st.selectbox("Step 1: Select a Runtime to Inspect", options=error_runtime_df['RUNTIME'].tolist())
                processor_filter_clause, time_filter_clause = "", ""
                if selected_runtime:
                    processor_error_query = f"""SELECT TRY_PARSE_JSON(value):loggerName::STRING as "PROCESSOR", COUNT(*) as "ERROR_COUNT" FROM {EVENTS_TABLE}
                    WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) AND resource_attributes:"k8s.namespace.name"::STRING = '{selected_runtime}'
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"""
                    processor_error_df = run_query(processor_error_query)
                    
                    if not processor_error_df.empty:
                        top_processors = processor_error_df['PROCESSOR'].tolist()
                        selected_processor = st.selectbox("Step 2: Select a Processor to Filter", options=['All Processors'] + top_processors)
                        if selected_processor != 'All Processors':
                            processor_filter_clause = f"AND TRY_PARSE_JSON(value):loggerName::STRING = '{selected_processor}'"
                    else:
                        st.info("No processor-specific errors found.")
                        selected_processor = "All Processors"
            with dcol2:
                st.write("Processor Error Distribution")
                st.caption("This helps you pinpoint the exact faulty component within the failing runtime.")
                if selected_runtime and not processor_error_df.empty:
                    processor_error_df['% OF TOTAL'] = (processor_error_df['ERROR_COUNT'] / processor_error_df['ERROR_COUNT'].sum() * 100)
                    st.dataframe(processor_error_df, use_container_width=True, hide_index=True, column_config={"% OF TOTAL": st.column_config.ProgressColumn("Share", format="%.1f%%", min_value=0, max_value=100)})
                else:
                    st.info("Select a runtime with errors to see processor distribution.")

            st.divider()
            st.caption("Step 3: Refine your search by time and message content.")
            if selected_runtime:
                time_range_query = f"""SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts FROM {EVENTS_TABLE} WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) AND resource_attributes:"k8s.namespace.name"::STRING = '{selected_runtime}' {processor_filter_clause};"""
                time_range_df = run_query(time_range_query)
                if not time_range_df.empty and time_range_df['MIN_TS'][0] is not pd.NaT:
                    min_ts, max_ts = pd.to_datetime(time_range_df['MIN_TS'][0]).to_pydatetime(), pd.to_datetime(time_range_df['MAX_TS'][0]).to_pydatetime()
                    if min_ts == max_ts: max_ts = min_ts + timedelta(seconds=1)
                    selected_time_range = st.slider("Filter by Time Range", min_value=min_ts, max_value=max_ts, value=(min_ts, max_ts), format="MM/DD/YY - hh:mm a")
                    time_filter_clause = f"AND timestamp BETWEEN '{selected_time_range[0]}' AND '{selected_time_range[1]}'"
            
            log_search_term = st.text_input("Search for a specific message:")
            
            if selected_runtime:
                error_log_query = f"""SELECT timestamp, TRY_PARSE_JSON(value):loggerName::STRING as processor, TRY_PARSE_JSON(value):formattedMessage::STRING AS message FROM {EVENTS_TABLE} 
                WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND resource_attributes:"k8s.namespace.name"::STRING = '{selected_runtime}'
                {processor_filter_clause} {time_filter_clause} AND message ILIKE '%{log_search_term}%'
                ORDER BY timestamp DESC LIMIT 200;"""
                error_log_df = run_query(error_log_query)
                st.dataframe(error_log_df, use_container_width=True, height=300)
        else:
            st.success("No errors found for the selected filters. ✅")

#==============================================================================
# TAB 4: Performance Bottleneck Analyzer
#==============================================================================
with tabs[3]:
    st.markdown("<h2 style='text-align: center;'>Performance Bottleneck Analyzer</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This tab helps you find performance bottlenecks. Use the sliders to define what you consider a problem.")
        col1, col2 = st.columns(2)
        with col1: backpressure_threshold = st.slider("Min Backpressure to Show (MiB)", 0, 500, 0)
        with col2: queue_time_threshold = st.slider("Min Queue Time to Show (Minutes)", 0, 60, 0)
        
        bcol1, bcol2 = st.columns(2)
        with bcol1:
            st.subheader(f"Backpressure > {backpressure_threshold} MiB")
            st.caption("This shows connections overwhelmed by data volume. The problem is the amount of data, not necessarily the speed of the next component.")
            backpressure_query = f"""SELECT resource_attributes:"k8s.namespace.name" || ' | ' || record_attributes:name::STRING as "RUNTIME_CONNECTION", MAX(value) / (1024*1024) as "Peak Queued (MiB)"
            FROM {EVENTS_TABLE} WHERE record_type = 'METRIC' AND record:metric:name = 'connection.queued.bytes' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {runtime_filter_clause}
            GROUP BY 1 HAVING "Peak Queued (MiB)" > {backpressure_threshold} ORDER BY "Peak Queued (MiB)" DESC LIMIT 15;"""
            backpressure_df = run_query(backpressure_query)
            if not backpressure_df.empty: st.bar_chart(backpressure_df.set_index("RUNTIME_CONNECTION"))
            else: st.info("No connections exceeded the backpressure threshold.")
        with bcol2:
            st.subheader(f"Queue Times > {queue_time_threshold} Minutes")
            st.caption("This shows connections experiencing long delays. The problem is the speed of the next component, which can't process data fast enough.")
            queue_time_query = f"""SELECT resource_attributes:"k8s.namespace.name" || ' | ' || record_attributes:name::STRING as "RUNTIME_CONNECTION", MAX(value / 60000) as "Max Queue Time (Min)"
            FROM {EVENTS_TABLE} WHERE record_type = 'METRIC' AND record:metric:name = 'connection.queued.duration.max' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {runtime_filter_clause}
            GROUP BY 1 HAVING "Max Queue Time (Min)" > {queue_time_threshold} ORDER BY "Max Queue Time (Min)" DESC LIMIT 15;"""
            queue_time_df = run_query(queue_time_query)
            if not queue_time_df.empty: st.bar_chart(queue_time_df.set_index("RUNTIME_CONNECTION"))
            else: st.info("No connections exceeded the queue time threshold.")
        with st.expander("💡 Recommended Actions"):
            st.markdown("- **Analyze Bottlenecks:** For connections at the top of these charts, identify the processor that *consumes* data from that connection.\n- **Scale or Fix:** The consuming processor may need more resources, or it may be failing. **Check the Root Cause Analysis tab** for corresponding errors.")

#==============================================================================
# TAB 5: Anomaly Tracker
#==============================================================================
with tabs[4]:
    st.markdown("<h2 style='text-align: center;'>Anomaly Tracker</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This tab helps you discover recurring, time-based error patterns.")
        
        st.subheader("Filter Heatmap by Runtime")
        heatmap_runtime_query = f"SELECT DISTINCT resource_attributes:\"k8s.namespace.name\"::STRING as RUNTIME FROM {EVENTS_TABLE} WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {runtime_filter_clause};"
        heatmap_runtimes_df = run_query(heatmap_runtime_query)
        if not heatmap_runtimes_df.empty:
            selected_runtime_for_heatmap = st.selectbox("Select a Runtime to visualize its error pattern", options=['All Selected Runtimes'] + heatmap_runtimes_df['RUNTIME'].tolist())
            heatmap_filter_clause = runtime_filter_clause
            if selected_runtime_for_heatmap != 'All Selected Runtimes':
                heatmap_filter_clause = f"AND resource_attributes:\"k8s.namespace.name\"::STRING = '{selected_runtime_for_heatmap}'"

            heatmap_query = f"""
            WITH all_cells AS (
                SELECT d.day_name as "DAY_OF_WEEK", h.hour as "HOUR_OF_DAY"
                FROM (SELECT seq4()-1 as hour from table(generator(rowcount => 24))) h
                CROSS JOIN (SELECT DECODE(seq4(), 1, 'Mon', 2, 'Tue', 3, 'Wed', 4, 'Thu', 5, 'Fri', 6, 'Sat', 7, 'Sun') as day_name FROM table(generator(rowcount => 7))) d
            ), error_counts AS (
                SELECT EXTRACT(hour from timestamp) as "HOUR_OF_DAY", DECODE(EXTRACT(dayofweekiso from timestamp), 1, 'Mon', 2, 'Tue', 3, 'Wed', 4, 'Thu', 5, 'Fri', 6, 'Sat', 7, 'Sun') as "DAY_OF_WEEK", COUNT(*) as "ERROR_COUNT"
                FROM {EVENTS_TABLE} WHERE record_type = 'LOG' AND TRY_PARSE_JSON(value):level = 'ERROR' AND timestamp > DATEADD(hour, -{time_window_hours}, CURRENT_TIMESTAMP()) {heatmap_filter_clause} GROUP BY 1, 2
            )
            SELECT a."DAY_OF_WEEK", a."HOUR_OF_DAY", ZEROIFNULL(e."ERROR_COUNT") as "ERROR_COUNT"
            FROM all_cells a LEFT JOIN error_counts e ON a."DAY_OF_WEEK" = e."DAY_OF_WEEK" AND a."HOUR_OF_DAY" = e."HOUR_OF_DAY";"""
            heatmap_df = run_query(heatmap_query)
            if not heatmap_df.empty:
                day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                heatmap_chart = alt.Chart(heatmap_df).mark_rect().encode(
                    x=alt.X('HOUR_OF_DAY:O', title='Hour of Day'), y=alt.Y('DAY_OF_WEEK:O', title='Day of Week', sort=day_order),
                    color=alt.condition(alt.datum.ERROR_COUNT > 0, alt.Color('ERROR_COUNT:Q', scale=alt.Scale(scheme='yelloworangered'), title='Error Count'), alt.value('#C8E6C9')),
                    tooltip=['HOUR_OF_DAY', 'DAY_OF_WEEK', 'ERROR_COUNT']
                ).properties(title=f"Error Concentration for {selected_runtime_for_heatmap}")
                st.altair_chart(heatmap_chart, use_container_width=True)
            else:
                st.warning("Could not generate heatmap data.")
        else:
            st.info("No errors found in the selected time window to generate a heatmap.")
        with st.expander("💡 Recommended Actions"):
            st.markdown("- **Identify Patterns:** Look for red/orange squares. A consistent pattern suggests an issue with a scheduled job.\n- **Check Schedulers:** Investigate external job schedulers (like Airflow) for jobs that run at these problematic times.")


#==============================================================================
# TAB 6: Runtime Status
#==============================================================================
with tabs[5]:
    st.markdown("<h2 style='text-align: center;'>Runtime Status</h2>", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("This view provides an inventory of all known runtimes and their reporting status, helping you quickly spot systems that might be down or misconfigured.")
        col1, col2 = st.columns([3, 1])
        with col1: inactivity_hours = st.slider("Set Inactivity Threshold (Hours)", 1, 168, 24, key="runtime_status_slider")
        with col2: show_user_only_status = st.toggle("Show User Created Runtimes Only", value=True, key="runtime_status_toggle")
        
        all_known_runtimes_query = f"SELECT resource_attributes:\"k8s.namespace.name\"::STRING AS NAME, MIN(timestamp) as CREATED_ON FROM {EVENTS_TABLE} WHERE NAME IS NOT NULL GROUP BY 1;"
        all_known_runtimes_df = run_query(all_known_runtimes_query)
        if not all_known_runtimes_df.empty:
            active_runtimes_query = f"SELECT DISTINCT resource_attributes:\"k8s.namespace.name\"::STRING AS ACTIVE_RUNTIME_NAME FROM {EVENTS_TABLE} WHERE timestamp > DATEADD(hour, -{inactivity_hours}, CURRENT_TIMESTAMP());"
            active_runtimes_df = run_query(active_runtimes_query)
            all_known_runtimes_df['TYPE'] = all_known_runtimes_df['NAME'].apply(lambda x: 'User Created' if str(x).startswith('runtime-') else 'Internal')
            all_known_runtimes_df['IS_ACTIVE'] = all_known_runtimes_df['NAME'].isin(active_runtimes_df['ACTIVE_RUNTIME_NAME'])
            all_known_runtimes_df['STATUS'] = all_known_runtimes_df['IS_ACTIVE'].apply(lambda x: '🟢 Active' if x else '🔴 Inactive')
            last_event_query = f"SELECT resource_attributes:\"k8s.namespace.name\"::STRING as NAME, MAX(timestamp) as LAST_EVENT_TIME FROM {EVENTS_TABLE} WHERE NAME IS NOT NULL GROUP BY 1;"
            last_event_df = run_query(last_event_query)
            final_status_df = pd.merge(all_known_runtimes_df, last_event_df, on='NAME', how='left')
            if show_user_only_status: final_status_df = final_status_df[final_status_df['TYPE'] == 'User Created'].copy()
            total_count, active_count = len(final_status_df), final_status_df['IS_ACTIVE'].sum()
            mcol1, mcol2, mcol3 = st.columns(3)
            mcol1.metric("Total Displayed Runtimes", total_count)
            mcol2.metric("Active Runtimes", active_count)
            mcol3.metric("Inactive Runtimes", total_count - active_count)
            st.dataframe(final_status_df[['NAME', 'TYPE', 'STATUS', 'CREATED_ON', 'LAST_EVENT_TIME']], use_container_width=True)
            with st.expander("💡 Recommended Actions"):
                st.markdown("- **Investigate Inactive Runtimes:** For any runtime marked `🔴 Inactive`, verify its deployment and configuration.\n- **Review Old Runtimes:** Check the `CREATED_ON` date to identify and decommission old or unused runtimes to save costs.")
        else:
            st.warning("Could not find any runtimes in the events table.")
            
