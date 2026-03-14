"""
DC Infrastructure Monitoring App
Streamlit + Google Sheets — Digital Checklist, History, Analytics
"""

import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_gsheets import GSheetsConnection

# -----------------------------------------------------------------------------
# Config & constants
# -----------------------------------------------------------------------------
SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1Zs6v812MkWol2Tq6fybbOk_mQYKkhuTUyloUOMQNybs/edit?usp=sharing"
)
SHEET_MASTER_SERVER = "master_server"
SHEET_COMPONENTS = "components"
SHEET_LOG_PENGECEKAN = "log_pengecekan"

# Worksheet GID for log_pengecekan (from tab URL #gid=...) — using GID avoids HTTP 400 with public sheets
GID_LOG_PENGECEKAN = 727509916


def _worksheet_param(sheet_key: str, default_name_or_gid):
    """Use worksheet from secrets if set (GID int), else default (name str or GID int)."""
    try:
        gid_key = f"worksheet_{sheet_key}"
        if hasattr(st.secrets.connections.gsheets, gid_key):
            return int(getattr(st.secrets.connections.gsheets, gid_key))
    except Exception:
        pass
    return default_name_or_gid

st.set_page_config(
    page_title="DC Infrastructure Monitoring",
    page_icon="🖥️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# GSheets connection & data loading
# -----------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_master_servers():
    conn = st.connection("gsheets", type=GSheetsConnection)
    ws = _worksheet_param("master_server", SHEET_MASTER_SERVER)
    return conn.read(spreadsheet=SPREADSHEET_URL, worksheet=ws)


@st.cache_data(ttl=60)
def load_components():
    conn = st.connection("gsheets", type=GSheetsConnection)
    ws = _worksheet_param("components", SHEET_COMPONENTS)
    return conn.read(spreadsheet=SPREADSHEET_URL, worksheet=ws)


@st.cache_data(ttl=60)
def load_log_pengecekan():
    conn = st.connection("gsheets", type=GSheetsConnection)
    ws = _worksheet_param("log_pengecekan", GID_LOG_PENGECEKAN)
    return conn.read(spreadsheet=SPREADSHEET_URL, worksheet=ws)


def get_next_log_id(df_log: pd.DataFrame) -> int:
    """Compute next log_id as max(existing IDs) + 1."""
    if df_log is None or df_log.empty:
        return 1
    col = "log_id"
    if col not in df_log.columns:
        return 1
    try:
        numeric = pd.to_numeric(df_log[col], errors="coerce").dropna()
        if numeric.empty:
            return 1
        return int(numeric.max()) + 1
    except Exception:
        return 1


def append_to_log_pengecekan(conn, new_rows: list[dict]) -> None:
    """Append rows to log_pengecekan. Requires CRUD/Service Account in secrets."""
    if not new_rows:
        return
    df_existing = load_log_pengecekan()
    headers = list(df_existing.columns) if not df_existing.empty else list(new_rows[0].keys())
    try:
        # Try GSheetsConnection's underlying gspread client (when using Service Account)
        client = getattr(conn, "_connection", None) or getattr(conn, "client", None) or getattr(conn, "_instance", None)
        if client is not None and hasattr(client, "open_by_url"):
            sh = client.open_by_url(SPREADSHEET_URL)
            ws = sh.worksheet(SHEET_LOG_PENGECEKAN)
            for row in new_rows:
                values = [str(row.get(h, "")) for h in headers]
                ws.append_row(values, value_input_option="USER_ENTERED")
            return
    except Exception as e:
        st.error(
            f"Could not append to sheet: {e}. "
            "For write access: add Service Account credentials to `.streamlit/secrets.toml` and share the spreadsheet with the service account email."
        )
        raise
    st.error(
        "Append not available. Configure Service Account in `.streamlit/secrets.toml` (see st-gsheets-connection docs) and share the spreadsheet with the service account for write access."
    )
    raise RuntimeError("Write not configured")


# -----------------------------------------------------------------------------
# Sidebar: Sync + navigation
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("🖥️ DC Monitoring")
    if st.button("🔄 Sync Data", use_container_width=True):
        load_master_servers.clear()
        load_components.clear()
        load_log_pengecekan.clear()
        st.rerun()
    st.divider()
    page = st.radio(
        "Navigate",
        ["📋 Digital Checklist Form", "📜 History & Logs", "📊 Management Analytics"],
        label_visibility="collapsed",
    )

# Map radio to page key
PAGES = {
    "📋 Digital Checklist Form": "form",
    "📜 History & Logs": "history",
    "📊 Management Analytics": "analytics",
}
current_page = PAGES[page]

# -----------------------------------------------------------------------------
# Page 1: Digital Checklist Form
# -----------------------------------------------------------------------------
if current_page == "form":
    head_col1, head_col2 = st.columns([3, 1])
    with head_col1:
        st.header("Digital Checklist Form")
    with head_col2:
        if st.button("🔄 Sync Data", key="sync_form"):
            load_master_servers.clear()
            load_components.clear()
            load_log_pengecekan.clear()
            st.rerun()
    st.markdown("Select a server and complete the component status for this inspection.")

    try:
        df_master = load_master_servers()
        df_components = load_components()
        df_log = load_log_pengecekan()
    except Exception as e:
        err = str(e)
        st.error(f"Could not load data from Google Sheets. Error: {err}")
        if "400" in err or "Bad Request" in err:
            st.info(
                "**If you see 400 Bad Request:** (1) Ensure the spreadsheet URL is in `.streamlit/secrets.toml` under `[connections.gsheets]` → `spreadsheet = \"...\"`. "
                "(2) Share the sheet as **Anyone with the link** (Viewer). "
                "(3) Use sheet tab names exactly: **master_server**, **components**, **log_pengecekan** — or in `secrets.toml` set `worksheet_master_server`, `worksheet_components`, `worksheet_log_pengecekan` to the numeric GID of each tab (from the tab URL #gid=…)."
            )
        st.stop()

    if df_master.empty:
        st.warning("No servers found in master_server sheet.")
        st.stop()

    # Normalize column names (allow code_assets / code_asset, nama_server / server name, etc.)
    def norm_cols(df: pd.DataFrame, *candidates: tuple[str, ...]) -> dict:
        out = {}
        cols_lower = {c.lower().strip(): c for c in df.columns}
        for canonical, aliases in candidates:
            for a in aliases:
                key = a.lower().replace(" ", "_").strip()
                if key in cols_lower:
                    out[canonical] = cols_lower[key]
                    break
            if canonical not in out and df.columns.any():
                out[canonical] = df.columns[0]
        return out

    master_map = norm_cols(
        df_master,
        ("code_assets", ("code_assets", "code_asset", "server_code")),
        ("nama_server", ("nama_server", "nama server", "server_name", "server")),
    )
    comp_map = norm_cols(
        df_components,
        ("code_assets", ("code_assets", "code_asset", "server_code")),
        ("component_name", ("component_name", "component name", "name", "component")),
    )

    code_col_master = master_map.get("code_assets") or df_master.columns[0]
    name_col_master = master_map.get("nama_server") or (df_master.columns[1] if len(df_master.columns) > 1 else df_master.columns[0])
    code_col_comp = comp_map.get("code_assets") or df_components.columns[0]
    comp_name_col = comp_map.get("component_name") or (df_components.columns[1] if len(df_components.columns) > 1 else df_components.columns[0])

    server_options = df_master[[name_col_master, code_col_master]].drop_duplicates()
    server_display = server_options[name_col_master].tolist()
    server_codes = server_options[code_col_master].tolist()
    server_by_display = dict(zip(server_display, server_codes))

    selected_display = st.selectbox("Server", options=server_display, key="server_select")
    selected_code = server_by_display.get(selected_display)

    components_for_server = (
        df_components[df_components[code_col_comp].astype(str).str.strip() == str(selected_code).strip()]
        if selected_code is not None
        else pd.DataFrame()
    )

    if components_for_server.empty:
        st.info("No components found for this server. Add rows in the components sheet with this server's code_assets.")
    else:
        comp_names = components_for_server[comp_name_col].drop_duplicates().tolist()
        conn = st.connection("gsheets", type=GSheetsConnection)
        next_id = get_next_log_id(df_log)

        with st.form("checklist_form"):
            st.subheader(f"Inspection for **{selected_display}**")
            petugas = st.text_input("Petugas Name", placeholder="Your name", key="petugas")
            status_options = ["Healthy", "Warning", "Critical"]
            component_data = []
            all_filled = True
            for comp in comp_names:
                st.markdown(f"**{comp}**")
                col_radio, col_notes = st.columns([1, 2])
                with col_radio:
                    status = st.radio(
                        "Status",
                        options=status_options,
                        key=f"status_{comp}",
                        horizontal=True,
                        label_visibility="collapsed",
                    )
                with col_notes:
                    notes = st.text_area("XClarity Log/Notes", key=f"notes_{comp}", height=80, label_visibility="collapsed")
                if not status:
                    all_filled = False
                component_data.append({"component": comp, "status": status, "notes": notes or ""})

            submitted = st.form_submit_button("Submit Inspection")
            if submitted:
                if not petugas or not petugas.strip():
                    st.warning("Please enter Petugas Name.")
                else:
                    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    new_rows = []
                    for item in component_data:
                        new_rows.append({
                            "log_id": next_id,
                            "timestamp": ts,
                            "code_assets": selected_code,
                            "nama_server": selected_display,
                            "petugas": petugas.strip(),
                            "component_name": item["component"],
                            "status": item["status"],
                            "notes": item["notes"],
                        })
                    try:
                        append_to_log_pengecekan(conn, new_rows)
                        load_log_pengecekan.clear()
                        st.success(f"Inspection submitted with log_id **{next_id}**.")
                    except Exception as e:
                        st.error(f"Submit failed: {e}")

# -----------------------------------------------------------------------------
# Page 2: History & Logs
# -----------------------------------------------------------------------------
elif current_page == "history":
    st.header("History & Logs")
    try:
        df_log = load_log_pengecekan()
    except Exception as e:
        err = str(e)
        st.error(f"Could not load logs: {err}")
        if "400" in err or "Bad Request" in err:
            st.info("Check `.streamlit/secrets.toml` has `[connections.gsheets]` with `spreadsheet` URL, sheet is shared, and tab names are **master_server**, **components**, **log_pengecekan** (or set worksheet_* GIDs in secrets).")
        st.stop()

    if df_log.empty:
        st.info("No inspection logs yet.")
    else:
        # Normalize common column names for search and date filter
        cols_lower = {c.lower().replace(" ", "_"): c for c in df_log.columns}
        code_col = cols_lower.get("code_assets") or cols_lower.get("code_asset") or df_log.columns[0]
        ts_col = None
        for k in ("timestamp", "date", "datetime", "created"):
            if k in cols_lower:
                ts_col = cols_lower[k]
                break
        if ts_col is None and df_log.columns.any():
            ts_col = df_log.columns[0]
        status_col = cols_lower.get("status") or (df_log.columns[2] if len(df_log.columns) > 2 else None)

        search = st.text_input("Search by code_assets", placeholder="Filter by server code...", key="history_search")
        date_filter = st.date_input("Filter by date", value=None, key="history_date")

        if search:
            df_log = df_log[df_log[code_col].astype(str).str.lower().str.contains(search.lower(), na=False)]
        if date_filter and ts_col:
            try:
                df_log[ts_col] = pd.to_datetime(df_log[ts_col], errors="coerce")
                df_log = df_log[df_log[ts_col].dt.date == date_filter]
            except Exception:
                pass

        def row_style(row, status_column):
            if status_column is None or status_column not in row:
                return []
            s = str(row.get(status_column, "")).strip().lower()
            if s == "critical":
                return ["background-color: rgba(239,68,68,0.25)"]
            if s == "warning":
                return ["background-color: rgba(234,179,8,0.25)"]
            return []

        if status_col:
            styled = df_log.style.apply(
                lambda row: row_style(row, status_col),
                axis=1,
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)
        else:
            st.dataframe(df_log, use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# Page 3: Management Analytics
# -----------------------------------------------------------------------------
elif current_page == "analytics":
    st.header("Management Analytics")
    try:
        df_master = load_master_servers()
        df_log = load_log_pengecekan()
    except Exception as e:
        err = str(e)
        st.error(f"Could not load data: {err}")
        if "400" in err or "Bad Request" in err:
            st.info("Check `.streamlit/secrets.toml` has `[connections.gsheets]` with `spreadsheet` URL and sheet tab names (or worksheet_* GIDs).")
        st.stop()

    cols_lower_log = {c.lower().replace(" ", "_"): c for c in df_log.columns}
    status_col = cols_lower_log.get("status")
    ts_col = cols_lower_log.get("timestamp") or cols_lower_log.get("date") or (df_log.columns[0] if len(df_log.columns) else None)
    code_col_log = cols_lower_log.get("code_assets") or cols_lower_log.get("code_asset") or (df_log.columns[0] if df_log.columns.any() else None)

    total_servers = 0 if df_master.empty else df_master.drop_duplicates().shape[0]
    if df_master.columns.any():
        first_col = df_master.columns[0]
        total_servers = df_master[first_col].nunique()

    today = datetime.date.today()
    active_issues = 0
    completion_today = 0
    total_today = 0
    if not df_log.empty and status_col and ts_col:
        _log = df_log.copy()
        _log[ts_col] = pd.to_datetime(_log[ts_col], errors="coerce")
        today_logs = _log[_log[ts_col].dt.date == today]
        total_today = len(today_logs)
        completion_today = total_today
        active_issues = int((_log[status_col].astype(str).str.lower().isin(["critical", "warning"])).sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Servers", total_servers)
    col2.metric("Active Issues (Warning + Critical)", active_issues)
    col3.metric("Completions Today", completion_today)

    st.subheader("DC Health Overview")
    if df_log.empty or not status_col:
        st.info("No status data for pie chart.")
    else:
        status_counts = df_log[status_col].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig_pie = px.pie(status_counts, values="count", names="status", title="Overall DC Health")
        st.plotly_chart(fig_pie, use_container_width=True)

    st.subheader("Servers with Most Warning/Critical (Last 30 Days)")
    if df_log.empty or not status_col or not code_col_log:
        st.info("No data for bar chart.")
    else:
        _log30 = df_log.copy()
        _log30[ts_col] = pd.to_datetime(_log30[ts_col], errors="coerce")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
        last30 = _log30[_log30[ts_col] >= cutoff]
        issues = last30[last30[status_col].astype(str).str.lower().isin(["warning", "critical"])]
        by_server = issues.groupby(code_col_log).size().reset_index(name="count").sort_values("count", ascending=True)
        if by_server.empty:
            st.info("No Warning/Critical in the last 30 days.")
        else:
            fig_bar = px.bar(by_server, x="count", y=code_col_log, orientation="h", title="By server (last 30 days)")
            st.plotly_chart(fig_bar, use_container_width=True)
