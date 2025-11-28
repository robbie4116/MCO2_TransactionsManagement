import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import threading
from datetime import datetime
import json
from contextlib import contextmanager

# NODE CONFIGURATION ==============================
NODE_CONFIGS = {
    "Central Node": {
        "host": "ccscloud.dlsu.edu.ph",
        "port": 60703,
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    },
    "Node 2": {
        "host": "ccscloud.dlsu.edu.ph",
        "port": 60704,
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    },
    "Node 3": {
        "host": "ccscloud.dlsu.edu.ph",
        "port": 60705,
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    }
}


# SESSION STATE INITIALIZATION ==============================
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []
if 'replication_log' not in st.session_state:
    st.session_state.replication_log = []
if 'recovery_log' not in st.session_state:
    st.session_state.recovery_log = []
if 'node_status' not in st.session_state:
    st.session_state.node_status = {
        "Central Node": True,
        "Node 2": True,
        "Node 3": True
    }
if 'simulated_failures' not in st.session_state:
    st.session_state.simulated_failures = {}


# DATABASE CONNECTION ==============================
class DatabaseConnection:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            return True
        except Error as e:
            st.error(f"Connection failed: {e}")
            return False
    
    def execute_query(self, query, params=None, fetch=True):
        # TODO thara: concurrency control
        # TODO jeff: add transaction logging here

        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return None
            
            self.cursor = self.connection.cursor(dictionary=True)
            self.cursor.execute(query, params or ())
            
            if fetch and query.strip().upper().startswith('SELECT'):
                result = self.cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return {"affected_rows": self.cursor.rowcount}
                
        except Error as e:
            if self.connection:
                self.connection.rollback()
            return {"error": str(e)}
        
        # TODO (jeff): add connection retry logic for failure scenarios

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()


# CONCURRENCY CONTROL 
# TODO (thara): Implement all functions in this section
def set_isolation_level(connection, level):
    """
    TODO (thara): make this function set the isolation level for the given connection
    - READ UNCOMMITTED
    - READ COMMITTED  
    - REPEATABLE READ
    - SERIALIZABLE
    """
    pass

def execute_concurrent_transaction(node_name, query, params, isolation_level, transaction_id):
    """
    TODO (thara): Complete this function to:
    1. connect to the specified node
    2. set the isolation level using set_isolation_level()
    3. execute the query
    4. log the transaction (timestamp, duration, status)
    5. handle any errors/conflicts that arise
    """
    start_time = time.time()
    
    # TODO (thara): Should support the 3 test cases:
        # case 1: concurrent reads
        # case 2: read + Write conflicts
        # case 3: write + Write conflicts
    
    end_time = time.time()
    
    # Log the transaction
    log_entry = {
        "transaction_id": transaction_id,
        "node": node_name,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "duration": f"{(end_time - start_time):.4f}s",
        "isolation_level": isolation_level,
        "status": "pending",  # TODO (thara): update based on execution result
        "query": query,
        "params": params
    }
    
    st.session_state.transaction_log.append(log_entry)

def test_concurrent_reads(isolation_level):
    """
    case #1: concurrent transactions reading the same data item
    
    TODO (thara): Implement this test case
    Steps:
    1. create 2+ threads that read the same row(s) simultaneously
    2. use threading to simulate concurrency
    3. verify all reads return consistent data
    4. log results
    """
    pass

def test_read_write_conflict(isolation_level):
    """
    case #2: at least one transaction writing, others reading same data
    
    TODO (thara): Implement this test case
    Steps:
    1. create threads: 1+ writer, 1+ reader on same data
    2. test for dirty reads, non-repeatable reads
    3. verify behavior matches isolation level expectations
    4. log results and any anomalies detected
    """
    pass

def test_write_write_conflict(isolation_level):
    """
    case #3: concurrent transactions writing to same data item
    
    TODO (thara): Implement this test case
    Steps:
    1. create 2+ threads that update/delete the same row(s)
    2. test for lost updates
    3. verify final database state is consistent
    4. log which transaction succeeded and any failures
    """
    pass

# REPLICATION MODULE
# TODO (jeff): Implement all functions in this section
def replicate_to_central(source_node, query, params):
    """
    Replicate a write operation from Node 2/3 to Central Node
    
    TODO (jeff): Implement replication logic
    Steps:
    1. Execute the query on Central Node
    2. Log the replication attempt
    3. Handle replication failures (Case #1)
    4. Return success/failure status
    
    This supports your master-slave or multi-master design
    """
    # TODO (jeff): Add implementation
    
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "source": source_node,
        "target": "Central Node",
        "query": query,
        "status": "pending"  # TODO (jeff): Update based on result
    }
    st.session_state.replication_log.append(log_entry)
    pass

def replicate_from_central(target_node, query, params):
    """
    Replicate a write operation from Central Node to Node 2/3
    
    TODO (jeff): Implement replication logic
    Steps:
    1. cetermine which node(s) need the update (based on fragmentation)
    2. execute the query on target node(s)
    3. log the replication attempt
    4. handle replication failures (Case #3)
    5. return success/failure status
    """
    # TODO (jeff): Add implementation
    
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "source": "Central Node",
        "target": target_node,
        "query": query,
        "status": "pending"  # TODO (jeff): Update based on result
    }
    st.session_state.replication_log.append(log_entry)
    pass

# RECOVERY MODULE
# TODO (jeff): all functions here
def log_write_operation(node, query, params, transaction_id):
    """
    Log write operations for recovery purposes
    
    TODO (jeff): Implement write-ahead logging or similar mechanism
    This log will be used when nodes recover from failure to replay missed transactions
    
    Store: timestamp, node, query, params, transaction_id, status
    Consider: file-based log, database log table, or in-memory structure
    """
    pass

def simulate_node_failure(node_name):
    """
    Simulate a node going offline
    
    TODO (jeff): Set flag to reject connections to this node
    This will be used to test Cases #1 and #3 (replication failures)
    """
    st.session_state.node_status[node_name] = False
    st.session_state.simulated_failures[node_name] = datetime.now()
    pass

def simulate_node_recovery(node_name):
    """
    Simulate a node coming back online
    
    TODO (jeff): Implement recovery logic for Cases #2 and #4
    Steps:
    1. Mark node as available
    2. Retrieve missed transactions from log
    3. Replay transactions on recovered node
    4. Verify data consistency after recovery
    5. Log recovery process
    """
    st.session_state.node_status[node_name] = True
    
    # TODO (jeff): Add recovery logic
    # 1. Get all missed transactions while node was down
    # 2. Replay them in order
    # 3. Handle any conflicts
    
    recovery_log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "node": node_name,
        "downtime": None,  # TODO (jeff): Calculate downtime
        "missed_transactions": 0,  # TODO (jeff): Count missed transactions
        "status": "recovered"
    }
    st.session_state.recovery_log.append(recovery_log)
    pass

def test_replication_failure_to_central():
    """
    Case #1: Transaction fails when replicating from Node 2/3 to Central
    
    TODO (jeff): Implement this test case
    Steps:
    1. Simulate Central Node failure
    2. Execute write on Node 2 or 3
    3. Attempt replication (should fail)
    4. Log failure
    5. Verify Node 2/3 still has the data
    """
    pass

def test_central_recovery_missed_writes():
    """
    Case #2: Central Node recovers and needs to catch up on missed writes
    
    TODO (jeff): Implement this test case
    Steps:
    1. Simulate Central Node failure
    2. Execute writes on Node 2/3 while Central is down
    3. Bring Central back online
    4. Replay missed transactions
    5. Verify all nodes are consistent
    """
    pass

def test_replication_failure_from_central():
    """
    Case #3: Transaction fails when replicating from Central to Node 2/3
    
    TODO (jeff): Implement this test case
    Steps:
    1. Simulate Node 2 or 3 failure
    2. Execute write on Central
    3. Attempt replication (should fail)
    4. Log failure
    5. Verify Central still has the data
    """
    pass

def test_node_recovery_missed_writes():
    """
    Case #4: Node 2/3 recovers and needs to catch up on missed writes
    
    TODO (jeff): Implement this test case
    Steps:
    1. Simulate Node 2 or 3 failure
    2. Execute writes on Central while node is down
    3. Bring node back online
    4. Replay missed transactions
    5. Verify all nodes are consistent
    """
    pass

# UTILITY FUNCTIONS 
def check_node_health(node_name):
    if node_name in st.session_state.simulated_failures:
        return False
    
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    status = db.connect()
    db.close()
    return status

def get_table_data(node_name, trans=None, limit=100):
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if not db.connect():
        return None
    
    query = f"SELECT * FROM trans LIMIT {limit}"
    result = db.execute_query(query)
    db.close()
    return result

# STREAMLIT UI 
st.set_page_config(page_title="Distributed Database System", layout="wide")
st.title("MCO2: Transaction Management & Recovery")
st.markdown("### Corpuz, Cumti, Pineda, & Punsalan")

# Sidebar - Node Status
with st.sidebar:
    st.header("Node Status")
    for node_name in NODE_CONFIGS.keys():
        if check_node_health(node_name):
            st.success(f"✅ {node_name}")
        else:
            st.error(f"❌ {node_name} (Offline)")

    st.divider()
    st.header("Node Failure / Recovery Simulation")

    node_to_simulate = st.selectbox("Select Node", list(NODE_CONFIGS.keys()), key="simulate_node")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Simulate Failure"):
            simulate_node_failure(node_to_simulate)
            st.warning(f"{node_to_simulate} failure simulated")
            
    with col2:
        if st.button("Simulate Recovery"):
            simulate_node_recovery(node_to_simulate)
            st.success(f"{node_to_simulate} recovery initiated")

    st.divider()
    st.header("Quick Actions")
    
    if st.button("Clear All Logs"):
        st.session_state.transaction_log = []
        st.session_state.replication_log = []
        st.session_state.recovery_log = []
        st.rerun()

# Main Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Database View", 
    "Concurrency Testing", 
    "Failure Recovery", 
    "Transaction Logs",
    "Manual Operations"
])

# TAB 1: DATABASE VIEW
with tab1:
    st.header("Current Database State Across All Nodes")
    
    if st.button("Refresh Data"):
        st.experimental_rerun()
        
    #  Central Node 
    st.subheader("Central Node")
    try:
        central_data = get_table_data("Central Node", "trans") 
        st.write(f"Rows: {len(central_data)}")
        st.dataframe(central_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Central Node data: {e}")
    
    # Node 2
    st.subheader("Node 2")
    try:
        node2_data = get_table_data("Node 2", "trans")
        st.write(f"Rows: {len(node2_data)}")
        st.dataframe(node2_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 2 data: {e}")
    
    # Node 3 
    st.subheader("Node 3")
    try:
        node3_data = get_table_data("Node 3", "trans")
        st.write(f"Rows: {len(node3_data)}")
        st.dataframe(node3_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 3 data: {e}")


# TAB 2: CONCURRENCY TESTING
# TODO (emman): Create UI for thara's test cases
# TODO (thara): Wire up your test functions here
with tab2:
    st.header("Concurrency Control Testing")
    
    # Isolation level selector
    isolation_level = st.selectbox(
        "Select Isolation Level",
        ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
        key="isolation_level"
    )
    
    st.divider()
    
    # Test Case Selection
    test_case = st.radio(
        "Select Test Case",
        [
            "Case #1: Concurrent Reads",
            "Case #2: Read + Write Conflict",
            "Case #3: Write + Write Conflict"
        ]
    )
    
    # TODO (emman): Add input fields for test parameters
    # - Number of concurrent transactions
    # - Which nodes to use
    # - Data to read/write
    
    if st.button("Run Concurrency Test"):
        st.info(f"Running {test_case} with {isolation_level}...")
        
        # TODO (thara): Call appropriate test function based on test_case
        # if test_case == "Case #1: Concurrent Reads":
        #     test_concurrent_reads(isolation_level)
        # elif test_case == "Case #2: Read + Write Conflict":
        #     test_read_write_conflict(isolation_level)
        # elif test_case == "Case #3: Write + Write Conflict":
        #     test_write_write_conflict(isolation_level)
        
        st.success("Test completed! Check Transaction Logs tab for results.")
    
    st.divider()
    st.subheader("Recent Test Results")

    recent_transactions = st.session_state.get("transaction_log", [])
    if recent_transactions:
        df_recent = pd.DataFrame(recent_transactions)
        # Show only last 10 transactions
        st.dataframe(df_recent.tail(10), use_container_width=True, height=300)
    else:
        st.info("No transactions logged yet...")

# TAB 3: FAILURE RECOVERY TESTING
with tab3:
    st.header("Global Failure Recovery Testing")
    
    # Test Case Selection
    recovery_test_case = st.radio(
        "Select Recovery Test Case",
        [
            "Case #1: Node 2/3 → Central Replication Failure",
            "Case #2: Central Node Recovery (Missed Writes)",
            "Case #3: Central → Node 2/3 Replication Failure",
            "Case #4: Node 2/3 Recovery (Missed Writes)"
        ]
    )
    
    # TODO (emman): Add controls for:
    # - Which node to fail/recover
    # - Number of transactions to execute during failure
    # - Data to use for testing
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Simulate Node Failure"):
            # TODO (jeff): call simulate_node_failure() for selected node
            st.warning("Node failure simulated")
    
    with col2:
        if st.button("Simulate Node Recovery"):
            # TODO (jeff): call simulate_node_recovery() for selected node
            st.success("Node recovery initiated")
    
    st.divider()
    
    if st.button("Run Recovery Test"):
        st.info(f"Running {recovery_test_case}...")
        
        # TODO (jeff): call function based on recovery_test_case
        # if recovery_test_case == "Case #1...":
        #     test_replication_failure_to_central()
        # elif recovery_test_case == "Case #2...":
        #     test_central_recovery_missed_writes()
        # ... etc
        
        st.success("Recovery test completed! Check logs.")
    
    st.divider()
    st.subheader("Recovery Log")
    
    recovery_logs = st.session_state.get("recovery_log", [])

    if recovery_logs:
        df_recovery = pd.DataFrame(recovery_logs)
        
        # Optional filters
        selected_node = st.selectbox(
            "Select Node",
            ["All Nodes"] + list(NODE_CONFIGS.keys()),
            key="recovery_node_filter"
        )
        
        status_options = ["All", "Recovered", "Pending"]
        selected_status = st.selectbox(
            "Select Status",
            status_options,
            key="recovery_status_filter"
        )
        
        # Apply filters
        if selected_node != "All Nodes" and "node" in df_recovery.columns:
            df_recovery = df_recovery[df_recovery["node"] == selected_node]
        
        if selected_status != "All" and "status" in df_recovery.columns:
            df_recovery = df_recovery[df_recovery["status"].str.lower() == selected_status.lower()]
        
        if not df_recovery.empty:
            st.dataframe(df_recovery, use_container_width=True, height=300)
        else:
            st.info("No recovery logs match the selected filters.")
    else:
        st.info("No recovery logs available yet...")


# TAB 4: TRANSACTION LOGS
with tab4:
    st.header("Transaction & Replication Logs")
    
    log_type = st.selectbox(
        "Select Log Type",
        ["Transaction Logs", "Replication Logs", "Recovery Logs", "All Logs"],
        key = "log_type"
    )
    
    # filtering options
    nodes = ["Central Node", "Node 2", "Node 3"]
    selected_node = st.selectbox("Select Node", ["All Nodes"] + nodes, key="selected_node")
    
    start_time = st.date_input("Start Date", value=None, key="start_time")
    end_time = st.date_input("End Date", value=None, key="end_time")

    status_options = ["All", "Success", "Failed"]
    selected_status = st.selectbox("Status", status_options, key="selected_status")
    
    transaction_id_filter = st.text_input("Transaction ID (optional)")
    
    def filter_logs(df):
        if df is None or df.empty:
            return pd.DataFrame()
        
        filtered = df.copy()
        if selected_node != "All Nodes" and "node" in filtered.columns:
            filtered = filtered[filtered["node"] == selected_node]
        if selected_status != "All" and "status" in filtered.columns:
            filtered = filtered[filtered["status"].str.lower() == selected_status.lower()]
        if transaction_id_filter and "transaction_id" in filtered.columns:
            filtered = filtered[filtered["transaction_id"].astype(str).str.contains(transaction_id_filter)]
        if start_time and "timestamp" in filtered.columns:
            filtered = filtered[pd.to_datetime(filtered["timestamp"]).dt.date >= start_time]
        if end_time and "timestamp" in filtered.columns:
            filtered = filtered[pd.to_datetime(filtered["timestamp"]).dt.date <= end_time]
        return filtered
    
    # Display logs
    def display_log(name, log_data):
        st.subheader(name)
        if log_data:
            df = pd.DataFrame(log_data)
            df = filter_logs(df)
            if not df.empty:
                st.dataframe(df, use_container_width=True, height=400)
            else:
                st.info(f"No {name.lower()} for selected filters")
        else:
            st.info(f"No {name.lower()} yet")
    
    if log_type in ["Transaction Logs", "All Logs"]:
        display_log("Transaction Logs", st.session_state.get("transaction_log", []))
    
    if log_type in ["Replication Logs", "All Logs"]:
        display_log("Replication Logs", st.session_state.get("replication_log", []))
    
    if log_type in ["Recovery Logs", "All Logs"]:
        display_log("Recovery Logs", st.session_state.get("recovery_log", []))

# TAB 5: MANUAL OPERATIONS
with tab5:
    st.header("Manual Database Operations")
    
    selected_node = st.selectbox("Select Node", list(NODE_CONFIGS.keys()), key="manual_node")
    query = st.text_area("SQL Query", height=100)
    operation_type = st.radio("Operation Type", ["Read", "Write"])
    
    if st.button("Execute Query"):
        if query:
            db = DatabaseConnection(NODE_CONFIGS[selected_node])
            if db.connect():
                result = db.execute_query(query, fetch=(operation_type == "Read"))
                
                if result and isinstance(result, list):
                    st.dataframe(pd.DataFrame(result))
                elif result:
                    st.json(result)
                else:
                    st.error("Query execution failed")
                
                db.close()
        else:
            st.warning("Please enter a query")

st.divider()
st.caption("MCO2 - Group 8 | STADVDB - S17")