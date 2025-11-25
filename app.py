import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import threading
from datetime import datetime
import json

# Configuration for your 3 nodes
NODE_CONFIGS = {
    "Central Node": {
        "host": "10.2.14.3",
        "port": 60703,
        "user": "root",
        "password": "mhgWe4sbBVZ2XS5H6PJ7KCFM",
        "database": "db" # FIXME: lagay db name here !
    },
    "Node 2": {
        "host": "10.2.14.4",
        "port": 60704,
        "user": "root",
        "password": "mhgWe4sbBVZ2XS5H6PJ7KCFM",
        "database": "db" # FIXME: lagay db name here !
    },
    "Node 3": {
        "host": "10.2.14.5",
        "port": 60705,
        "user": "root",
        "password": "mhgWe4sbBVZ2XS5H6PJ7KCFM",
        "database": "db" # FIXME: lagay db name here !
    }
}

# start session state for logs
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []
if 'replication_log' not in st.session_state:
    st.session_state.replication_log = []

class DatabaseConnection:
    def __init__(self, config):
        self.config = config
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            return True
        except Error as e:
            st.error(f"Connection failed: {e}")
            return False
        
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()

def execute_concurrent_transaction(node_name, query, params, isolation_level, transaction_id):
    start_time = time.time()
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    result = db.execute_query(query, params, isolation_level)
    end_time = time.time()
    
    log_entry = {
        "transaction_id": transaction_id,
        "node": node_name,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "duration": f"{(end_time - start_time):.4f}s",
        "isolation_level": isolation_level,
        "status": "success" if result and "error" not in result else "failed",
        "result": result
    }
    
    st.session_state.transaction_log.append(log_entry)
    db.close()

# header
st.set_page_config(page_title="Distributed Database System", layout="wide")
st.title("MCO2: Transaction Management & Recovery")
st.markdown("### Corpuz, Cumti, Pineda, Punsalan")

# sidebar for node status and quick actions
with st.sidebar:
    st.header("Node Status")
    for node_name, config in NODE_CONFIGS.items():
        db = DatabaseConnection(config)
        if db.connect():
            st.success(f"✅ {node_name}")
            db.close()
        else:
            st.error(f"❌ {node_name}")
    
    st.divider()
    st.header("Quick Actions")
    if st.button("Clear Logs"):
        st.session_state.transaction_log = []
        st.session_state.replication_log = []
        st.rerun()

# main tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Database View", 
    "Concurrency Testing", 
    "Failure Recovery", 
    "Transaction Logs",
    "Manual Operations"
])

# tab 1: database view
with tab1:
    st.header("Current Database State")

# tab 2: concurrency testing
with tab2:
    st.header("Concurrency Control Testing")

# tab 3: failure recovery
with tab3:
    st.header("Global Failure Recovery Testing")

# tab 4: transaction logs
with tab4:
    st.header("Transaction & Replication Logs")