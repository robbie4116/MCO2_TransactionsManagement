import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import threading
from datetime import datetime
import json
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import logging

# Suppress Streamlit deprecation warnings
logging.getLogger("streamlit.deprecation_util").setLevel(logging.ERROR)

# STREAMLIT UI 
st.set_page_config(page_title="Distributed Database System", layout="wide")
st.title("MCO2: Transaction Management & Recovery")
st.markdown("### Corpuz, Cumti, Pineda, & Punsalan")

# LOAD ENVIRONMENT VARIABLES ==============================
load_dotenv()
CURRENT_NODE_NAME = os.getenv("APP_NODE_ID") if os.getenv("APP_NODE_ID") else "Central Node"


# NODE CONFIGURATION ==============================
# NODE_CONFIGS = {
#     "Central Node": {
#         "host": "10.2.14.3",
#         "port": 3306,
#         "user": "user1",
#         "password": "UserPass123!",
#         "database": "mco2financedata"
#     },
#     "Node 2": {
#         "host": "10.2.14.4",
#         "port": 3306,
#         "user": "user1",
#         "password": "UserPass123!",
#         "database": "mco2financedata"
#     },
#     "Node 3": {
#         "host": "10.2.14.5",
#         "port": 3306,
#         "user": "user1",
#         "password": "UserPass123!",
#         "database": "mco2financedata"
#    }
#}


# TO RUN LOCAL, use the following NODE_CONFIGS instead
NODE_CONFIGS = {
    "Central Node": {
            "host": "ccscloud.dlsu.edu.ph", # type: ignore # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
            "port": 60703, # TO RUN LOCAL, change to 60703
            "user": "user1",
            "password": "UserPass123!",
            "database": "mco2financedata"
    },
    "Node 2": {
        "host": "ccscloud.dlsu.edu.ph",  # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
        "port": 60704, # TO RUN LOCAL, change to 60704
        "host": "ccscloud.dlsu.edu.ph",  # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
        "port": 60704, # TO RUN LOCAL, change to 60704
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    },
    "Node 3": {
        "host": "ccscloud.dlsu.edu.ph", # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
        "port": 60705, # TO RUN LOCAL, change to 60705
        "host": "ccscloud.dlsu.edu.ph", # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
        "port": 60705, # TO RUN LOCAL, change to 60705
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    }
}


# SESSION STATE INITIALIZATION ==============================
if "transaction_log" not in st.session_state:
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

# Manual Transaction Control State
if 'active_connection' not in st.session_state:
    st.session_state.active_connection = None
if 'active_node' not in st.session_state:
    st.session_state.active_node = None
if 'transaction_active' not in st.session_state:
    st.session_state.transaction_active = False
if 'transaction_operations' not in st.session_state:
    st.session_state.transaction_operations = []
if 'transaction_start_time' not in st.session_state:
    st.session_state.transaction_start_time = None


# DATABASE CONNECTION ==============================
class DatabaseConnection:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
        self.in_transaction = False  # Track if we're in a managed transaction
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            # Set timezone to Manila for this connection
            cursor = self.connection.cursor()
            cursor.execute("SET time_zone = '+08:00'")
            cursor.close()
            return True
        except Error as e:
            st.error(f"Connection failed: {e}")
            return False
    
    def is_connection_alive(self):
        """Check if connection is still valid"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.ping(reconnect=False)
                return True
        except:
            return False
        return False
    
    def ensure_connected(self):
        """Reconnect if connection was lost"""
        if not self.is_connection_alive():
            self.connect()
    
    def begin_transaction(self):
        """Start an explicit transaction without auto-committing"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                raise Exception("Failed to connect to database")
        
        cursor = self.connection.cursor()
        cursor.execute("START TRANSACTION")
        cursor.close()
        
        # Disable auto-commit for this connection
        self.connection.autocommit = False
        self.in_transaction = True
    
    def commit_transaction(self):
        """Commit the current transaction and re-enable auto-commit"""
        if self.in_transaction and self.connection:
            self.connection.commit()
            self.in_transaction = False
            self.connection.autocommit = True
    
    def rollback_transaction(self):
        """Rollback the current transaction and re-enable auto-commit"""
        if self.in_transaction and self.connection:
            self.connection.rollback()
            self.in_transaction = False
            self.connection.autocommit = True
    
    def execute_query(self, query, params=None, fetch=True, isolation_level=None):
        # TODO thara: concurrency control
        # TODO jeff: add transaction logging here

        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return None

            # If caller requested a per-transaction isolation level, apply it on the connection
            if isolation_level:
                set_isolation_level(self.connection, isolation_level, per_transaction=True)

            self.cursor = self.connection.cursor(dictionary=True)

            # Handle multi-statement queries (e.g., "UPDATE ...; SELECT SLEEP(2);")
            # mysql-connector's cursor.execute(..., multi=True) yields a cursor for each statement.
            if ";" in query and query.strip().count(";") >= 1:
                results = []
                last_rowcount = 0
                # Execute each statement in order and collect SELECT results if requested
                for result_cursor in self.cursor.execute(query, params or (), multi=True):
                    try:
                        if fetch and getattr(result_cursor, "with_rows", False):
                            rows = result_cursor.fetchall()
                            if rows:
                                results.extend(rows)
                    except Exception:
                        # ignore fetch errors for statements that don't return rows
                        pass
                    try:
                        last_rowcount = result_cursor.rowcount
                    except Exception:
                        last_rowcount = getattr(self.cursor, "rowcount", 0)

                # Commit after multi-statement execution
                try:
                    self.connection.commit()
                except Exception:
                    pass

                if fetch:
                    return results
                else:
                    return {"affected_rows": last_rowcount}

            # Single statement case
            self.cursor.execute(query, params or ())

            if fetch and query.strip().upper().startswith('SELECT'):
                result = self.cursor.fetchall()
                return result
            else:
                # write operation - only auto-commit if NOT in a managed transaction
                if not self.in_transaction:
                    self.connection.commit()
                return {"affected_rows": self.cursor.rowcount}

        except Error as e:
            if self.connection:
                try:
                    # Only rollback if not in managed transaction (let user control it)
                    if not self.in_transaction:
                        self.connection.rollback()
                except Exception:
                    pass
            return {"error": str(e)}
         # TODO (jeff): add connection retry logic for failure scenarios


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()


# LOGGING & REPLICATION HELPERS ==============================
def parse_year_from_date(value):
    try:
        if value is None:
            return None
        if hasattr(value, "year"):
            return value.year
        # try ISO string
        return datetime.fromisoformat(str(value)).year
    except Exception:
        return None

def get_mysql_now(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT NOW() as server_time")
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else datetime.now()

def to_log_node_name(node_name):
    """Fit node names into VARCHAR(10) columns for logs."""
    mapping = {
        "Central Node": "Central",
        "Node 2": "Node2",
        "Node 3": "Node3",
    }
    if not node_name:
        return None
    if node_name in mapping:
        return mapping[node_name]
    return str(node_name)[:10]
def choose_target_nodes_by_year(date_value):
    """
    Decide which fragment nodes should store the row based on newdate.
    Node 2: 1993-1995, Node 3: 1996-1998.
    """
    year = parse_year_from_date(date_value)
    targets = []
    if year is None:
        return targets
    if 1993 <= year <= 1995:
        targets.append("Node 2")
    if 1996 <= year <= 1998:
        targets.append("Node 3")
    return targets


def fetch_trans_row(node_name, trans_id):
    """Fetch a single trans row by PK from a node."""
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if not db.connect():
        return None
    row = None
    try:
        res = db.execute_query("SELECT * FROM trans WHERE trans_id = %s", params=(trans_id,), fetch=True)
        if isinstance(res, list) and res:
            row = res[0]
    except Exception:
        row = None
    try:
        db.close()
    except Exception:
        pass
    return row


def route_and_replicate_write(source_node, trans_id, op_type, row_before=None, row_after=None):
    """
    Decide targets and perform replication of a completed write on trans.
    Uses row_after.newdate (fallback to row_before) for shard routing.
    """
    print(f"[ROUTE_REPLICATE] Called with source_node={source_node}, trans_id={trans_id}, op_type={op_type}")
    
    newdate = None
    if row_after and row_after.get("newdate"):
        newdate = row_after.get("newdate")
    elif row_before and row_before.get("newdate"):
        newdate = row_before.get("newdate")

    old_amt = row_before.get("amount") if row_before else None
    new_amt = row_after.get("amount") if row_after else None

    # For balance-changing operations, prefer balance column
    if row_before and "balance" in row_before:
        old_amt = row_before.get("balance")
    if row_after and "balance" in row_after:
        new_amt = row_after.get("balance")

    if source_node == "Central Node":
        targets = choose_target_nodes_by_year(newdate)
        # Build a simple deterministic update/insert/delete for the shard
        for t in targets:
            if op_type == "INSERT" and row_after:
                rep_q = """
                    INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        account_id=VALUES(account_id),
                        newdate=VALUES(newdate),
                        type=VALUES(type),
                        operation=VALUES(operation),
                        amount=VALUES(amount),
                        balance=VALUES(balance),
                        account=VALUES(account)
                """
                rep_p = (
                    row_after.get("trans_id"),
                    row_after.get("account_id"),
                    row_after.get("newdate"),
                    row_after.get("type"),
                    row_after.get("operation"),
                    row_after.get("amount"),
                    row_after.get("balance"),
                    row_after.get("account"),
                )
            elif op_type == "UPDATE" and row_after:
                rep_q = """
                    UPDATE trans SET account_id=%s, newdate=%s, type=%s, operation=%s, amount=%s, balance=%s, account=%s
                    WHERE trans_id=%s
                """
                rep_p = (
                    row_after.get("account_id"),
                    row_after.get("newdate"),
                    row_after.get("type"),
                    row_after.get("operation"),
                    row_after.get("amount"),
                    row_after.get("balance"),
                    row_after.get("account"),
                    trans_id,
                )
            elif op_type == "DELETE":
                rep_q = "DELETE FROM trans WHERE trans_id = %s"
                rep_p = (trans_id,)
            else:
                continue

            replicate_from_central(
                target_node=t,
                query=rep_q,
                params=rep_p,
                trans_id=trans_id,
                op_type=op_type,
                pk_value=str(trans_id),
                old_amount=old_amt,
                new_amount=new_amt,
            )
    else:
        # Source is Node 2 or Node 3 -> replicate to Central
        if op_type == "INSERT" and row_after:
            rep_q = """
                INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    account_id=VALUES(account_id),
                    newdate=VALUES(newdate),
                    type=VALUES(type),
                    operation=VALUES(operation),
                    amount=VALUES(amount),
                    balance=VALUES(balance),
                    account=VALUES(account)
            """
            rep_p = (
                row_after.get("trans_id"),
                row_after.get("account_id"),
                row_after.get("newdate"),
                row_after.get("type"),
                row_after.get("operation"),
                row_after.get("amount"),
                row_after.get("balance"),
                row_after.get("account"),
            )
        elif op_type == "UPDATE" and row_after:
            rep_q = """
                UPDATE trans SET account_id=%s, newdate=%s, type=%s, operation=%s, amount=%s, balance=%s, account=%s
                WHERE trans_id=%s
            """
            rep_p = (
                row_after.get("account_id"),
                row_after.get("newdate"),
                row_after.get("type"),
                row_after.get("operation"),
                row_after.get("amount"),
                row_after.get("balance"),
                row_after.get("account"),
                trans_id,
            )
        elif op_type == "DELETE":
            rep_q = "DELETE FROM trans WHERE trans_id = %s"
            rep_p = (trans_id,)
        else:
            return

        print(f"[ROUTE_REPLICATE] Calling replicate_to_central for trans_id={trans_id}")
        replicate_to_central(
            source_node=source_node,
            query=rep_q,
            params=rep_p,
            trans_id=trans_id,
            op_type=op_type,
            pk_value=str(trans_id),
            old_amount=old_amt,
            new_amount=new_amt,
        )

def log_transaction_event(node_name, trans_id, op_type, pk_value, old_amount=None, new_amount=None, status="PENDING", error_message=None):
    """Write a row to transaction_log on the given node."""
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if not db.connect():
        return None
    try:
        cur = db.connection.cursor()
        cur.execute(
            """
            INSERT INTO transaction_log
            (trans_id, node, table_name, op_type, pk_value, old_amount, new_amount, status, error_message, started_at, ended_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            (
                trans_id,
                        to_log_node_name(node_name),
                "trans",
                op_type,
                pk_value,
                old_amount,
                new_amount,
                status,
                error_message,
            ),
        )
        db.connection.commit()
        log_id = cur.lastrowid
        cur.close()
        return log_id
    except Exception:
        try:
            db.connection.rollback()
        except Exception:
            pass
        return None
    finally:
        try:
            db.close()
        except Exception:
            pass

def insert_replication_log(source_node, target_node, trans_id, op_type, old_amount, new_amount):
    """Insert PENDING entry into replication_log on the source node and return log_id."""
    db = DatabaseConnection(NODE_CONFIGS[source_node])
    if not db.connect():
        st.warning(f"replication_log insert skipped: cannot connect to {source_node}")
        return None
    try:
        cur = db.connection.cursor()
        cur.execute(
            """
            INSERT INTO replication_log
            (source_node, target_node, trans_id, op_type, old_amount, new_amount, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', NOW())
            """,
            (
                to_log_node_name(source_node),
                to_log_node_name(target_node),
                trans_id,
                op_type,
                old_amount,
                new_amount,
            ),
        )
        db.connection.commit()
        log_id = cur.lastrowid
        cur.close()
        return log_id
    except Exception as e:
        st.warning(f"replication_log insert failed on {source_node}: {e}")
        try:
            db.connection.rollback()
        except Exception:
            pass
        return None
    finally:
        try:
            db.close()
        except Exception:
            pass


def update_replication_log_status(source_node, log_id, status, error_message=None):
    """Update replication_log status for a given log_id on the source node."""
    if log_id is None:
        return
    db = DatabaseConnection(NODE_CONFIGS[source_node])
    if not db.connect():
        return
    try:
        cur = db.connection.cursor()
        cur.execute(
            """
            UPDATE replication_log
            SET status = %s,
                error_message = %s,
                completed_at = CASE WHEN %s = 'SUCCESS' THEN NOW() ELSE completed_at END
            WHERE log_id = %s
            """,
            (status, error_message, status, log_id),
        )
        db.connection.commit()
        cur.close()
    except Exception:
        try:
            db.connection.rollback()
        except Exception:
            pass
    finally:
        try:
            db.close()
        except Exception:
            pass


# CONCURRENCY CONTROL ==============================
def set_isolation_level(connection, level, per_transaction=False): # sets level of isolation
    if connection is None:
        st.warning("set_isolation_level: connection is None")
        return False

    if not isinstance(level, str) or not level.strip():
        st.warning("set_isolation_level: invalid isolation level")
        return False

    normalized = level.strip().upper().replace("_", " ")

    valid_levels = {
        "READ UNCOMMITTED": "READ UNCOMMITTED",
        "READ COMMITTED": "READ COMMITTED",
        "REPEATABLE READ": "REPEATABLE READ",
        "SERIALIZABLE": "SERIALIZABLE"
    }

    sql_level = valid_levels.get(normalized)
    if not sql_level:
        st.warning(f"set_isolation_level: unsupported level '{level}'")
        return False

    if per_transaction:
        stmt = f"SET TRANSACTION ISOLATION LEVEL {sql_level}"
    else:
        stmt = f"SET SESSION TRANSACTION ISOLATION LEVEL {sql_level}"

    cur = None
    try:
        cur = connection.cursor()
        cur.execute(stmt)
        cur.close()
        return True

    except Exception as e:
        st.warning(f"set_isolation_level: failed to set '{sql_level}': {e}")
        try:
            if cur:
                cur.close()
        except:
            pass
        return False

def execute_concurrent_transaction(node_name, query, params, isolation_level, transaction_id):
    start_time = time.time()

    status = "pending"
    error_msg = None
    result = None

    # connect to the specified node
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if not db.connect():
        status = "error"
        error_msg = "Connection to node failed"
        end_time = time.time()
        st.session_state.transaction_log.append({
            "transaction_id": transaction_id,
            "node": node_name,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "duration": f"{(end_time - start_time):.4f}s",
            "isolation_level": isolation_level,
            "status": status,
            "query": query,
            "params": params,
            "error": error_msg
        })
        return {"error": error_msg}

    try:
        # set the isolation level on the DB connection for the next transaction (if provided)
        if isolation_level:
            set_isolation_level(db.connection, isolation_level, per_transaction=True)

        # read or write
        qtype = (query.strip().split()[0].upper() if query and query.strip() else "")
        fetch = True if qtype == "SELECT" else False

        # execute the query (pass isolation_level so execute_query can apply it per-transaction too)
        result = db.execute_query(query, params=params, fetch=fetch, isolation_level=isolation_level)

        # Determine status based on result
        if isinstance(result, dict) and result.get("error"):
            status = "error"
            error_msg = result.get("error")
        else:
            status = "success"

    except Exception as e:
        status = "error"
        error_msg = str(e)
        result = {"error": error_msg}

    finally:
        end_time = time.time()

        # logs transaction
        log_entry = {
            "transaction_id": transaction_id,
            "node": node_name,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "duration": f"{(end_time - start_time):.4f}s",
            "isolation_level": isolation_level,
            "status": status,
            "query": query,
            "params": params
        }
        if error_msg:
            log_entry["error"] = error_msg
        if result is not None:
            log_entry["result"] = result

        st.session_state.transaction_log.append(log_entry)

        # close connection
        try:
            db.close()
        except Exception:
            pass

    return result

def test_concurrent_reads(isolation_level):
    """
    Case #1: Multiple transactions reading the same data
    Tests if concurrent readers see consistent snapshots
    """
    reader_node_1 = "Central Node"
    reader_node_2 = "Node 2"
    
    trans_id = 1
    test_id = f"test-cr-{int(time.time()*1000)}"
    start_time = time.time()
    
    # Verify test data exists on BOTH nodes
    st.info("Verifying test data on both nodes...")
    for node_name in [reader_node_1, reader_node_2]:
        db = DatabaseConnection(NODE_CONFIGS[node_name])
        if db.connect():
            cursor = db.connection.cursor(dictionary=True)
            cursor.execute("SELECT trans_id, balance FROM trans WHERE trans_id = %s", (trans_id,))
            row = cursor.fetchone()
            cursor.close()
            db.close()
            
            if not row:
                st.error(f"❌ trans_id = {trans_id} does NOT exist on {node_name}!")
                return
            elif row['balance'] == 0 or row['balance'] is None:
                db2 = DatabaseConnection(NODE_CONFIGS[node_name])
                if db2.connect():
                    cursor2 = db2.connection.cursor()
                    cursor2.execute("UPDATE trans SET balance = 1000 WHERE trans_id = %s", (trans_id,))
                    db2.connection.commit()
                    cursor2.close()
                    db2.close()
                    st.success(f"{node_name} balance updated to 1000")
    
    # Synchronization
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    
    # Results containers
    results = {
        "reader1_result": None,
        "reader2_result": None
    }
    
    def reader_worker(node_name, result_key, reader_id):
        """Generic reader worker"""
        try:
            barrier.wait(timeout=10)
            
            db = DatabaseConnection(NODE_CONFIGS[node_name])
            if not db.connect():
                with lock:
                    results[result_key] = {"error": "Connection failed"}
                return
            
            set_isolation_level(db.connection, isolation_level, per_transaction=True)
            
            db.connection.start_transaction()
            cursor = db.connection.cursor(dictionary=True)
            
            cursor.execute(f"SELECT * FROM trans WHERE trans_id = {trans_id}")
            row = cursor.fetchone()
            
            with lock:
                results[result_key] = row
            
            db.connection.commit()
            cursor.close()
            db.close()
            
        except Exception as e:
            with lock:
                results[result_key] = {"error": str(e)}
    
    # ===== EXECUTE CONCURRENT READS =====
    t_reader1 = threading.Thread(
        target=reader_worker, 
        args=(reader_node_1, "reader1_result", "R1"),
        daemon=True
    )
    t_reader2 = threading.Thread(
        target=reader_worker, 
        args=(reader_node_2, "reader2_result", "R2"),
        daemon=True
    )
    
    t_reader1.start()
    t_reader2.start()
    
    t_reader1.join(timeout=15)
    t_reader2.join(timeout=15)
    
    # ===== ANALYZE RESULTS =====
    reader1_data = results["reader1_result"]
    reader2_data = results["reader2_result"]
    
    # Extract balances
    def get_balance(result):
        if not result or isinstance(result, dict) and result.get("error"):
            return None
        return result.get("balance")
    
    r1_balance = get_balance(reader1_data)
    r2_balance = get_balance(reader2_data)
    
    consistent = False
    if r1_balance is not None and r2_balance is not None:
        consistent = (r1_balance == r2_balance)
    
    try:
        r1_json = json.dumps(reader1_data, sort_keys=True, default=str) if reader1_data else None
        r2_json = json.dumps(reader2_data, sort_keys=True, default=str) if reader2_data else None
        exact_match = (r1_json == r2_json)
    except:
        exact_match = False
    
    # ===== DETECT ANOMALIES =====
    # For concurrent reads without writes:
    # - Dirty reads: NOT POSSIBLE (no writer)
    # - Non-repeatable reads: NOT APPLICABLE (single read per transaction)
    # - Phantom reads: NOT APPLICABLE (single row query)
    # - Inconsistent reads: Readers see DIFFERENT data for same row
    
    dirty_read = False  # No writer in this test
    non_repeatable_read = False  # Each reader only reads once
    inconsistent_reads = not consistent  # Different balances = inconsistent
    
    summary = {
        "test_case": "Case #1: Concurrent Reads",
        "isolation_level": isolation_level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "reader1_node": reader_node_1,
        "reader2_node": reader_node_2,
        "reader1_balance": r1_balance,
        "reader2_balance": r2_balance,
        "reader1_full_data": reader1_data,
        "reader2_full_data": reader2_data,
        "balances_match": consistent,
        "exact_data_match": exact_match,
        "dirty_read": dirty_read,
        "non_repeatable_read": non_repeatable_read,
        "inconsistent_reads": inconsistent_reads
    }
    
    # ===== LOG TO DATABASE & SESSION STATE =====
    end_time = time.time()
    duration = end_time - start_time
    
    # Log to database
    for node in [reader_node_1, reader_node_2]:
        log_transaction_event(
            node_name=node,
            trans_id=trans_id,
            op_type="SELECT",
            pk_value=str(trans_id),
            old_amount=None,
            new_amount=None,
            status="completed" if consistent else "inconsistent",
            error_message=None if consistent else "Inconsistent read detected"
        )
    
    # Log test summary to session state
    st.session_state.transaction_log.append({
        "transaction_id": test_id,
        "node": f"{reader_node_1} & {reader_node_2}",
        "timestamp": summary["timestamp"],
        "duration": f"{duration:.4f}s",
        "isolation_level": isolation_level,
        "status": "completed",
        "query": "Concurrent Reads Test (2 Readers, No Writer)",
        "params": (trans_id,),
        "test_case": "Case #1: Concurrent Reads",
        "test_summary": summary
    })

def test_read_write_conflict(isolation_level):
    """
    Case #2: Read + Write Conflict
    
    Tests for:
    - Dirty reads: Reader sees uncommitted data from writer
    - Non-repeatable reads: Reader gets different values in same transaction
    - Phantom reads: Reader sees rows that appear/disappear
    
    Strategy:
    1. Start reader transaction and perform first read
    2. Start writer transaction and modify data (but don't commit yet)
    3. Reader performs second read (should not see uncommitted changes in most isolation levels)
    4. Writer commits
    5. Reader performs third read (may or may not see changes depending on isolation level)
    6. Analyze results for anomalies
    """
    reader_node = "Node 2"
    writer_node = "Central Node"
    
    trans_id = 1
    test_id = f"test-rw-{int(time.time()*1000)}"
    start_time = time.time()
    
    # Results containers
    reader_results = {
        "read1_before_write": None,  # Before writer starts
        "read2_during_write": None,  # While writer has uncommitted changes
        "read3_after_commit": None   # After writer commits
    }
    writer_result = {"success": False, "error": None}
    writer_row_before = None
    writer_row_after = None
    
    # Synchronization
    barrier_start = threading.Barrier(2)  # Both start together
    writer_uncommitted = threading.Event()  # Writer signals it has uncommitted data
    writer_committed = threading.Event()    # Writer signals commit is done
    lock = threading.Lock()
    
    def reader_worker():
        """
        Reader performs 3 reads:
        1. Before writer starts writing
        2. While writer has uncommitted changes
        3. After writer commits
        """
        nonlocal reader_results
        
        db = None
        try:
            # Connect and set isolation level
            db = DatabaseConnection(NODE_CONFIGS[reader_node])
            if not db.connect():
                with lock:
                    reader_results["error"] = "Connection failed"
                return
            
            # Set isolation level for this transaction
            set_isolation_level(db.connection, isolation_level, per_transaction=True)
            
            # Wait for both threads to be ready
            barrier_start.wait(timeout=10)
            
            # Start explicit transaction
            db.begin_transaction()
            cursor = db.connection.cursor(dictionary=True)
            
            # READ 1: Initial read before writer does anything
            cursor.execute(f"SELECT balance FROM trans WHERE trans_id = {trans_id}")
            read1 = cursor.fetchone()
            with lock:
                reader_results["read1_before_write"] = read1.copy() if read1 else None
            
            # Wait for writer to have uncommitted data
            writer_uncommitted.wait(timeout=10)
            time.sleep(0.5)  # Small delay to ensure writer's changes are in progress
            
            # READ 2: Read while writer has uncommitted changes
            # This tests for DIRTY READS
            cursor.execute(f"SELECT balance FROM trans WHERE trans_id = {trans_id}")
            read2 = cursor.fetchone()
            with lock:
                reader_results["read2_during_write"] = read2.copy() if read2 else None
            
            # Wait for writer to commit
            writer_committed.wait(timeout=10)
            time.sleep(0.5)  # Small delay to ensure commit is processed
            
            # READ 3: Read after writer commits
            # This tests for NON-REPEATABLE READS
            cursor.execute(f"SELECT balance FROM trans WHERE trans_id = {trans_id}")
            read3 = cursor.fetchone()
            with lock:
                reader_results["read3_after_commit"] = read3.copy() if read3 else None
            
            # Commit reader transaction
            db.commit_transaction()
            cursor.close()
            
        except Exception as e:
            with lock:
                reader_results["error"] = str(e)
            if db and db.in_transaction:
                db.rollback_transaction()
        finally:
            if db:
                db.close()
    
    def writer_worker():
        """
        Writer modifies data in explicit transaction:
        1. Start transaction
        2. Update balance
        3. Signal that data is uncommitted
        4. Wait a bit (to give reader time to see uncommitted data)
        5. Commit
        6. Signal commit is done
        """
        nonlocal writer_result, writer_row_before, writer_row_after
        
        db = None
        try:
            # Connect
            db = DatabaseConnection(NODE_CONFIGS[writer_node])
            if not db.connect():
                with lock:
                    writer_result = {"success": False, "error": "Connection failed"}
                return
            
            # Set isolation level
            set_isolation_level(db.connection, isolation_level, per_transaction=True)
            
            # Wait for both threads to be ready
            barrier_start.wait(timeout=10)
            
            # Capture row BEFORE update
            row_before = fetch_trans_row(writer_node, trans_id)
            
            # Start explicit transaction
            db.begin_transaction()
            cursor = db.connection.cursor()
            
            # Perform UPDATE but don't commit yet
            cursor.execute(f"UPDATE trans SET balance = balance + 100 WHERE trans_id = {trans_id}")
            
            # Signal that we have uncommitted data
            writer_uncommitted.set()
            
            # Wait a bit to simulate slow transaction (gives reader time to attempt dirty read)
            time.sleep(2)
            
            # Now commit the transaction
            db.commit_transaction()
            cursor.close()
            
            # Signal that commit is done
            writer_committed.set()
            
            # Capture row AFTER update
            row_after = fetch_trans_row(writer_node, trans_id)
            
            with lock:
                writer_result = {"success": True, "error": None}
                writer_row_before = row_before
                writer_row_after = row_after
            
        except Exception as e:
            with lock:
                writer_result = {"success": False, "error": str(e)}
            if db and db.in_transaction:
                db.rollback_transaction()
            writer_uncommitted.set()  # Unblock reader even if we fail
            writer_committed.set()
        finally:
            if db:
                db.close()
    
    # ===== EXECUTE CONCURRENTLY =====
    t_reader = threading.Thread(target=reader_worker, daemon=True)
    t_writer = threading.Thread(target=writer_worker, daemon=True)
    
    t_reader.start()
    t_writer.start()
    
    t_reader.join(timeout=30)
    t_writer.join(timeout=30)
    
    # ===== ANALYZE RESULTS =====
    def extract_balance(result):
        if not result or isinstance(result, dict) and result.get("error"):
            return None
        return result.get("balance")
    
    b1 = extract_balance(reader_results.get("read1_before_write"))
    b2 = extract_balance(reader_results.get("read2_during_write"))
    b3 = extract_balance(reader_results.get("read3_after_commit"))
    
    # ===== DETECT ANOMALIES =====
    dirty_read = False
    non_repeatable_read = False
    
    # Dirty read: If read2 (during uncommitted write) shows changed data
    # In READ UNCOMMITTED, b2 might equal b1 + 100 (seeing uncommitted data)
    # In higher isolation levels, b2 should equal b1 (blocking or snapshot)
    if b1 is not None and b2 is not None:
        if b2 != b1:
            dirty_read = True  # Reader saw uncommitted data
    
    # Non-repeatable read: If read1 and read3 differ (both in same reader transaction)
    # In REPEATABLE READ or SERIALIZABLE, b1 should equal b3 (snapshot isolation)
    # In READ COMMITTED, b1 and b3 can differ (sees committed changes)
    if b1 is not None and b3 is not None:
        if b3 != b1:
            non_repeatable_read = True
    
    # ===== BUILD SUMMARY =====
    summary = {
        "test_case": "Case #2: Read + Write Conflict",
        "isolation_level": isolation_level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "reader_node": reader_node,
        "writer_node": writer_node,
        "read1_before_write": b1,
        "read2_during_write": b2,
        "read3_after_commit": b3,
        "writer_success": writer_result.get("success", False),
        "writer_error": writer_result.get("error"),
        "dirty_read": dirty_read,
        "non_repeatable_read": non_repeatable_read,
        "expected_behavior": {
            "READ UNCOMMITTED": "May see dirty reads (b2 != b1) and non-repeatable reads (b3 != b1)",
            "READ COMMITTED": "No dirty reads (b2 == b1), but may see non-repeatable reads (b3 != b1)",
            "REPEATABLE READ": "No dirty reads (b2 == b1) and no non-repeatable reads (b3 == b1)",
            "SERIALIZABLE": "No anomalies - full isolation (b1 == b2 == b3 or blocking)"
        }[isolation_level]
    }
    
    # ===== LOG TO DATABASE & SESSION STATE =====
    end_time = time.time()
    duration = end_time - start_time
    
    # Log reader transaction
    log_transaction_event(
        node_name=reader_node,
        trans_id=trans_id,
        op_type="SELECT",
        pk_value=str(trans_id),
        old_amount=b1,
        new_amount=b3,
        status="completed",
        error_message=reader_results.get("error")
    )
    
    # Log writer transaction
    if writer_result.get("success"):
        log_transaction_event(
            node_name=writer_node,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=writer_row_before.get("balance") if writer_row_before else None,
            new_amount=writer_row_after.get("balance") if writer_row_after else None,
            status="COMMITTED",
            error_message=None
        )
        
        # Replicate writer's changes
        if writer_row_after:
            route_and_replicate_write(
                source_node=writer_node,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=writer_row_before,
                row_after=writer_row_after
            )
    else:
        log_transaction_event(
            node_name=writer_node,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=None,
            new_amount=None,
            status="FAILED",
            error_message=writer_result.get("error")
        )
    
    # Log test summary to session state
    st.session_state.transaction_log.append({
        "transaction_id": test_id,
        "node": f"{reader_node} (R) & {writer_node} (W)",
        "timestamp": summary["timestamp"],
        "duration": f"{duration:.4f}s",
        "isolation_level": isolation_level,
        "status": "completed",
        "query": "RW Test: 3 Reads + 1 Write",
        "params": (trans_id,),
        "test_case": "Case #2: Read + Write Conflict",
        "test_summary": summary
    })
    
    # Display immediate results
    st.success("✅ Test completed!")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Read 1 (Before)", f"${b1:,.2f}" if b1 else "N/A")
    with col2:
        st.metric("Read 2 (During Write)", f"${b2:,.2f}" if b2 else "N/A", 
                  delta=f"+${b2-b1:,.2f}" if (b1 and b2) else None)
    with col3:
        st.metric("Read 3 (After Commit)", f"${b3:,.2f}" if b3 else "N/A",
                  delta=f"+${b3-b1:,.2f}" if (b1 and b3) else None)

def test_write_write_conflict(isolation_level):
    """
    Case #3: Write + Write Conflict (Lost Updates)
    
    Tests for:
    - Lost updates: One writer's changes are overwritten by another
    - Deadlocks: Writers block each other indefinitely
    - Serialization failures: Database rejects conflicting writes
    
    Strategy:
    1. Both writers read initial balance
    2. Both calculate new balance based on initial value
    3. Both try to write back (classic lost update scenario)
    4. Check if final balance reflects BOTH updates or just one (lost update)
    """
    node_a = "Node 2"
    node_b = "Central Node"
    
    trans_id = 1
    test_id = f"test-ww-{int(time.time()*1000)}"
    start_time = time.time()
    
    # Results containers
    writer_results = {"A": {"success": False, "error": None}, "B": {"success": False, "error": None}}
    writer_rows_before = {"A": None, "B": None}
    writer_rows_after = {"A": None, "B": None}
    initial_balance = None
    final_balance = None
    
    # Synchronization
    barrier_start = threading.Barrier(2)  # Both start together
    both_read = threading.Event()  # Both have read initial value
    writer_a_done = threading.Event()
    writer_b_done = threading.Event()
    lock = threading.Lock()
    reads_completed = {"count": 0}
    
    def writer_a():
        """
        Writer A: Read-Modify-Write pattern
        1. Read current balance
        2. Add 100
        3. Write back
        """
        nonlocal writer_results, writer_rows_before, writer_rows_after
        
        db = None
        try:
            # Connect
            db = DatabaseConnection(NODE_CONFIGS[node_a])
            if not db.connect():
                with lock:
                    writer_results["A"] = {"success": False, "error": "Connection failed"}
                return
            
            # Set isolation level
            set_isolation_level(db.connection, isolation_level, per_transaction=True)
            
            # Wait for both threads to be ready
            barrier_start.wait(timeout=10)
            
            # Start explicit transaction
            db.begin_transaction()
            cursor = db.connection.cursor(dictionary=True)
            
            # READ: Get current balance
            cursor.execute(f"SELECT * FROM trans WHERE trans_id = {trans_id}")
            row_before = cursor.fetchone()
            
            if not row_before:
                raise Exception("Transaction row not found")
            
            current_balance = row_before.get("balance", 0)
            
            with lock:
                writer_rows_before["A"] = row_before.copy()
                reads_completed["count"] += 1
                if reads_completed["count"] == 2:
                    both_read.set()
            
            # Wait for both writers to have read the initial value
            both_read.wait(timeout=10)
            
            # Simulate some processing time
            time.sleep(0.5)
            
            # MODIFY: Calculate new balance
            new_balance = current_balance + 100
            
            # WRITE: Update with new balance
            cursor.execute(
                f"UPDATE trans SET balance = %s WHERE trans_id = {trans_id}",
                (new_balance,)
            )
            
            # Small delay before commit to increase chance of conflict
            time.sleep(1)
            
            # Commit transaction
            db.commit_transaction()
            cursor.close()
            
            # Read final state
            row_after = fetch_trans_row(node_a, trans_id)
            
            with lock:
                writer_results["A"] = {"success": True, "error": None}
                writer_rows_after["A"] = row_after
            
            writer_a_done.set()
            
        except Exception as e:
            with lock:
                writer_results["A"] = {"success": False, "error": str(e)}
            if db and db.in_transaction:
                db.rollback_transaction()
            writer_a_done.set()
        finally:
            if db:
                db.close()
    
    def writer_b():
        """
        Writer B: Same Read-Modify-Write pattern
        1. Read current balance (should be same as Writer A)
        2. Add 100
        3. Write back
        """
        nonlocal writer_results, writer_rows_before, writer_rows_after
        
        db = None
        try:
            # Connect
            db = DatabaseConnection(NODE_CONFIGS[node_b])
            if not db.connect():
                with lock:
                    writer_results["B"] = {"success": False, "error": "Connection failed"}
                return
            
            # Set isolation level
            set_isolation_level(db.connection, isolation_level, per_transaction=True)
            
            # Wait for both threads to be ready
            barrier_start.wait(timeout=10)
            
            # Start explicit transaction
            db.begin_transaction()
            cursor = db.connection.cursor(dictionary=True)
            
            # READ: Get current balance
            cursor.execute(f"SELECT * FROM trans WHERE trans_id = {trans_id}")
            row_before = cursor.fetchone()
            
            if not row_before:
                raise Exception("Transaction row not found")
            
            current_balance = row_before.get("balance", 0)
            
            with lock:
                writer_rows_before["B"] = row_before.copy()
                reads_completed["count"] += 1
                if reads_completed["count"] == 2:
                    both_read.set()
            
            # Wait for both writers to have read the initial value
            both_read.wait(timeout=10)
            
            # Simulate some processing time
            time.sleep(0.5)
            
            # MODIFY: Calculate new balance
            new_balance = current_balance + 100
            
            # WRITE: Update with new balance
            cursor.execute(
                f"UPDATE trans SET balance = %s WHERE trans_id = {trans_id}",
                (new_balance,)
            )
            
            # Small delay before commit to increase chance of conflict
            time.sleep(1)
            
            # Commit transaction
            db.commit_transaction()
            cursor.close()
            
            # Read final state
            row_after = fetch_trans_row(node_b, trans_id)
            
            with lock:
                writer_results["B"] = {"success": True, "error": None}
                writer_rows_after["B"] = row_after
            
            writer_b_done.set()
            
        except Exception as e:
            with lock:
                writer_results["B"] = {"success": False, "error": str(e)}
            if db and db.in_transaction:
                db.rollback_transaction()
            writer_b_done.set()
        finally:
            if db:
                db.close()
    
    # ===== EXECUTE CONCURRENT WRITERS =====
    tA = threading.Thread(target=writer_a, daemon=True)
    tB = threading.Thread(target=writer_b, daemon=True)
    
    tA.start()
    tB.start()
    
    tA.join(timeout=30)
    tB.join(timeout=30)
    
    # ===== ANALYZE RESULTS =====
    # Get initial balance (both writers should have read the same value)
    initial_balance_a = writer_rows_before["A"].get("balance") if writer_rows_before["A"] else 0
    initial_balance_b = writer_rows_before["B"].get("balance") if writer_rows_before["B"] else 0
    
    # In proper concurrent execution, both should read the same initial value
    initial_balance = initial_balance_a if initial_balance_a == initial_balance_b else None
    
    # Read final balance from database
    final_row = fetch_trans_row(node_a, trans_id)
    final_balance = final_row.get("balance") if final_row else None
    
    # ===== DETECT ANOMALIES =====
    lost_update = False
    deadlock_occurred = False
    serialization_failure = False
    
    success_a = writer_results["A"].get("success", False)
    success_b = writer_results["B"].get("success", False)
    error_a = writer_results["A"].get("error", "")
    error_b = writer_results["B"].get("error", "")
    
    # Check for deadlock
    if ("deadlock" in str(error_a).lower() or "deadlock" in str(error_b).lower()):
        deadlock_occurred = True
    
    # Check for serialization failure
    if ("serialization" in str(error_a).lower() or "serialization" in str(error_b).lower() or
        "lock timeout" in str(error_a).lower() or "lock timeout" in str(error_b).lower()):
        serialization_failure = True
    
    # Check for lost update
    if initial_balance is not None and final_balance is not None and success_a and success_b:
        expected_balance = initial_balance + 200  # Both add 100
        
        # Lost update if final != expected (one update was lost)
        if final_balance != expected_balance:
            # Also check if it only reflects one update
            if final_balance == initial_balance + 100:
                lost_update = True
    
    # ===== BUILD SUMMARY =====
    summary = {
        "test_case": "Case #3: Write + Write Conflict",
        "isolation_level": isolation_level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "node_a": node_a,
        "node_b": node_b,
        "initial_balance_a": initial_balance_a,
        "initial_balance_b": initial_balance_b,
        "final_balance": final_balance,
        "expected_balance": (initial_balance + 200) if initial_balance else None,
        "writer_a_success": success_a,
        "writer_b_success": success_b,
        "writer_a_error": error_a,
        "writer_b_error": error_b,
        "lost_update": lost_update,
        "deadlock": deadlock_occurred,
        "serialization_failure": serialization_failure,
        "consistent_reads": initial_balance_a == initial_balance_b,
        "expected_behavior": {
            "READ UNCOMMITTED": "Lost updates likely - final = initial + 100 (not +200)",
            "READ COMMITTED": "Lost updates likely - final = initial + 100 (not +200)",
            "REPEATABLE READ": "May cause deadlock or one writer waits; final should = initial + 200",
            "SERIALIZABLE": "One writer will fail or wait; prevents lost updates"
        }[isolation_level]
    }
    
    # ===== LOG TO DATABASE & SESSION STATE =====
    end_time = time.time()
    duration = end_time - start_time
    
    # Log Writer A
    if success_a:
        log_transaction_event(
            node_name=node_a,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=initial_balance_a,
            new_amount=writer_rows_after["A"].get("balance") if writer_rows_after["A"] else None,
            status="COMMITTED",
            error_message=None
        )
        
        # Replicate Writer A's changes
        if writer_rows_after["A"]:
            route_and_replicate_write(
                source_node=node_a,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=writer_rows_before["A"],
                row_after=writer_rows_after["A"]
            )
    else:
        log_transaction_event(
            node_name=node_a,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=initial_balance_a,
            new_amount=None,
            status="FAILED",
            error_message=error_a
        )
    
    # Log Writer B
    if success_b:
        log_transaction_event(
            node_name=node_b,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=initial_balance_b,
            new_amount=writer_rows_after["B"].get("balance") if writer_rows_after["B"] else None,
            status="COMMITTED",
            error_message=None
        )
        
        # Replicate Writer B's changes
        if writer_rows_after["B"]:
            route_and_replicate_write(
                source_node=node_b,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=writer_rows_before["B"],
                row_after=writer_rows_after["B"]
            )
    else:
        log_transaction_event(
            node_name=node_b,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=initial_balance_b,
            new_amount=None,
            status="FAILED",
            error_message=error_b
        )
    
    # Log test summary to session state
    st.session_state.transaction_log.append({
        "transaction_id": test_id,
        "node": f"{node_a} (A) & {node_b} (B)",
        "timestamp": summary["timestamp"],
        "duration": f"{duration:.4f}s",
        "isolation_level": isolation_level,
        "status": "completed",
        "query": "WW Test: 2 Concurrent Writers (Read-Modify-Write)",
        "params": (trans_id,),
        "test_case": "Case #3: Write + Write Conflict",
        "test_summary": summary
    })
    
    # ===== DISPLAY IMMEDIATE RESULTS =====
    st.success("✅ Test completed!")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Initial Balance", f"${initial_balance:,.2f}" if initial_balance else "N/A")
    with col2:
        st.metric("Expected Balance", f"${initial_balance + 200:,.2f}" if initial_balance else "N/A",
                  help="Initial + 200 (both writers add 100)")
    with col3:
        st.metric("Final Balance", f"${final_balance:,.2f}" if final_balance else "N/A",
                  delta=f"+${final_balance - initial_balance:,.2f}" if (initial_balance and final_balance) else None)
    
    st.divider()

# REPLICATION MODULE ==============================
def replicate_to_central(source_node, query, params, trans_id=None, op_type=None, pk_value=None, old_amount=None, new_amount=None):
    """
    Replicate a write operation from Node 2/3 to Central Node
    
    Steps:
    1. Log PENDING replication attempt
    2. Execute the write on Central Node
    3. Update replication_log with SUCCESS/FAILED
    4. Return success flag and error (if any)
    """
    print(f"[REPLICATION] Attempting to replicate trans_id {trans_id} from {source_node} to Central Node")
    
    log_id = insert_replication_log(
        source_node=source_node,
        target_node="Central Node",
        trans_id=trans_id,
        op_type=op_type,
        old_amount=old_amount,
        new_amount=new_amount,
    )
    
    print(f"[REPLICATION] Replication log entry created with log_id: {log_id}")

    success = False
    error_msg = None
    try:
        # Check if Central Node is in simulated failure state
        if not st.session_state.node_status.get("Central Node", True):
            raise Exception("Central Node is offline (simulated failure)")
        
        db = DatabaseConnection(NODE_CONFIGS["Central Node"])
        if not db.connect():
            raise Exception("Cannot connect to Central Node")
        res = db.execute_query(query, params=params, fetch=False)
        if isinstance(res, dict) and res.get("error"):
            raise Exception(res.get("error"))
        success = True
        # Log on target (Central) for the applied write
        log_transaction_event(
            node_name="Central Node",
            trans_id=trans_id,
            op_type=op_type or "UNKNOWN",
            pk_value=pk_value or (str(trans_id) if trans_id is not None else None),
            old_amount=old_amount,
            new_amount=new_amount,
            status="COMMITTED"
        )
    except Exception as e:
        error_msg = str(e)
        print(f"[REPLICATION] Replication failed: {error_msg}")
    finally:
        if log_id is not None:
            update_replication_log_status(
                source_node=source_node,
                log_id=log_id,
                status="SUCCESS" if success else "FAILED",
                error_message=error_msg,
            )
            print(f"[REPLICATION] Updated replication_log[{log_id}] with status: {'SUCCESS' if success else 'FAILED'}")
        else:
            st.warning(f"No replication_log row created for trans {trans_id} from {source_node}; replication {'succeeded' if success else 'failed'}")
        try:
            db.close()
        except Exception:
            pass
    return success, error_msg

def replicate_from_central(target_node, query, params, trans_id=None, op_type=None, pk_value=None, old_amount=None, new_amount=None):
    """
    Replicate a write operation from Central Node to Node 2/3
    
    Steps:
    1. Log PENDING replication attempt
    2. Execute the write on target node
    3. Update replication_log with SUCCESS/FAILED
    4. Return success flag and error (if any)
    """
    log_id = insert_replication_log(
        source_node="Central Node",
        target_node=target_node,
        trans_id=trans_id,
        op_type=op_type,
        old_amount=old_amount,
        new_amount=new_amount,
    )

    success = False
    error_msg = None
    try:
        # Check if target node is in simulated failure state
        if not st.session_state.node_status.get(target_node, True):
            raise Exception(f"{target_node} is offline (simulated failure)")
        
        db = DatabaseConnection(NODE_CONFIGS[target_node])
        if not db.connect():
            raise Exception(f"Cannot connect to {target_node}")
        res = db.execute_query(query, params=params, fetch=False)
        if isinstance(res, dict) and res.get("error"):
            raise Exception(res.get("error"))
        success = True
        # Log on target node for the applied write
        log_transaction_event(
            node_name=target_node,
            trans_id=trans_id,
            op_type=op_type or "UNKNOWN",
            pk_value=pk_value or (str(trans_id) if trans_id is not None else None),
            old_amount=old_amount,
            new_amount=new_amount,
            status="COMMITTED"
        )
    except Exception as e:
        error_msg = str(e)
    finally:
        if log_id is not None:
            update_replication_log_status(
                source_node="Central Node",
                log_id=log_id,
                status="SUCCESS" if success else "FAILED",
                error_message=error_msg,
            )
        else:
            st.warning(f"No replication_log row created for trans {trans_id} from Central to {target_node}; replication {'succeeded' if success else 'failed'}")
        try:
            db.close()
        except Exception:
            pass
    return success, error_msg

# RECOVERY MODULE ==============================
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
    print(f"\n{'='*60}")
    print(f"[FAILURE SIMULATION] Starting failure simulation for {node_name}")
    print(f"{'='*60}")
    
    try: 
        # Get timestamp from MySQL to ensure consistency
        db = DatabaseConnection(NODE_CONFIGS["Central Node"])
        if db.connect():
            failure_time = get_mysql_now(db.connection)
            
            # Store in session state
            st.session_state.node_status[node_name] = False
            st.session_state.simulated_failures[node_name] = failure_time
            
            print(f"[FAILURE SIMULATION] Downtime started at: {failure_time}")
            
            cur = db.connection.cursor()
            cur.execute(
                "INSERT INTO recovery_log (node, downtime_start, status) VALUES (%s, %s, 'FAILED')",
                (to_log_node_name(node_name), failure_time)
            )
            db.connection.commit()
            cur.close()
            db.close()
        else:
            # Fallback if can't connect
            st.session_state.node_status[node_name] = False
            st.session_state.simulated_failures[node_name] = datetime.now()
    except Exception as e:
        print(f"[FAILURE SIMULATION] Exception: {e}")
        st.warning(f"Failed to log failure for {node_name}: {e}")

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
    print(f"\n{'='*60}")
    print(f"[RECOVERY] Starting recovery process for {node_name}")
    print(f"{'='*60}")
    
    st.session_state.node_status[node_name] = True
    print(f"[RECOVERY] Session state updated: node_status[{node_name}] = True")
    
    # TODO (jeff): Add recovery logic
    # 1. Get all missed transactions while node was down
    # 2. Replay them in order
    # 3. Handle any conflicts
    if node_name not in st.session_state.simulated_failures:
        print(f"[RECOVERY] {node_name} was never marked as failed")
        st.warning(f"{node_name} was never marked as failed; cannot simulate recovery")
        return 
    downtime_start = st.session_state.simulated_failures[node_name]

    # Get downtime_end from MySQL for consistency
    db_temp = DatabaseConnection(NODE_CONFIGS["Central Node"])
    if db_temp.connect():
        downtime_end = get_mysql_now(db_temp.connection)
        db_temp.close()
    else:
        downtime_end = datetime.now()  # fallback

    downtime_duration = downtime_end - downtime_start
    print(f"[RECOVERY] Downtime window: {downtime_start} to {downtime_end}")
    print(f"[RECOVERY] Total downtime duration: {downtime_duration}")

    if node_name == "Central Node":
        print(f"[RECOVERY] Path A: Recovering Central Node from slave nodes")
        source_nodes = ["Node 2", "Node 3"]
        replayed_count = 0

        for src in source_nodes:
            print(f"\n[RECOVERY] Checking {src} for FAILED replications...")
            db_src = DatabaseConnection(NODE_CONFIGS[src])
            if not db_src.connect():
                print(f"[RECOVERY] Could not connect to {src}, skipping")
                continue
            #Query replication_log on source node for FAILED replications to Central during downtime
            curr = db_src.connection.cursor(dictionary=True)
            
            # Debug: Check what's in the replication_log
            curr.execute("SELECT log_id, trans_id, target_node, status, created_at FROM replication_log ORDER BY created_at DESC LIMIT 5")
            debug_logs = curr.fetchall()
            print(f"[RECOVERY] Recent replication_log entries on {src}: {debug_logs}")
            
            # Query for FAILED replications - don't use timestamp comparison due to timezone issues
            # Instead, get all FAILED replications that haven't been replayed yet
            # Note: Successfully replayed entries are marked as 'REPLAYED' to prevent re-processing
            curr.execute(
                """
                SELECT log_id, trans_id, op_type, old_amount, new_amount, created_at
                FROM replication_log
                WHERE target_node = %s
                AND created_at >= %s
                AND status = 'FAILED'
                ORDER BY created_at ASC
                """,
                (to_log_node_name("Central Node"), downtime_start)
            )
            failed_reps = curr.fetchall()
            curr.close()
            print(f"[RECOVERY] Found {len(failed_reps)} FAILED replications from {src}")
            
            # Replay Each missed transaction on Central Node
            for rep in failed_reps:
                print(f"\n[RECOVERY] Replaying transaction {rep['trans_id']} from {src}...")
                print(f"[RECOVERY]   - Operation: {rep['op_type']}")
                print(f"[RECOVERY]   - Replication Log ID: {rep['log_id']}")
                trans_id = rep['trans_id']

                row = fetch_trans_row(src, trans_id)
                if not row:
                    continue
                if rep['op_type'] == 'INSERT':
                    query = """
                    INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    account_id=VALUES(account_id),
                    newdate=VALUES(newdate),
                    type=VALUES(type),
                    operation=VALUES(operation),
                    amount=VALUES(amount),
                    balance=VALUES(balance),
                    account=VALUES(account)
                    """
                    params = (row['trans_id'], row['account_id'], row['newdate'], row['type'], row['operation'], row['amount'], row['balance'], row['account'])
                elif rep['op_type'] == 'UPDATE':
                    query = """
                        UPDATE trans SET account_id=%s, newdate=%s, type=%s, operation=%s, amount=%s, balance=%s, account=%s
                        WHERE trans_id=%s
                    """
                    params = (row['account_id'], row['newdate'], row['type'], row['operation'], row['amount'], row['balance'], row['account'], trans_id)
                elif rep['op_type'] == 'DELETE':
                    query = "DELETE FROM trans WHERE trans_id=%s"
                    params = (trans_id,)
                else:
                    continue

                db_central = DatabaseConnection(NODE_CONFIGS["Central Node"])
                if db_central.connect():
                    result = db_central.execute_query(query, params=params, fetch=False)
                    if not (isinstance(result, dict) and result.get("error")):
                        replayed_count += 1
                        print(f"[RECOVERY]   Successfully replayed trans_id {rep['trans_id']}")
                        # Mark as REPLAYED in replication_log (this prevents re-processing)
                        update_replication_log_status(
                            src, rep['log_id'], "REPLAYED")
                        print(f"[RECOVERY]   Marked replication_log[{rep['log_id']}] as REPLAYED")
                    else:
                        print(f"[RECOVERY]   Failed to replay trans_id {rep['trans_id']}: {result.get('error')}")
                    db_central.close()
                else:
                    print(f"[RECOVERY]   Could not connect to Central Node for replay")
            db_src.close()
            print(f"[RECOVERY] Finished processing {src}")
    else:
        print(f"[RECOVERY] Path B: Recovering {node_name} from Central Node")
        db_central = DatabaseConnection(NODE_CONFIGS["Central Node"])
        if not db_central.connect():
            print(f"[RECOVERY] Cannot connect to Central Node for recovery")
            st.warning("Cannot connect to Central Node for recovery")
            return
        print(f"[RECOVERY] Connected to Central Node")
        # Query replication_log on Central Node for FAILED replications to this node during downtime
        curr = db_central.connection.cursor(dictionary=True)
        
        # Debug: Check what's in the replication_log
        curr.execute("SELECT log_id, trans_id, target_node, status, created_at FROM replication_log ORDER BY created_at DESC LIMIT 5")
        debug_logs = curr.fetchall()
        print(f"[RECOVERY] Recent replication_log entries on Central Node: {debug_logs}")
        
        # Query for FAILED replications - don't use timestamp comparison due to timezone issues
        # Note: Successfully replayed entries are marked as 'REPLAYED' to prevent re-processing
        curr.execute(
            """
            SELECT log_id, trans_id, op_type, old_amount, new_amount, created_at
            FROM replication_log
            WHERE target_node = %s
            AND created_at >= %s
            AND status = 'FAILED'
            ORDER BY created_at ASC
            """,
            (to_log_node_name(node_name), downtime_start)
        )
        failed_reps = curr.fetchall()
        curr.close()
        print(f"[RECOVERY] Found {len(failed_reps)} FAILED replications to {node_name}")
        replayed_count = 0

        #Replay each missed transaction on recovered node
        for rep in failed_reps:
            print(f"\n[RECOVERY] Replaying transaction {rep['trans_id']} to {node_name}...")
            print(f"[RECOVERY]   - Operation: {rep['op_type']}")
            print(f"[RECOVERY]   - Replication Log ID: {rep['log_id']}")
            trans_id = rep['trans_id']

            row = fetch_trans_row("Central Node", trans_id)
            if not row:
                continue

            #Reconstruct query based on op_type
            if rep['op_type'] == 'INSERT':
                query = """
                    INSERT INTO trans(trans_id, account_id, newdate, type, operation, amount, balance, account)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    account_id=VALUES(account_id),
                    newdate=VALUES(newdate),
                    type=VALUES(type),
                    operation=VALUES(operation),
                    amount=VALUES(amount),
                    balance=VALUES(balance),
                    account=VALUES(account)
                """
                params = (row['trans_id'], row['account_id'], row['newdate'], row['type'], row['operation'], row['amount'], row['balance'], row['account'])
            elif rep['op_type'] == 'UPDATE':
                query = """
                    UPDATE trans SET account_id=%s, newdate=%s, type=%s, operation=%s, amount=%s, balance=%s, account=%s
                    WHERE trans_id=%s
                """
                params = (row['account_id'], row['newdate'], row['type'], row['operation'], row['amount'], row['balance'], row['account'], trans_id)
            elif rep['op_type'] == 'DELETE':
                query = "DELETE FROM trans WHERE trans_id=%s"
                params = (trans_id,)
            else:
                continue
            db_node = DatabaseConnection(NODE_CONFIGS[node_name])
            if db_node.connect():
                result = db_node.execute_query(query, params=params, fetch=False)
                if not (isinstance(result, dict) and result.get("error")):
                    replayed_count += 1
                    print(f"[RECOVERY]   Successfully replayed trans_id {rep['trans_id']}")
                    # Mark as REPLAYED in replication_log
                    update_replication_log_status(
                        "Central Node", rep['log_id'], "REPLAYED")
                    print(f"[RECOVERY]   Marked replication_log[{rep['log_id']}] as REPLAYED")
                else:
                    print(f"[RECOVERY]   Failed to replay trans_id {rep['trans_id']}: {result.get('error')}")
                db_node.close()
            else:
                print(f"[RECOVERY]   Could not connect to {node_name} for replay")
        db_central.close()

    print(f"\n[RECOVERY] Updating recovery_log in database...")
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if db.connect():
        curr = db.connection.cursor()
        curr.execute(
            """
            UPDATE recovery_log
            SET downtime_end = NOW(), 
                replayed_count = %s,
                status = 'SUCCESS'
            WHERE node = %s
                AND status = 'FAILED'
            ORDER BY downtime_start DESC
            LIMIT 1
            """,
            (replayed_count, to_log_node_name(node_name),)
        )
        db.connection.commit()
        print(f"[RECOVERY] Updated recovery_log: replayed_count={replayed_count}, status=SUCCESS")
        curr.close()
        db.close()
    else:
        print(f"[RECOVERY] Could not connect to {node_name} to update recovery_log")

    # Also add to session state for immediate display
    recovery_log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "node": node_name,
        "downtime": str(downtime_end - downtime_start),
        "missed_transactions": replayed_count,
        "status": "SUCCESS"
    }
    st.session_state.recovery_log.append(recovery_log)
    del st.session_state.simulated_failures[node_name]
    
    print(f"\n{'='*60}")
    print(f"[RECOVERY] Recovery completed for {node_name}")
    print(f"[RECOVERY] Total transactions replayed: {replayed_count}")
    print(f"[RECOVERY] {node_name} is now ONLINE")
    print(f"{'='*60}\n")
    
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
    simulate_node_failure("Central Node")
    st.info("Central Node marked as offline")

    trans_id= 999991

    db_node = DatabaseConnection(NODE_CONFIGS["Node 2"])
    if not db_node.connect():
        st.error("Cannot connect to Node 2 for test")
        return
    result = db_node.execute_query(
        """
        INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params=(trans_id, 1, '1994-06-15', 'TestCredit', 'Test Case #1: Replication Failure', 500.00, 500.00, 'Test 001'),
        fetch=False
    )
    db_node.close()

    if isinstance(result, dict) and result.get("error"):
        st.error(f"Failed to perform write on Node 2: {result.get('error')}")
        return
    
    st.info(f"Transaction {trans_id} written to Node 2")

    #log transaction
    log_transaction_event(
        node_name="Node 2",
        trans_id=trans_id,
        op_type="INSERT",
        pk_value=str(trans_id),
        old_amount=None,
        new_amount=500.00,
        status="COMMITTED"
    )

    #Attempt replication (should fail)
    row = fetch_trans_row("Node 2", trans_id)
    if row:
        route_and_replicate_write("Node 2", trans_id, "INSERT", None, row)
        st.info(f"Replication attempted - should be logged as FAILED")
    
    # Verify Node 2 still has the data
    db_verify = DatabaseConnection(NODE_CONFIGS["Node 2"])
    if db_verify.connect():
        verify_result = db_verify.execute_query(
            "SELECT * FROM trans WHERE trans_id = %s",
            params=(trans_id,),
            fetch=True
        )
        db_verify.close()
        
        if verify_result and len(verify_result) > 0:
            st.success(f"Node 2 transaction {trans_id}")
            st.json(verify_result[0]) 
        else:
            st.error(f"Verification failed - transaction not found on Node 2")
    st.divider()
    st.markdown('### Test Case #1 Complete')
    st.info("Check 'Transaction Logs' tab ('Replication Logs') to see FAILED status")
    st.info("Central Node is offline, so replication failed but Node 2 kept the data")

def test_central_recovery_missed_writes():
    """
    Case #2: Central Node recovers and needs to catch up on missed writes
    
    Steps:
    1. Simulate Central Node failure
    2. Execute writes on Node 2/3 while Central is down
    3. Bring Central back online
    4. Replay missed transactions
    5. Verify all nodes are consistent
    """
    print(f"\n[TEST_CASE_2] ===== STARTING TEST CASE #2 =====")
    
    # 1. Fail Central
    simulate_node_failure("Central Node")
    st.info(" Central Node marked as offline")
    print(f"[TEST_CASE_2] Central Node marked as offline")

    # 2. Write to Node 2 while Central is down
    trans_id = 999992
    print(f"[TEST_CASE_2] Attempting to write trans_id {trans_id} to Node 2")
    
    db = DatabaseConnection(NODE_CONFIGS["Node 2"])
    if not db.connect():
        st.error("Cannot connect to Node 2")
        print(f"[TEST_CASE_2] ERROR: Cannot connect to Node 2")
        return
    
    result = db.execute_query(
        """
        INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params=(trans_id, 2, '1994-08-20', 'TestDebit', 'Test Case #2: Central Recovery', 750.00, 7500.00, 'TEST002'),
        fetch=False
    )
    db.close()
    
    print(f"[TEST_CASE_2] Write result: {result}")
    
    if isinstance(result, dict) and result.get("error"):
        st.error(f"Write failed on Node 2: {result.get('error')}")
        print(f"[TEST_CASE_2] ERROR: Write failed - {result.get('error')}")
        return
    
    st.info(f"Transaction {trans_id} written to Node 2 while Central is offline")
    print(f"[TEST_CASE_2] Transaction written successfully")
    
    # 3. Log and attempt replication (will fail)
    print(f"[TEST_CASE_2] Logging transaction event")
    log_transaction_event(
        node_name="Node 2",
        trans_id=trans_id,
        op_type="INSERT",
        pk_value=str(trans_id),
        old_amount=None,
        new_amount=750.00,
        status="COMMITTED"
    )
    
    row = fetch_trans_row("Node 2", trans_id)
    print(f"[TEST_CASE_2] Fetched row from Node 2: {row is not None}")
    if row:
        print(f"[TEST_CASE_2] About to call route_and_replicate_write")
        route_and_replicate_write("Node 2", trans_id, "INSERT", None, row)
        st.info("Replication failed - logged as FAILED in replication_log")
    else:
        print(f"[TEST_CASE_2] Row not found, skipping replication")
    
    # 4. Recover Central (this triggers REDO)
    st.info("Recovering Central Node...")
    simulate_node_recovery("Central Node")
    st.success("Central Node recovery completed")
    
    # 5. Verify Central now has the transaction
    db_central = DatabaseConnection(NODE_CONFIGS["Central Node"])
    if db_central.connect():
        verify_result = db_central.execute_query(
            "SELECT * FROM trans WHERE trans_id = %s",
            params=(trans_id,),
            fetch=True
        )
        db_central.close()
        
        if verify_result and len(verify_result) > 0:
            st.success(f"REDO successful! Central now has transaction {trans_id}")
            st.json(verify_result[0])
        else:
            st.error("failed - transaction not found on Central after recovery")
    
    # Summary
    st.divider()
    st.markdown("### Case #2 Test Complete")
    st.info("Check 'Failure Recovery' tab → 'Recovery Log' to see recovery details")
    st.info("Central replayed missed transactions from Node 2 using REDO recovery")

def test_replication_failure_from_central():
    """
    Case #3: Transaction fails when replicating from Central to Node 2/3
    
    Steps:
    1. Simulate Node 2 or 3 failure
    2. Execute write on Central
    3. Attempt replication (should fail)
    4. Log failure
    5. Verify Central still has the data
    """
    # 1. Fail Node 2
    simulate_node_failure("Node 2")
    st.info("Node 2 marked as offline")
    
    # 2. Execute a write on Central (that should replicate to Node 2)
    trans_id = 999993
    
    db = DatabaseConnection(NODE_CONFIGS["Central Node"])
    if not db.connect():
        st.error("Cannot connect to Central Node")
        return
    
    result = db.execute_query(
        """
        INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params=(trans_id, 3, '1995-03-10', 'TestCredit', 'Test Case #3: Replication Failure', 1000.00, 10000.00, 'TEST003'),
        fetch=False
    )
    db.close()
    
    if isinstance(result, dict) and result.get("error"):
        st.error(f"Write failed on Central: {result.get('error')}")
        return
    
    st.info(f"Transaction {trans_id} written to Central Node")
    
    # 3. Log the transaction
    log_transaction_event(
        node_name="Central Node",
        trans_id=trans_id,
        op_type="INSERT",
        pk_value=str(trans_id),
        old_amount=None,
        new_amount=1000.00,
        status="COMMITTED"
    )
    
    # 4. Attempt replication to Node 2 (will fail because Node 2 is offline)
    row = fetch_trans_row("Central Node", trans_id)
    if row:
        # Note: date 1995-03-10 is in Node 2's range (1993-1995)
        route_and_replicate_write("Central Node", trans_id, "INSERT", None, row)
        st.info("Replication to Node 2 attempted - should be logged as FAILED")
    
    # 5. Verify Central still has the data
    db_verify = DatabaseConnection(NODE_CONFIGS["Central Node"])
    if db_verify.connect():
        verify_result = db_verify.execute_query(
            "SELECT * FROM trans WHERE trans_id = %s",
            params=(trans_id,),
            fetch=True
        )
        db_verify.close()
        
        if verify_result and len(verify_result) > 0:
            st.success(f"Verified - Central has transaction {trans_id}")
            st.json(verify_result[0])
        else:
            st.error("Verification failed - transaction not found on Central")
    
    # Summary
    st.divider()
    st.markdown("### Case #3 Test Complete")
    st.info("Check 'Transaction Logs' tab → 'Replication Logs' to see FAILED status")
    st.info("Node 2 is offline, so replication failed but Central kept the data")

def test_node_recovery_missed_writes():
    """
    Case #4: Node 2/3 recovers and needs to catch up on missed writes
    
    Steps:
    1. Simulate Node 2 or 3 failure
    2. Execute writes on Central while node is down
    3. Bring node back online
    4. Replay missed transactions
    5. Verify all nodes are consistent
    """
    # 1. Fail Node 3
    simulate_node_failure("Node 3")
    st.info(" Node 3 marked as offline")
    
    # 2. Write to Central while Node 3 is down
    trans_id = 999994
    
    db = DatabaseConnection(NODE_CONFIGS["Central Node"])
    if not db.connect():
        st.error("Cannot connect to Central Node")
        return
    
    result = db.execute_query(
        """
        INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params=(trans_id, 4, '1997-11-25', 'TestDebit', 'Test Case #4: Node Recovery', 1250.00, 12500.00, 'TEST004'),
        fetch=False
    )
    db.close()
    
    if isinstance(result, dict) and result.get("error"):
        st.error(f"Write failed on Central: {result.get('error')}")
        return
    
    st.info(f"Transaction {trans_id} written to Central while Node 3 is offline")
    
    # 3. Log and attempt replication (will fail)
    log_transaction_event(
        node_name="Central Node",
        trans_id=trans_id,
        op_type="INSERT",
        pk_value=str(trans_id),
        old_amount=None,
        new_amount=1250.00,
        status="COMMITTED"
    )
    
    row = fetch_trans_row("Central Node", trans_id)
    if row:
        # Note: date 1997-11-25 is in Node 3's range (1996-1998)
        route_and_replicate_write("Central Node", trans_id, "INSERT", None, row)
        st.info("Replication to Node 3 failed - logged as FAILED")
    
    # 4. Recover Node 3 (this triggers REDO)
    st.info("Recovering Node 3...")
    simulate_node_recovery("Node 3")
    st.success("Node 3 recovery completed")
    
    # 5. Verify Node 3 now has the transaction
    db_node3 = DatabaseConnection(NODE_CONFIGS["Node 3"])
    if db_node3.connect():
        verify_result = db_node3.execute_query(
            "SELECT * FROM trans WHERE trans_id = %s",
            params=(trans_id,),
            fetch=True
        )
        db_node3.close()
        
        if verify_result and len(verify_result) > 0:
            st.success(f"REDO successful! Node 3 now has transaction {trans_id}")
            st.json(verify_result[0])
        else:
            st.error("failed - transaction not found on Node 3 after recovery")
    
    # Summary
    st.divider()
    st.markdown("### Case #4 Test Complete")
    st.info("Check 'Failure Recovery' tab → 'Recovery Log' to see recovery details")
    st.info("Node 3 replayed missed transactions from Central using REDO recovery")

# UTILITY FUNCTIONS 
def check_node_health(node_name):
    # Check simulated status first
    if not st.session_state.node_status.get(node_name, True):
        return False
    
    # Then check actual database connection
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    status = db.connect()
    db.close()
    return status

def get_table_data(node_name, trans=None, limit=100):
    db = DatabaseConnection(NODE_CONFIGS[node_name])
    if not db.connect():
        return None
    
    query = f"SELECT * FROM trans LIMIT {limit}"
    result = db.execute_query(query, params=None, fetch=True)
    db.close()
    return result if isinstance(result, list) else []

# Sidebar - Node Status
with st.sidebar:
    st.header("Node Status")
    for node_name in NODE_CONFIGS.keys():
        if check_node_health(node_name):
            st.success(f"✅ {node_name}")
        else:
            st.error(f"❌ {node_name} (Offline)")

    st.divider()
    st.header("Quick Actions")
    
    if st.button("Clear All Logs"):
        st.session_state.transaction_log = []
        st.session_state.replication_log = []
        st.session_state.recovery_log = []
        st.rerun()

# MAIN TABS ==============================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Database View", 
    "Concurrency Testing", 
    "Failure Recovery", 
    "Log Tracking",
    "Database Operations"
])

# TAB 1: DATABASE VIEW ==============================
with tab1:
    st.header("Current Database State Across All Nodes")
    
    limit = st.number_input("Row Limit", min_value=10, step=100)

    if st.button("Refresh Data"):
        st.rerun()
        
    st.subheader("Central Node")
    try:
        central_data = get_table_data("Central Node", "trans", limit)
        st.dataframe(central_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Central Node data: {e}")
    
    # Node 2
    st.subheader("Node 2")
    try:
        node2_data = get_table_data("Node 2", "trans", limit)
        st.dataframe(node2_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 2 data: {e}")
    
    # Node 3 
    st.subheader("Node 3")
    try:
        node3_data = get_table_data("Node 3", "trans", limit)
        st.dataframe(node3_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 3 data: {e}")


# TAB 2: CONCURRENCY TESTING ==============================
with tab2:
    st.header("Concurrency Control Testing")
    
    # Create subtabs for different test modes
    concurrency_mode, high_volume_mode = st.tabs(["Single Test", "High Volume Analysis"])
    
    # ===== SINGLE TEST MODE =====
    with concurrency_mode:
        st.subheader("Run Individual Concurrency Test")
        st.markdown("""
        Test concurrency control at different isolation levels:
        - **Case #1**: Multiple transactions reading the same data
        - **Case #2**: One transaction reading while another writes
        - **Case #3**: Multiple transactions writing the same data
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            isolation_level = st.selectbox(
                "Select Isolation Level",
                ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                key="isolation_level_single"
            )
        
        with col2:
            test_case = st.selectbox(
                "Select Test Case",
                [
                    "Case #1: Concurrent Reads",
                    "Case #2: Read + Write Conflict",
                    "Case #3: Write + Write Conflict"
                ],
                key="test_case_single"
            )
        
        st.divider()
        
        if st.button("Run Test", key="run_single_test"):
            st.info(f"Running {test_case} with {isolation_level}...")
            
            try:
                if test_case == "Case #1: Concurrent Reads":
                    test_concurrent_reads(isolation_level)
                elif test_case == "Case #2: Read + Write Conflict":
                    test_read_write_conflict(isolation_level)
                elif test_case == "Case #3: Write + Write Conflict":
                    test_write_write_conflict(isolation_level)
                
                st.success("Test completed!")
            except Exception as e:
                st.error(f"Test failed: {str(e)}")
                print(f"[ERROR] Test failed: {e}")
        
        st.divider()
        recent_transactions = st.session_state.get("transaction_log", [])
        if recent_transactions:
            # Filter only test transactions
            test_logs = [
                t for t in recent_transactions
                if "test-" in str(t.get("transaction_id", ""))
            ]
            
            if test_logs:
                last_test = test_logs[-1]

                st.subheader("Test Summary")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Test Case", last_test.get("test_case", "N/A").split(":")[-1].strip())
                with col2:
                    st.metric("Isolation Level", last_test.get("isolation_level", "N/A"))
                with col3:
                    st.metric("Status", last_test.get("status", "N/A").upper())
                with col4:
                    st.metric("Timestamp", last_test.get("timestamp", "N/A")[:19])

                # Display anomaly detection results
                if last_test.get("test_summary"):
                    st.markdown("#### 🔍 Anomaly Detection Results")
                    summary = last_test.get("test_summary", {})
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        dirty_read = summary.get("dirty_read", False)
                        if dirty_read:
                            st.error("Dirty Reads: DETECTED")
                        else:
                            st.success("Dirty Reads: NONE")
                    
                    with col2:
                        non_rep = summary.get("non_repeatable_read", False)
                        if non_rep:
                            st.error("Non-Repeatable Reads: DETECTED")
                        else:
                            st.success("Non-Repeatable Reads: NONE")
                    
                    with col3:
                        lost_update = summary.get("lost_update", False)
                        if lost_update:
                            st.error("Lost Updates: DETECTED")
                        else:
                            st.success("Lost Updates: NONE")
                    
                    # Show detailed summary
                    st.markdown("#### Test Details")
                    st.json(summary)
                else:
                    st.info("No anomaly summary available for this test")
            else:
                st.info("No test transactions logged yet. Run a test to see results.")
        else:
            st.info("No transactions logged yet...")
    
    # ===== HIGH VOLUME TEST MODE =====
    with high_volume_mode:
        st.subheader("High Volume Concurrency Analysis")
        st.markdown("""
        ### Comprehensive Isolation Level Comparison
        
        This mode tests **all isolation levels** across **all test cases** to determine:
        - Which isolation level supports highest transaction throughput
        - Transaction success rate per isolation level  
        - Anomalies detected per isolation level
        - Consistency guarantees per level
        - Production recommendation
        """)
        
        st.divider()
        
        # Configuration Section
        st.markdown("### Test Configuration")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            num_iterations = st.number_input(
                "Iterations per Test",
                min_value=1,
                max_value=5,
                value=1,
                help="Run each test case this many times for averaging"
            )
        
        with col2:
            selected_isolation_levels = st.multiselect(
                "Isolation Levels",
                ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                default=["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                help="Which isolation levels to compare"
            )
        
        with col3:
            selected_test_cases = st.multiselect(
                "Test Cases",
                ["Case #1: Concurrent Reads", "Case #2: Read + Write Conflict", "Case #3: Write + Write Conflict"],
                default=["Case #1: Concurrent Reads", "Case #2: Read + Write Conflict", "Case #3: Write + Write Conflict"],
                help="Which test cases to include"
            )
        
        st.divider()
        
        if st.button("Start High Volume Analysis", key="run_high_volume"):
            if not selected_isolation_levels or not selected_test_cases:
                st.error("Please select at least one isolation level and test case")
            else:
                st.info(f"Starting high volume analysis...")
                st.info(f"Configuration: {len(selected_isolation_levels)} levels × {len(selected_test_cases)} cases × {num_iterations} iterations")
                
                # Initialize results container
                high_volume_results = {
                    "isolation_level": [],
                    "test_case": [],
                    "iteration": [],
                    "success": [],
                    "failed": [],
                    "duration": [],
                    "anomalies": []
                }
                
                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_tests = len(selected_isolation_levels) * len(selected_test_cases) * num_iterations
                current_test = 0
                
                # Run all tests
                for isolation in selected_isolation_levels:
                    for test_case in selected_test_cases:
                        for iteration in range(num_iterations):
                            current_test += 1
                            progress = current_test / total_tests
                            progress_bar.progress(progress)
                            status_text.text(f"Progress: {current_test}/{total_tests} | {isolation} | {test_case} | Iteration {iteration + 1}/{num_iterations}")
                            
                            # Record start time
                            start_time = time.time()
                            start_log_count = len(st.session_state.transaction_log)
                            
                            # Run the test
                            try:
                                if test_case == "Case #1: Concurrent Reads":
                                    test_concurrent_reads(isolation)
                                elif test_case == "Case #2: Read + Write Conflict":
                                    test_read_write_conflict(isolation)
                                elif test_case == "Case #3: Write + Write Conflict":
                                    test_write_write_conflict(isolation)
                            except Exception as e:
                                st.warning(f"Test error: {str(e)}")
                                print(f"[ERROR] Test failed: {e}")
                            
                            # Record end time
                            end_time = time.time()
                            duration = end_time - start_time
                            
                            # Count transactions in this test
                            end_log_count = len(st.session_state.transaction_log)
                            transactions_in_test = end_log_count - start_log_count
                            
                            # Count successes/failures
                            recent_logs = st.session_state.transaction_log[start_log_count:end_log_count]
                            success_count = sum(1 for log in recent_logs if log.get("status") in ["success", "completed", "consistent"])
                            failed_count = sum(1 for log in recent_logs if log.get("status") in ["error", "failed", "inconsistent"])
                            
                            # Detect anomalies
                            anomaly_count = 0
                            for log in recent_logs:
                                if log.get("test_summary"):
                                    summary = log.get("test_summary", {})
                                    if (summary.get("dirty_read") or 
                                        summary.get("non_repeatable_read") or 
                                        summary.get("lost_update")):
                                        anomaly_count += 1
                            
                            # Store results
                            high_volume_results["isolation_level"].append(isolation)
                            high_volume_results["test_case"].append(test_case)
                            high_volume_results["iteration"].append(iteration + 1)
                            high_volume_results["success"].append(success_count)
                            high_volume_results["failed"].append(failed_count)
                            high_volume_results["duration"].append(f"{duration:.2f}s")
                            high_volume_results["anomalies"].append(anomaly_count)
                
                progress_bar.progress(1.0)
                status_text.text("✅ Analysis complete!")
                
                # ===== DISPLAY RESULTS =====
                st.success("✅ High Volume Analysis Complete!")
                st.divider()
                
                # Create results dataframe
                results_df = pd.DataFrame(high_volume_results)
                
                # ===== SECTION 1: DETAILED RESULTS =====
                st.subheader("Detailed Test Results")
                st.dataframe(results_df, use_container_width=True, height=400)
                
                st.divider()
                
                # ===== SECTION 2: ISOLATION LEVEL COMPARISON =====
                st.subheader("Isolation Level Comparison")
                
                comparison_data = []
                for isolation in selected_isolation_levels:
                    isolation_logs = results_df[results_df["isolation_level"] == isolation]
                    
                    total_success = isolation_logs["success"].sum()
                    total_failed = isolation_logs["failed"].sum()
                    total_anomalies = isolation_logs["anomalies"].sum()
                    total_tests_count = len(isolation_logs)
                    
                    success_rate = (total_success / (total_success + total_failed) * 100) if (total_success + total_failed) > 0 else 0
                    
                    try:
                        avg_duration = isolation_logs["duration"].str.rstrip("s").astype(float).mean()
                    except:
                        avg_duration = 0
                    
                    comparison_data.append({
                        "Isolation Level": isolation,
                        "Tests Run": total_tests_count,
                        "Successful": total_success,
                        "Failed": total_failed,
                        "Success Rate": f"{success_rate:.1f}%",
                        "Anomalies": int(total_anomalies),
                        "Avg Duration (s)": f"{avg_duration:.2f}"
                    })
                
                comparison_df = pd.DataFrame(comparison_data)
                st.dataframe(comparison_df, use_container_width=True, height=300)
                
                st.divider()
                
                # ===== SECTION 3: ANOMALY DETECTION MATRIX =====
                st.subheader("Anomaly Detection Matrix")
                st.markdown("Shows which anomalies occur at each isolation level:")
                
                anomaly_matrix = []
                for isolation in selected_isolation_levels:
                    isolation_logs = results_df[results_df["isolation_level"] == isolation]
                    total_anomalies = isolation_logs["anomalies"].sum()
                    
                    anomaly_matrix.append({
                        "Isolation Level": isolation,
                        "Total Anomalies": int(total_anomalies),
                        "Consistency": "Guaranteed" if total_anomalies == 0 else f" {int(total_anomalies)} anomalies detected"
                    })
                
                anomaly_df = pd.DataFrame(anomaly_matrix)
                st.dataframe(anomaly_df, use_container_width=True, height=200)
                
                st.divider()
                
                # ===== SECTION 4: TEST CASE PERFORMANCE =====
                st.subheader("Performance by Test Case")
                
                test_case_perf = []
                for test_case in selected_test_cases:
                    test_logs = results_df[results_df["test_case"] == test_case]
                    
                    total_success = test_logs["success"].sum()
                    total_failed = test_logs["failed"].sum()
                    success_rate = (total_success / (total_success + total_failed) * 100) if (total_success + total_failed) > 0 else 0
                    total_tests_count = len(test_logs)
                    
                    test_case_perf.append({
                        "Test Case": test_case,
                        "Total Runs": total_tests_count,
                        "Success Rate": f"{success_rate:.1f}%",
                        "Total Successes": int(total_success)
                    })
                
                test_case_df = pd.DataFrame(test_case_perf)
                st.dataframe(test_case_df, use_container_width=True, height=200)
                
                st.divider()
                
                # ===== SECTION 5: RECOMMENDATION =====
                st.subheader("Analysis & Recommendation")
                
                # Find best isolation levels
                try:
                    best_consistency_idx = comparison_df["Anomalies"].astype(int).idxmin()
                    best_consistency = comparison_df.loc[best_consistency_idx]
                except:
                    best_consistency = comparison_df.iloc[0]
                
                try:
                    best_success_idx = comparison_df["Success Rate"].str.rstrip("%").astype(float).idxmax()
                    best_success = comparison_df.loc[best_success_idx]
                except:
                    best_success = comparison_df.iloc[0]
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.info(f"""
                    **Best Consistency:**
                    
                    {best_consistency['Isolation Level']}
                    
                    Anomalies: {best_consistency['Anomalies']}
                    
                    Success Rate: {best_consistency['Success Rate']}
                    """)
                
                with col2:
                    st.info(f"""
                    **Highest Success Rate:**
                    
                    {best_success['Isolation Level']}
                    
                    Success Rate: {best_success['Success Rate']}
                    
                    Anomalies: {best_success['Anomalies']}
                    """)
                
                with col3:
                    st.info(f"""
                    **Test Summary:**
                    
                    Total Tests: {len(results_df)}
                    
                    Isolation Levels: {len(selected_isolation_levels)}
                    
                    Test Cases: {len(selected_test_cases)}
                    """)
                
                st.divider()
                
                with col2:
                    summary_report = f"""
                        {'='*80}
                        CONCURRENCY CONTROL ANALYSIS REPORT
                        {'='*80}
                        Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

                        TEST CONFIGURATION:
                        {'-'*80}
                        Isolation Levels Tested: {', '.join(selected_isolation_levels)}
                        Test Cases Executed: {', '.join(selected_test_cases)}
                        Iterations Per Test: {num_iterations}
                        Total Tests Run: {len(results_df)}

                        COMPARISON SUMMARY:
                        {'-'*80}
                        {comparison_df.to_string(index=False)}

                        ANOMALY SUMMARY:
                        {'-'*80}
                        {anomaly_df.to_string(index=False)}

                        TEST CASE PERFORMANCE:
                        {'-'*80}
                        {test_case_df.to_string(index=False)}

                        RECOMMENDATION:
                        {'-'*80}
                        For Best Consistency: Use {best_consistency['Isolation Level']}
                        - Anomalies: {best_consistency['Anomalies']}
                        - Success Rate: {best_consistency['Success Rate']}

                        For Best Performance: Use {best_success['Isolation Level']}
                        - Success Rate: {best_success['Success Rate']}
                        - Anomalies: {best_success['Anomalies']}

                        For production systems, balance consistency with performance based on your requirements.
                        Use SERIALIZABLE for critical financial data, READ COMMITTED for general use.

                        {'='*80}
                        End of Report
                        {'='*80}
                    """

# TAB 3: FAILURE RECOVERY TESTING ==============================
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
        
        if recovery_test_case == "Case #1: Node 2/3 → Central Replication Failure":
            test_replication_failure_to_central()
        elif recovery_test_case == "Case #2: Central Node Recovery (Missed Writes)":
            test_central_recovery_missed_writes()
        elif recovery_test_case == "Case #3: Central → Node 2/3 Replication Failure":
            test_replication_failure_from_central()
        elif recovery_test_case == "Case #4: Node 2/3 Recovery (Missed Writes)":
            test_node_recovery_missed_writes()
        
        st.success("Recovery test completed! Check logs.")
        st.rerun()
    
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


# TAB 4: TRANSACTION LOGS ==============================
with tab4:
    st.header("Transaction, Replication, & Recovery Logs")
    
    # FETCH DATA FROM DATABASE - Define the function inside the tab
    def fetch_logs_from_db():
        """Fetch all logs from the database"""
        try:
            # Use Central Node to fetch logs (or whichever node stores your logs)
            db = DatabaseConnection(NODE_CONFIGS["Central Node"])
            if not db.connect():
                st.error("Failed to connect to database for logs")
                return [], [], []
            
            # Transaction logs
            transaction_logs = db.execute_query("""
                SELECT log_id, trans_id, node, table_name, op_type, 
                       pk_value, old_amount, new_amount, status, 
                       error_message, started_at, ended_at
                FROM transaction_log
                ORDER BY started_at DESC
                LIMIT 1000
            """, fetch=True)
            
            # Replication logs
            replication_logs = db.execute_query("""
                SELECT log_id, source_node, target_node, trans_id, 
                       old_amount, new_amount, op_type, status, 
                       error_message, created_at, completed_at
                FROM replication_log
                ORDER BY created_at DESC
                LIMIT 1000
            """, fetch=True)
            
            # Recovery logs
            recovery_logs = db.execute_query("""
                SELECT log_id, node, downtime_start, downtime_end, 
                       replayed_count, status, details, created_at, updated_at
                FROM recovery_log
                ORDER BY created_at DESC
                LIMIT 1000
            """, fetch=True)
            
            db.close()
            
            # Handle cases where queries return error dicts instead of lists
            if not isinstance(transaction_logs, list):
                transaction_logs = []
            if not isinstance(replication_logs, list):
                replication_logs = []
            if not isinstance(recovery_logs, list):
                recovery_logs = []
            
            return transaction_logs, replication_logs, recovery_logs
            
        except Exception as e:
            st.error(f"Error fetching logs: {str(e)}")
            return [], [], []
    
    # Fetch logs and store in session state
    if st.button("Refresh Logs", key="refresh_logs"):
        transaction_logs, replication_logs, recovery_logs = fetch_logs_from_db()
        st.session_state["transaction_log_db"] = transaction_logs
        st.session_state["replication_log_db"] = replication_logs
        st.session_state["recovery_log_db"] = recovery_logs
        st.success("Logs refreshed!")
    
    # Initialize session state if not exists - wrapped in try/except
    if ("transaction_log_db" not in st.session_state or 
        "replication_log_db" not in st.session_state or 
        "recovery_log_db" not in st.session_state):
        try:
            transaction_logs, replication_logs, recovery_logs = fetch_logs_from_db()
            st.session_state["transaction_log_db"] = transaction_logs
            st.session_state["replication_log_db"] = replication_logs
            st.session_state["recovery_log_db"] = recovery_logs
        except Exception as e:
            st.error(f"Failed to initialize logs: {str(e)}")
            st.session_state["transaction_log_db"] = []
            st.session_state["replication_log_db"] = []
            st.session_state["recovery_log_db"] = []
    
    log_type = st.selectbox(
        "Select Log Type",
        ["Transaction Logs", "Replication Logs", "Recovery Logs", "All Logs"],
        key = "log_type"
    )
    
    # filtering options
    nodes = ["Central", "Node2", "Node3"]  # Match your log format
    selected_node = st.selectbox("Select Node", ["All Nodes"] + nodes, key="selected_node")
    
    start_time = st.date_input("Start Date", min_value=datetime(1993, 1, 1).date(), 
                               max_value=datetime(2025, 12, 31).date(), value=None, key="start_time")
    end_time = st.date_input("End Date", min_value=datetime(1993, 1, 1).date(), 
                             max_value=datetime(2025, 12, 31).date(), value=None, key="end_time")

    status_options = ["All", "PENDING", "COMMITTED", "ROLLED_BACK", "FAILED", "SUCCESS", "PARTIAL", "IN_PROGRESS"]
    selected_status = st.selectbox("Status", status_options, key="selected_status")
    
    transaction_id_filter = st.text_input("Transaction ID (optional)")
    
    def filter_logs(df, timestamp_col="started_at"):
        if df is None or df.empty:
            return pd.DataFrame()
        
        filtered = df.copy()
        
        # Filter by node
        if selected_node != "All Nodes" and "node" in filtered.columns:
            filtered = filtered[filtered["node"] == selected_node]
        
        # Filter by status
        if selected_status != "All" and "status" in filtered.columns:
            filtered = filtered[filtered["status"].str.upper() == selected_status.upper()]
        
        # Filter by transaction ID
        if transaction_id_filter and "trans_id" in filtered.columns:
            filtered = filtered[filtered["trans_id"].astype(str).str.contains(transaction_id_filter, na=False)]
        
        # Filter by date range
        if timestamp_col in filtered.columns:
            if start_time:
                filtered = filtered[pd.to_datetime(filtered[timestamp_col], errors='coerce').dt.date >= start_time]
            if end_time:
                filtered = filtered[pd.to_datetime(filtered[timestamp_col], errors='coerce').dt.date <= end_time]
        
        return filtered
    
    # Display logs
    def display_log(name, log_data, timestamp_col="started_at"):
        st.subheader(name)
        if log_data:
            df = pd.DataFrame(log_data)
            df = filter_logs(df, timestamp_col)
            if not df.empty:
                st.dataframe(df, use_container_width=True, height=400)
                st.caption(f"Showing {len(df)} records")
            else:
                st.info(f"No {name.lower()} match the selected filters")
        else:
            st.info(f"No {name.lower()} found in database")
    
    if log_type in ["Transaction Logs", "All Logs"]:
        display_log("Transaction Logs", st.session_state.get("transaction_log_db", []), "started_at")
    
    if log_type in ["Replication Logs", "All Logs"]:
        display_log("Replication Logs", st.session_state.get("replication_log_db", []), "created_at")
    
    if log_type in ["Recovery Logs", "All Logs"]:
        display_log("Recovery Logs", st.session_state.get("recovery_log_db", []), "created_at")
    
# TAB 5: MANUAL OPERATIONS ==============================
with tab5:
    st.header("Manual Database Operations")
    
    # Node selection
    selected_node = st.selectbox("Select Node", list(NODE_CONFIGS.keys()), key="manual_node")
    
    # Operation type tabs
    crud_tab1, crud_tab2, crud_tab3, crud_tab4, crud_tab5, crud_tab6 = st.tabs([
        "Create", 
        "Read", 
        "Update", 
        "Delete",
        "Raw SQL",
        "Reports"
    ])
    
    # CREATE / INSERT
    with crud_tab1:
        st.subheader("Insert New Record")
        
        with st.form("insert_form"):
            col1, col2 = st.columns([3, 1])

            if st.form_submit_button("Generate ID"):
                    db = DatabaseConnection(NODE_CONFIGS[selected_node])
                    if db.connect():
                        result = db.execute_query("SELECT MAX(trans_id) AS max_id FROM trans", fetch=True)
                        if result and isinstance(result, list) and len(result) > 0:
                            max_id = result[0].get("max_id", 0) or 0
                            st.session_state.insert_trans_id = max_id + 1
                        db.close()

            # Set date range based on selected node
            if selected_node == "Node 2":
                min_date = datetime(1993, 1, 1).date()
                max_date = datetime(1995, 12, 31).date()
                date_info = "Node 2 accepts dates: 1993-1995"
            elif selected_node == "Node 3":
                min_date = datetime(1996, 1, 1).date()
                max_date = datetime(1998, 12, 31).date()
                date_info = "Node 3 accepts dates: 1996-1998"
            else:  # Central Node
                min_date = datetime(1993, 1, 1).date()
                max_date = datetime(1998, 12, 31).date()
                date_info = "Central Node accepts all dates: 1993-1998"

            newdate = st.date_input(
                "Transaction Date",
                value=min_date,
                min_value=min_date,
                max_value=max_date
            )

            st.info(date_info)
            
            with col1:
                trans_id = st.number_input(
                    "Transaction ID", 
                    min_value=1, 
                    step=1, 
                    key="insert_trans_id"
                )
            
            with col2:
                st.write("")  # spacing
                st.write("")  # spacing

            if 'generated_trans_id' in st.session_state and st.session_state.generated_trans_id:
                trans_id = st.session_state.generated_trans_id
            
            account_id = st.number_input("Account ID", min_value=1, step=1)

            type_options = ["Credit", "Debit (Withdrawal)", "VYBER", "Custom (type below)"]
            type_choice = st.selectbox("Type", type_options)
            
            if type_choice == "Custom (type below)":
                trans_type = st.text_input("Enter Custom Type")
            else:
                trans_type = type_choice
                type_override = st.text_input("Or override with custom type:", key="type_override", value="", placeholder="Leave blank to use selection above")
                if type_override.strip():
                    trans_type = type_override
            
            operation_options = [
                "Collection from Another Bank",
                "Credit Card Withdrawal", 
                "Credit in Cash",
                "Remittance to Another Bank",
                "Withdrawal in Cash",
                "Custom (type below)"
            ]
            operation_choice = st.selectbox("Operation", operation_options)
            
            if operation_choice == "Custom (type below)":
                operation = st.text_input("Enter Custom Operation")
            else:
                operation = operation_choice
                op_override = st.text_input("Or override with custom operation:", key="op_override", value="", placeholder="Leave blank to use selection above")
                if op_override.strip():
                    operation = op_override
            
            amount = st.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
            balance = st.number_input("Balance", min_value=0.0, step=0.01, format="%.2f")
            account = st.text_input("Account Number")
            
            submitted = st.form_submit_button("Insert Record")
            
            if submitted:
                # Validation
                if not trans_type or trans_type == "Custom (type below)":
                    st.error("Please enter a Type")
                elif not operation or operation == "Custom (type below)":
                    st.error("Please enter an Operation")
                else:
                    query = """
                    INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, balance, account) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    params = (trans_id, account_id, newdate, trans_type, operation, amount, balance, account)
                    
                    db = DatabaseConnection(NODE_CONFIGS[selected_node])
                    if db.connect():
                        result = db.execute_query(query, params=params, fetch=False)

                        if isinstance(result, dict) and not result.get("error"):
                            st.success(f"Record inserted successfully on {selected_node}")
                            st.json(result)
                            if 'generated_trans_id' in st.session_state:
                                del st.session_state.generated_trans_id

                            # log local transaction
                            log_transaction_event(
                                node_name=selected_node,
                                trans_id=trans_id,
                                op_type="INSERT",
                                pk_value=str(trans_id),
                                old_amount=None,
                                new_amount=amount,
                                status="COMMITTED"
                            )

                            # replicate
                            if selected_node == "Central Node":
                                targets = choose_target_nodes_by_year(newdate)
                                for t in targets:
                                    ok, err = replicate_from_central(
                                        target_node=t,
                                        query=query,
                                        params=params,
                                        trans_id=trans_id,
                                        op_type="INSERT",
                                        pk_value=str(trans_id),
                                        old_amount=None,
                                        new_amount=amount
                                    )
                                    if not ok:
                                        st.warning(f"Replication to {t} failed: {err}")
                            else:
                                ok, err = replicate_to_central(
                                    source_node=selected_node,
                                    query=query,
                                    params=params,
                                    trans_id=trans_id,
                                    op_type="INSERT",
                                    pk_value=str(trans_id),
                                    old_amount=None,
                                    new_amount=amount
                                )
                                if not ok:
                                    st.warning(f"Replication to Central failed: {err}")

                        elif isinstance(result, dict) and result.get("error"):
                            st.error(f"Insert failed: {result.get('error', 'Unknown error')}")
                        else:
                            st.error(f"Insert failed: Unexpected result type")

                        db.close()
                    else:
                        st.error(f"Failed to connect to {selected_node}")
    
    # READ / SELECT
    with crud_tab2:
        st.subheader("Search and View Records")
        
        col1, col2 = st.columns(2)
        
        with col1:
            search_trans_id = st.number_input("Transaction ID (0 = all)", min_value=0, step=1, key="search_id")
        
        with col2:
            limit = st.number_input("Limit Results", min_value=1, max_value=10000, value=100, step=10)
        
        if st.button("Search Records"):
            if search_trans_id > 0:
                query = "SELECT * FROM trans WHERE trans_id = %s"
                params = (search_trans_id,)
            else:
                query = f"SELECT * FROM trans LIMIT {limit}"
                params = None
            
            db = DatabaseConnection(NODE_CONFIGS[selected_node])
            if db.connect():
                result = db.execute_query(query, params=params, fetch=True)
                
                if isinstance(result, list):
                    if len(result) > 0:
                        st.success(f"Found {len(result)} record(s)")
                        st.dataframe(pd.DataFrame(result), use_container_width=True, height=400)
                    else:
                        st.warning("No records found matching trans_id...")
                        if search_trans_id > 0:
                            check_query = "SELECT trans_id FROM trans ORDER BY trans_id LIMIT 10"
                            sample = db.execute_query(check_query, fetch=True)
                elif isinstance(result, dict) and result.get("error"):
                    st.error(f"Query failed: {result.get('error', 'Unknown error')}")
                else:
                    st.error(f"Query execution failed - Unexpected result: {result}")
                
                db.close()
            else:
                st.error(f"Failed to connect to {selected_node}")
    
    # UPDATE
    with crud_tab3:
        st.subheader("Update Existing Record")
        
        with st.form("update_form"):
            upd_trans_id = st.number_input("Transaction ID to Update", min_value=1, step=1, key="upd_id")
            
            st.markdown("**Update Fields** (leave blank/zero to keep current value)")
            upd_account_id = st.number_input("New Account ID", min_value=0, step=1, key="upd_acc_id")
            
            # Set date range based on selected node
            if selected_node == "Node 2":
                min_date = datetime(1993, 1, 1).date()
                max_date = datetime(1995, 12, 31).date()
                date_info = "Node 2 accepts dates: 1993-1995"
            elif selected_node == "Node 3":
                min_date = datetime(1996, 1, 1).date()
                max_date = datetime(1998, 12, 31).date()
                date_info = "Node 3 accepts dates: 1996-1998"
            else:  # Central Node
                min_date = datetime(1993, 1, 1).date()
                max_date = datetime(1998, 12, 31).date()
                date_info = "Central Node accepts all dates: 1993-1998"

            upd_newdate = st.date_input(
                "New Transaction Date",
                value=min_date,  # FIX: Add value parameter
                min_value=min_date,
                max_value=max_date
            )

            st.info(date_info)

            upd_type = st.text_input("New Type", key="upd_type")
            upd_operation = st.text_input("New Operation", key="upd_op")
            upd_amount = st.number_input("New Amount", min_value=0.0, step=0.01, format="%.2f", key="upd_amt")
            upd_balance = st.number_input("New Balance", min_value=0.0, step=0.01, format="%.2f", key="upd_bal")
            upd_account = st.text_input("New Account Number", key="upd_acc")
            
            submitted_upd = st.form_submit_button("Update Record")
            
            if submitted_upd:
                updates = []
                params = []
                
                if upd_account_id > 0:
                    updates.append("account_id = %s")
                    params.append(upd_account_id)
                
                if upd_newdate is not None:
                    updates.append("newdate = %s")
                    params.append(upd_newdate)
                
                if upd_type.strip():
                    updates.append("type = %s")
                    params.append(upd_type)
                
                if upd_operation.strip():
                    updates.append("operation = %s")
                    params.append(upd_operation)
                
                if upd_amount > 0:
                    updates.append("amount = %s")
                    params.append(upd_amount)
                
                if upd_balance > 0:
                    updates.append("balance = %s")
                    params.append(upd_balance)
                
                if upd_account.strip():
                    updates.append("account = %s")
                    params.append(upd_account)
                
                if not updates:
                    st.warning("Please specify at least one field to update")
                else:
                    params.append(upd_trans_id)
                    query = f"UPDATE trans SET {', '.join(updates)} WHERE trans_id = %s"
                    existing_row = fetch_trans_row(selected_node, upd_trans_id)
                    
                    db = DatabaseConnection(NODE_CONFIGS[selected_node])
                    if db.connect():
                        result = db.execute_query(query, params=tuple(params), fetch=False)
                        
                        if isinstance(result, dict) and not result.get("error"):
                            affected = result.get("affected_rows", 0)
                            if affected > 0:
                                st.success(f"Updated {affected} record(s) on {selected_node}")
                            else:
                                st.warning("No records were updated (transaction ID may not exist)")
                            st.json(result)

                            old_amt = existing_row.get("amount") if existing_row else None
                            new_amt = upd_amount if upd_amount > 0 else old_amt
                            target_date = upd_newdate if upd_newdate else (existing_row.get("newdate") if existing_row else None)

                            log_transaction_event(
                                node_name=selected_node,
                                trans_id=upd_trans_id,
                                op_type="UPDATE",
                                pk_value=str(upd_trans_id),
                                old_amount=old_amt,
                                new_amount=new_amt,
                                status="COMMITTED"
                            )

                            # replicate only if a row was actually updated
                            if affected > 0:
                                if selected_node == "Central Node":
                                    targets = choose_target_nodes_by_year(target_date)
                                    for t in targets:
                                        ok, err = replicate_from_central(
                                            target_node=t,
                                            query=query,
                                            params=tuple(params),
                                            trans_id=upd_trans_id,
                                            op_type="UPDATE",
                                            pk_value=str(upd_trans_id),
                                            old_amount=old_amt,
                                            new_amount=new_amt
                                        )
                                        if not ok:
                                            st.warning(f"Replication to {t} failed: {err}")
                                else:
                                    ok, err = replicate_to_central(
                                        source_node=selected_node,
                                        query=query,
                                        params=tuple(params),
                                        trans_id=upd_trans_id,
                                        op_type="UPDATE",
                                        pk_value=str(upd_trans_id),
                                        old_amount=old_amt,
                                        new_amount=new_amt
                                    )
                                    if not ok:
                                        st.warning(f"Replication to Central failed: {err}")
                        else:
                            st.error(f"Update failed: {result.get('error', 'Unknown error')}")
                        
                        db.close()
    
    # DELETE
    with crud_tab4:
        st.subheader("Delete Record")
        
        st.warning("This operation cannot be undone!")
        
        with st.form("delete_form"):
            del_trans_id = st.number_input("Transaction ID to Delete", min_value=1, step=1, key="del_id")
            confirm = st.checkbox("I confirm I want to delete this record")
            
            submitted_del = st.form_submit_button("Delete Record")
            
            if submitted_del:
                if not confirm:
                    st.error("Please confirm deletion by checking the box")
                else:
                    existing_row = fetch_trans_row(selected_node, del_trans_id)
                    query = "DELETE FROM trans WHERE trans_id = %s"
                    params = (del_trans_id,)
                    
                    db = DatabaseConnection(NODE_CONFIGS[selected_node])
                    if db.connect():
                        result = db.execute_query(query, params=params, fetch=False)
                        
                        if isinstance(result, dict) and not result.get("error"):
                            affected = result.get("affected_rows", 0)
                            if affected > 0:
                                st.success(f"Deleted {affected} record(s) from {selected_node}")
                            else:
                                st.warning("No records were deleted (transaction ID may not exist)")
                            st.json(result)

                            old_amt = existing_row.get("amount") if existing_row else None
                            target_date = existing_row.get("newdate") if existing_row else None

                            # log + replicate only when a row was deleted
                            if affected > 0:
                                log_transaction_event(
                                    node_name=selected_node,
                                    trans_id=del_trans_id,
                                    op_type="DELETE",
                                    pk_value=str(del_trans_id),
                                    old_amount=old_amt,
                                    new_amount=None,
                                    status="COMMITTED"
                                )

                                if selected_node == "Central Node":
                                    targets = choose_target_nodes_by_year(target_date)
                                    for t in targets:
                                        ok, err = replicate_from_central(
                                            target_node=t,
                                            query=query,
                                            params=params,
                                            trans_id=del_trans_id,
                                            op_type="DELETE",
                                            pk_value=str(del_trans_id),
                                            old_amount=old_amt,
                                            new_amount=None
                                        )
                                        if not ok:
                                            st.warning(f"Replication to {t} failed: {err}")
                                else:
                                    ok, err = replicate_to_central(
                                        source_node=selected_node,
                                        query=query,
                                        params=params,
                                        trans_id=del_trans_id,
                                        op_type="DELETE",
                                        pk_value=str(del_trans_id),
                                        old_amount=old_amt,
                                        new_amount=None
                                    )
                                    if not ok:
                                        st.warning(f"Replication to Central failed: {err}")
                        else:
                            st.error(f"Delete failed: {result.get('error', 'Unknown error')}")
                        
                        db.close()
    
    # RAW SQL
    with crud_tab5:
        st.subheader("Execute SQL Query")
        
        query = st.text_area("SQL Query", height=150, placeholder="SELECT * FROM trans WHERE balance > 1000")
        operation_type = st.radio("Operation Type", ["Read (SELECT)", "Write (INSERT/UPDATE/DELETE)"], key="raw_op")
        
        if st.button("Execute Query"):
            if query.strip():
                db = DatabaseConnection(NODE_CONFIGS[selected_node])
                if db.connect():
                    is_read = operation_type == "Read (SELECT)"
                    result = db.execute_query(query, fetch=is_read)
                    
                    if result:
                        if isinstance(result, list) and len(result) > 0:
                            st.success(f"Query executed successfully. Returned {len(result)} row(s)")
                            st.dataframe(pd.DataFrame(result), use_container_width=True)
                        elif isinstance(result, list):
                            st.info("Query executed. No rows returned.")
                        elif result.get("error"):
                            st.error(f"Query failed: {result['error']}")
                        else:
                            st.success("Query executed successfully")
                            st.json(result)
                    else:
                        st.error("Query execution failed")
                    
                    db.close()
            else:
                st.warning("Please enter a query")

    # REPORTS
    with crud_tab6:
        st.header("Financial Reports")
        
        report_tabs = st.tabs(["Account Summary", "Transaction Analysis", "High-Value Transactions"])
        
        # report 1: account Summary
        with report_tabs[0]:
            st.subheader("Account Summary Report")
            st.markdown("Overview of account balances and transaction counts")
            
            report_node = st.selectbox("Select Node for Report", list(NODE_CONFIGS.keys()), key="report1_node")
            
            if st.button("Generate Account Summary", key="btn_report1"):
                db = DatabaseConnection(NODE_CONFIGS[report_node])
                if db.connect():
                    query = """
                    SELECT 
                        account_id,
                        COUNT(*) as total_transactions,
                        MIN(balance) as min_balance,
                        MAX(balance) as max_balance,
                        AVG(balance) as avg_balance,
                        SUM(amount) as total_amount
                    FROM trans
                    GROUP BY account_id
                    ORDER BY total_transactions DESC
                    LIMIT 20
                    """
                    result = db.execute_query(query, fetch=True)

                    st.dataframe(pd.DataFrame(result), use_container_width=True)
                    
                    db.close()
        
        # report 2: transaction analysis
        with report_tabs[1]:
            st.subheader("Transaction Type Analysis Report")
            st.markdown("Breakdown of transactions by type and operation")
            
            report_node2 = st.selectbox("Select Node for Report", list(NODE_CONFIGS.keys()), key="report2_node")
            
            if st.button("Generate Transaction Analysis", key="btn_report2"):
                db = DatabaseConnection(NODE_CONFIGS[report_node2])
                if db.connect():
                    query = """
                    SELECT 
                        type,
                        operation,
                        COUNT(*) as transaction_count,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount,
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount
                    FROM trans
                    GROUP BY type, operation
                    ORDER BY transaction_count DESC
                    LIMIT 15
                    """
                    result = db.execute_query(query, fetch=True)

                    st.dataframe(pd.DataFrame(result), use_container_width=True)
                    
                    db.close()
        
        # report 3: high-value transactions
        with report_tabs[2]:
            st.subheader("High-Value Transactions Report")
            st.markdown("Identify and track high-value transactions")
            
            report_node3 = st.selectbox("Select Node for Report", list(NODE_CONFIGS.keys()), key="report3_node")
            threshold = st.number_input("Amount Threshold", min_value=0.0, value=10000.0, step=1000.0, format="%.2f")
            
            if st.button("Generate High-Value Report", key="btn_report3"):
                db = DatabaseConnection(NODE_CONFIGS[report_node3])
                if db.connect():
                    query = """
                    SELECT 
                        trans_id,
                        account_id,
                        newdate,
                        type,
                        operation,
                        amount,
                        balance,
                        account
                    FROM trans
                    WHERE amount >= %s
                    ORDER BY amount DESC
                    LIMIT 50
                    """
                    result = db.execute_query(query, params=(threshold,), fetch=True)
                    
                    if result and isinstance(result, list) and len(result) > 0:
                        total_value = sum(row['amount'] for row in result)
                        st.success(f"Combined Value: ${total_value:,.2f}")
                        
                        st.dataframe(pd.DataFrame(result), use_container_width=True)
                        
                    else:
                        st.info(f"No transactions found above ${threshold:,.2f}")
                    
                    db.close()
    
    # Operation Log
    st.divider()
    st.subheader("Transaction Operation Log")
    
    if st.session_state.transaction_operations:
        for i, op in enumerate(st.session_state.transaction_operations):
            with st.expander(f"{i+1}. {op['time']} - {op['operation']}", expanded=(i == len(st.session_state.transaction_operations)-1)):
                st.json(op)
    else:
        st.info("No operations yet in this transaction")

st.divider()
st.caption("MCO2 - Group 8 | STADVDB - S17")
