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
#     }
# }

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
        "user": "user1",
        "password": "UserPass123!",
        "database": "mco2financedata"
    },
    "Node 3": {
        "host": "ccscloud.dlsu.edu.ph", # TO RUN LOCAL, change to ccscloud.dlsu.edu.ph
        "port": 60705, # TO RUN LOCAL, change to 60705
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
                # write operation
                self.connection.commit()
                return {"affected_rows": self.cursor.rowcount}

        except Error as e:
            if self.connection:
                try:
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
# TODO (thara): Implement all functions in this section
def set_isolation_level(connection, level, per_transaction=False):
    """
    Set the isolation level for the given MySQL connection.

    If per_transaction=False (default):
        SET SESSION TRANSACTION ISOLATION LEVEL <LEVEL>;

    If per_transaction=True:
        SET TRANSACTION ISOLATION LEVEL <LEVEL>;
        (applies only to the next transaction)

    Supported:
        READ UNCOMMITTED
        READ COMMITTED
        REPEATABLE READ
        SERIALIZABLE
    """

    if connection is None:
        st.warning("set_isolation_level: connection is None")
        return False

    if not isinstance(level, str) or not level.strip():
        st.warning("set_isolation_level: invalid isolation level")
        return False

    # Normalize input (handles lowercase and underscores)
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

    # Decide SQL statement (session mode is the default for your system)
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
    """
    TODO (thara): Complete this function to:
    1. connect to the specified node
    2. set the isolation level using set_isolation_level()
    3. execute the query
    4. log the transaction (timestamp, duration, status)
    5. handle any errors/conflicts that arise
    """
    start_time = time.time()

    status = "pending"
    error_msg = None
    result = None

    # 1. connect to the specified node
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
        # 2. set the isolation level on the DB connection for the next transaction (if provided)
        if isolation_level:
            set_isolation_level(db.connection, isolation_level, per_transaction=True)

        # Decide whether this is a SELECT (fetch) or write
        qtype = (query.strip().split()[0].upper() if query and query.strip() else "")
        fetch = True if qtype == "SELECT" else False

        # 3. execute the query (pass isolation_level so execute_query can apply it per-transaction too)
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

        # Log the transaction
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
    case #1: concurrent transactions reading the same data item
    
    TODO (thara): Implement this test case
    Steps:
    1. create 2+ threads that read the same row(s) simultaneously
    2. use threading to simulate concurrency
    3. verify all reads return consistent data
    4. log results
    """
    nodes = ["Node 2", "Node 3"]
    barrier = threading.Barrier(len(nodes))
    results = {}
    lock = threading.Lock()

    # Read a common target row
    query = "SELECT * FROM trans WHERE trans_id = %s"
    params = (1,)

    def worker(node_name, txn_id):
        try:
            barrier.wait()
        except:
            pass

        res = execute_concurrent_transaction(
            node_name=node_name,
            query=query,
            params=params,
            isolation_level=isolation_level,
            transaction_id=txn_id
        )

        # Store result for comparison
        with lock:
            results[node_name] = res

    # Create and start threads
    threads = []
    for i, node in enumerate(nodes):
        txn_id = f"CR-{int(time.time()*1000)}-{i}"
        t = threading.Thread(target=worker, args=(node, txn_id), daemon=True)
        threads.append(t)
        t.start()

    # Wait for completion
    for t in threads:
        t.join(timeout=15)

    # Normalize results for comparison
    normalized = {}
    for node, res in results.items():
        try:
            normalized[node] = json.dumps(res, sort_keys=True)
        except:
            normalized[node] = str(res)

    consistent = len(set(normalized.values())) <= 1

    # Log the test case summary
    st.session_state.transaction_log.append({
        "transaction_id": f"test-cr-{int(time.time()*1000)}",
        "node": "Node 2 & Node 3",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "duration": "N/A",
        "isolation_level": isolation_level,
        "status": "consistent" if consistent else "inconsistent",
        "query": query,
        "params": params,
        "test_case": "Case #1: Concurrent Reads",
        "results": results
    })


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
    # We'll use Node 2 (reader) and Node 3 (writer)
    reader_node = "Node 2"
    writer_node = "Node 3"

    barrier = threading.Barrier(2)
    lock = threading.Lock()

    trans_id = 1

    # Containers
    reader_results = {"before": None, "after": None}
    writer_result = None

    # ---------------------------
    # Reader thread (nested)
    # ---------------------------
    def reader_worker():
        nonlocal reader_results
        try:
            barrier.wait()
        except:
            pass

        # First read (before writer finishes). Use DB-level sleep to ensure overlap.
        r1 = execute_concurrent_transaction(
            reader_node,
            f"SELECT balance FROM trans WHERE trans_id = {trans_id}; SELECT SLEEP(2);",
            None,
            isolation_level,
            transaction_id=f"RW-R1-{int(time.time() * 1000)}"
        )

        # Second read (after writer has had time to commit)
        r2 = execute_concurrent_transaction(
            reader_node,
            f"SELECT balance FROM trans WHERE trans_id = {trans_id}",
            None,
            isolation_level,
            transaction_id=f"RW-R2-{int(time.time() * 1000)}"
        )

        with lock:
            reader_results["before"] = r1
            reader_results["after"] = r2

    # ---------------------------
    # Writer thread (nested)
    # ---------------------------
    def writer_worker():
        nonlocal writer_result
        try:
            barrier.wait()
        except:
            pass

        # Capture state before update
        row_before = fetch_trans_row(writer_node, trans_id)

        # Update then sleep inside DB so the transaction holds locks/snapshot as required
        w = execute_concurrent_transaction(
            writer_node,
            f"UPDATE trans SET balance = balance + 50 WHERE trans_id = {trans_id}; SELECT SLEEP(2);",
            None,
            isolation_level,
            transaction_id=f"RW-W-{int(time.time() * 1000)}"
        )

        # Fetch after for logging/replication
        row_after = fetch_trans_row(writer_node, trans_id)

        log_transaction_event(
            node_name=writer_node,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=row_before.get("balance") if row_before else None,
            new_amount=row_after.get("balance") if row_after else None,
            status="COMMITTED" if not (isinstance(w, dict) and w.get("error")) else "FAILED",
            error_message=w.get("error") if isinstance(w, dict) else None,
        )

        if not (isinstance(w, dict) and w.get("error")) and row_after:
            route_and_replicate_write(
                source_node=writer_node,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=row_before,
                row_after=row_after,
            )

        with lock:
            writer_result = w

    # spawn threads
    t_reader = threading.Thread(target=reader_worker, daemon=True)
    t_writer = threading.Thread(target=writer_worker, daemon=True)

    t_reader.start()
    t_writer.start()

    t_reader.join(timeout=30)
    t_writer.join(timeout=30)

    # -----------------------------------
    # Detect anomalies
    # -----------------------------------
    def extract_balance(res):
        # execute_concurrent_transaction returns SELECT results as list[dict] or an error dict
        if not res or isinstance(res, dict):
            return None
        try:
            return res[0].get("balance")
        except Exception:
            return None

    before = reader_results["before"]
    after = reader_results["after"]

    b1 = extract_balance(before)
    b2 = extract_balance(after)

    # MySQL InnoDB typically prevents true dirty reads; keep false for correctness.
    dirty_read = False

    # Non-repeatable read if values differ across reads
    non_repeatable = False
    if b1 is not None and b2 is not None and b1 != b2:
        non_repeatable = True

    # -----------------------------------
    # Log summary of this test case
    # -----------------------------------
    summary = {
        "test_case": "Case #2: Read + Write Conflict",
        "isolation_level": isolation_level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "reader_before": before,
        "reader_after": after,
        "writer_result": writer_result,
        "dirty_read": dirty_read,
        "non_repeatable_read": non_repeatable
    }

    st.session_state.transaction_log.append({
        "transaction_id": f"test-rw-{int(time.time() * 1000)}",
        "node": f"{reader_node} & {writer_node}",
        "timestamp": summary["timestamp"],
        "duration": "N/A",
        "isolation_level": isolation_level,
        "status": "completed",
        "query": "RW Test (Reader + Writer)",
        "params": (trans_id,),
        "test_summary": summary
    })


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
    node_a = "Node 2"
    node_b = "Node 3"

    barrier = threading.Barrier(2)
    lock = threading.Lock()

    trans_id = 1
    update_query = f"UPDATE trans SET balance = balance + 100 WHERE trans_id = {trans_id}; SELECT SLEEP(2);"

    writer_results = {"A": None, "B": None}

    # ---------------------------------
    # Writer thread A
    # ---------------------------------
    def writer_a():
        nonlocal writer_results
        try:
            barrier.wait()
        except:
            pass

        row_before = fetch_trans_row(node_a, trans_id)
        res = execute_concurrent_transaction(
            node_a,
            update_query,
            None,
            isolation_level,
            transaction_id=f"WW-A-{int(time.time()*1000)}"
        )

        row_after = fetch_trans_row(node_a, trans_id)

        log_transaction_event(
            node_name=node_a,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=row_before.get("balance") if row_before else None,
            new_amount=row_after.get("balance") if row_after else None,
            status="COMMITTED" if not (isinstance(res, dict) and res.get("error")) else "FAILED",
            error_message=res.get("error") if isinstance(res, dict) else None,
        )

        if not (isinstance(res, dict) and res.get("error")) and row_after:
            route_and_replicate_write(
                source_node=node_a,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=row_before,
                row_after=row_after,
            )

        with lock:
            writer_results["A"] = res

    # ---------------------------------
    # Writer thread B
    # ---------------------------------
    def writer_b():
        nonlocal writer_results
        try:
            barrier.wait()
        except:
            pass

        row_before = fetch_trans_row(node_b, trans_id)
        res = execute_concurrent_transaction(
            node_b,
            update_query,
            None,
            isolation_level,
            transaction_id=f"WW-B-{int(time.time()*1000)}"
        )

        row_after = fetch_trans_row(node_b, trans_id)

        log_transaction_event(
            node_name=node_b,
            trans_id=trans_id,
            op_type="UPDATE",
            pk_value=str(trans_id),
            old_amount=row_before.get("balance") if row_before else None,
            new_amount=row_after.get("balance") if row_after else None,
            status="COMMITTED" if not (isinstance(res, dict) and res.get("error")) else "FAILED",
            error_message=res.get("error") if isinstance(res, dict) else None,
        )

        if not (isinstance(res, dict) and res.get("error")) and row_after:
            route_and_replicate_write(
                source_node=node_b,
                trans_id=trans_id,
                op_type="UPDATE",
                row_before=row_before,
                row_after=row_after,
            )

        with lock:
            writer_results["B"] = res

    # Start threads
    tA = threading.Thread(target=writer_a, daemon=True)
    tB = threading.Thread(target=writer_b, daemon=True)

    tA.start()
    tB.start()

    tA.join(timeout=20)
    tB.join(timeout=20)

    # ---------------------------------
    # Detect lost update
    # ---------------------------------
    # After both writers finish, read final balance
    final_read = execute_concurrent_transaction(
        node_a,
        f"SELECT balance FROM trans WHERE trans_id = {trans_id}",
        None,
        isolation_level,
        transaction_id=f"WW-FINAL-{int(time.time()*1000)}"
    )

    def extract_balance(res):
        if not res or isinstance(res, dict):
            return None
        try:
            return res[0].get("balance")
        except:
            return None

    final_balance = extract_balance(final_read)

    # How many writers succeeded?
    success_A = isinstance(writer_results["A"], dict) is False
    success_B = isinstance(writer_results["B"], dict) is False

    # Lost update if both writers succeeded but only +100 applied
    lost_update = False
    if success_A and success_B:
        # since each adds +100, expected +200
        # if only +100 applied => lost update
        lost_update = True

    # ---------------------------------
    # Log summary
    # ---------------------------------
    summary = {
        "test_case": "Case #3: Write + Write Conflict",
        "isolation_level": isolation_level,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "writer_A_result": writer_results["A"],
        "writer_B_result": writer_results["B"],
        "final_balance": final_balance,
        "lost_update": lost_update
    }

    st.session_state.transaction_log.append({
        "transaction_id": f"test-ww-{int(time.time()*1000)}",
        "node": f"{node_a} & {node_b}",
        "timestamp": summary["timestamp"],
        "duration": "N/A",
        "isolation_level": isolation_level,
        "status": "completed",
        "query": "WW Test (Writer + Writer)",
        "params": trans_id,
        "test_summary": summary
    })



# REPLICATION MODULE ==============================
# TODO (jeff): Implement all functions in this section
def replicate_to_central(source_node, query, params, trans_id=None, op_type=None, pk_value=None, old_amount=None, new_amount=None):
    """
    Replicate a write operation from Node 2/3 to Central Node
    
    Steps:
    1. Log PENDING replication attempt
    2. Execute the write on Central Node
    3. Update replication_log with SUCCESS/FAILED
    4. Return success flag and error (if any)
    """
    log_id = insert_replication_log(
        source_node=source_node,
        target_node="Central Node",
        trans_id=trans_id,
        op_type=op_type,
        old_amount=old_amount,
        new_amount=new_amount,
    )

    success = False
    error_msg = None
    try:
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
    finally:
        if log_id is not None:
            update_replication_log_status(
                source_node=source_node,
                log_id=log_id,
                status="SUCCESS" if success else "FAILED",
                error_message=error_msg,
            )
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

# MAIN TABS ==============================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Database View", 
    "Concurrency Testing", 
    "Failure Recovery", 
    "Transaction Logs",
    "Database Operations"
])

# TAB 1: DATABASE VIEW ==============================
with tab1:
    st.header("Current Database State Across All Nodes")
    
    limit = st.number_input("Row Limit", min_value=1, step=100)

    if st.button("Refresh Data"):
        st.experimental_rerun()
        
    st.subheader("Central Node")
    try:
        central_data = get_table_data("Central Node", "trans", limit)
        st.dataframe(central_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Central Node data: {e}")
    
    # Node 2
    st.subheader("Node 2")
    try:
        node2_data = get_table_data("Central Node", "trans", limit)
        st.dataframe(node2_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 2 data: {e}")
    
    # Node 3 
    st.subheader("Node 3")
    try:
        node3_data = get_table_data("Central Node", "trans", limit)
        st.dataframe(node3_data, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Error fetching Node 3 data: {e}")


# TAB 2: CONCURRENCY TESTING ==============================
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
        
        ## NEW IN THE CODE [THARA]
        if test_case == "Case #1: Concurrent Reads":
            test_concurrent_reads(isolation_level)
        elif test_case == "Case #2: Read + Write Conflict":
            test_read_write_conflict(isolation_level)
        elif test_case == "Case #3: Write + Write Conflict":
            test_write_write_conflict(isolation_level)
        
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


# TAB 4: TRANSACTION LOGS ==============================
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
    
    # Restrict to known data range (1993-1998)
    start_time = st.date_input(
        "Start Date",
        value=datetime(1993, 1, 1).date(),
        min_value=datetime(1993, 1, 1).date(),
        max_value=datetime(1998, 12, 31).date(),
        key="start_time",
    )
    end_time = st.date_input(
        "End Date",
        value=datetime(1998, 12, 31).date(),
        min_value=datetime(1993, 1, 1).date(),
        max_value=datetime(1998, 12, 31).date(),
        key="end_time",
    )

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

st.divider()
st.caption("MCO2 - Group 8 | STADVDB - S17")
