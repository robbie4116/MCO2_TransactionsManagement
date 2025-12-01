# Concurrency Testing Guide

## Summary of Changes

### 1. **DatabaseConnection Class Enhancements**
**Location:** Lines ~103-195

**Added Methods:**
- `is_connection_alive()` - Check if connection is still valid
- `ensure_connected()` - Reconnect if connection was lost
- `begin_transaction()` - Start explicit transaction without auto-commit
- `commit_transaction()` - Commit transaction and re-enable auto-commit
- `rollback_transaction()` - Rollback transaction and re-enable auto-commit

**Modified:**
- `execute_query()` - Now respects managed transactions (doesn't auto-commit if `in_transaction = True`)

**Why:** Enables holding transactions open across multiple operations for proper concurrency testing.

---

### 2. **Session State Additions**
**Location:** Lines ~95-105

**Added State Variables:**
```python
active_connection      # Stores open database connection
active_node           # Which node the transaction is on
transaction_active    # Boolean flag
transaction_operations # Log of operations in current transaction
transaction_start_time # Track transaction duration
```

**Why:** Maintains transaction state across Streamlit reruns (button clicks).

---

### 3. **New Tab: Manual Transaction Control**
**Location:** End of file (~line 2800+)

**Features:**
- Start/Stop transactions with button controls
- Execute SELECT queries (with optional FOR UPDATE locking)
- Execute UPDATE queries (with automatic row locking)
- Execute custom SQL
- View transaction operation log
- Connection health monitoring
- Transaction duration warnings

**Why:** Allows manual coordination of concurrent transactions between multiple instances.

---

## How to Test

### **Method 1: Automated Threading Tests (No Changes Needed)**

Your existing automated tests still work perfectly:

1. **Start the app:**
   ```bash
   streamlit run app.py
   ```

2. **Go to "Concurrency Testing" tab**

3. **Select test case and isolation level**

4. **Click "Run Concurrency Test"**

5. **Check results in "Transaction Logs" tab**

**These tests use threading internally and work identically locally and when deployed.**

---

### **Method 2: Manual Testing with 2 Local Instances**

#### **Setup:**

**Terminal 1:**
```bash
cd /Users/jeffrigel/Documents/GitHub/MCO2_TransactionsManagement
streamlit run app.py --server.port 8501
```

**Terminal 2:**
```bash
cd /Users/jeffrigel/Documents/GitHub/MCO2_TransactionsManagement
streamlit run app.py --server.port 8502
```

**Browser:**
- Open Chrome: `http://localhost:8501`
- Open Firefox: `http://localhost:8502`
- **Arrange windows side-by-side**

---

### **Test Case #1: Concurrent Reads**

**Goal:** Verify multiple transactions can read same row without blocking.

**Instance 1 (Port 8501):**
```
1. Go to "Manual Transaction Control" tab
2. Select Node: Central Node
3. Isolation Level: REPEATABLE READ
4. Click "▶️ START TRANSACTION"
5. Execute SELECT: trans_id = 7
   → Note the balance (e.g., 1000.00)
6. WAIT - keep transaction open
```

**Instance 2 (Port 8502) - Do immediately:**
```
1. Go to "Manual Transaction Control" tab
2. Select Node: Node 2
3. Isolation Level: REPEATABLE READ
4. Click "▶️ START TRANSACTION"
5. Execute SELECT: trans_id = 7
   → Should get result IMMEDIATELY (no blocking)
6. Compare balance - should match Instance 1
```

**Both Instances:**
```
7. Click "✅ COMMIT"
```

**✅ Expected Result:**
- Both SELECTs succeed without waiting
- Both see same data
- No blocking occurred
- Proves concurrent reads work

---

### **Test Case #2: Read + Write Conflict**

**Goal:** Demonstrate non-repeatable read vs repeatable read isolation.

#### **Test 2A: READ COMMITTED (Non-Repeatable Read)**

**Instance 1 (Reader):**
```
1. Manual Transaction Control tab
2. Select Node: Node 2
3. Isolation Level: READ COMMITTED
4. Click "▶️ START TRANSACTION"
5. Execute SELECT: trans_id = 7
   → Note balance: 1000.00
6. WAIT - keep transaction open (don't commit)
```

**Instance 2 (Writer) - Do while Instance 1 is waiting:**
```
1. Manual Transaction Control tab
2. Select Node: Node 3
3. Isolation Level: READ COMMITTED
4. Click "▶️ START TRANSACTION"
5. Execute UPDATE: trans_id = 7, balance = 1500.00
6. Click "✅ COMMIT" immediately
   → Change is now committed to database
```

**Back to Instance 1 (still in same transaction):**
```
7. Execute SELECT again: trans_id = 7
   → Balance now shows: 1500.00 ⚠️ CHANGED!
8. This is a NON-REPEATABLE READ
9. Click "✅ COMMIT"
```

**✅ Expected Result:**
- First read: 1000.00
- Second read (after writer commits): 1500.00
- Values differ in same transaction
- Non-repeatable read occurred

---

#### **Test 2B: REPEATABLE READ (Consistent Snapshot)**

**Instance 1 (Reader):**
```
1. Manual Transaction Control tab
2. Select Node: Node 2
3. Isolation Level: REPEATABLE READ ← Changed!
4. Click "▶️ START TRANSACTION"
5. Execute SELECT: trans_id = 7
   → Note balance: 1000.00
6. WAIT - keep transaction open
```

**Instance 2 (Writer):**
```
1. Manual Transaction Control tab
2. Select Node: Node 3
3. Isolation Level: REPEATABLE READ
4. Click "▶️ START TRANSACTION"
5. Execute UPDATE: trans_id = 7, balance = 1500.00
6. Click "✅ COMMIT"
```

**Back to Instance 1:**
```
7. Execute SELECT again: trans_id = 7
   → Balance STILL shows: 1000.00 ✅ Same!
8. This is REPEATABLE READ (snapshot isolation)
9. Click "✅ COMMIT"
10. Execute SELECT one more time (new transaction)
    → NOW shows: 1500.00 (after commit)
```

**✅ Expected Result:**
- Both reads in transaction show 1000.00
- Consistent snapshot maintained
- No non-repeatable read
- Proves isolation level difference

---

### **Test Case #3: Write + Write Conflict (BLOCKING)**

**Goal:** Demonstrate row locking and blocking behavior.

#### **Setup both instances first:**

**Instance 1:**
```
1. Manual Transaction Control tab
2. Select Node: Node 2
3. Isolation Level: SERIALIZABLE
4. Click "▶️ START TRANSACTION"
   → Transaction active ✓
```

**Instance 2:**
```
1. Manual Transaction Control tab
2. Select Node: Node 3
3. Isolation Level: SERIALIZABLE
4. Click "▶️ START TRANSACTION"
   → Transaction active ✓
```

#### **Now create the conflict:**

**Instance 1 (do this FIRST):**
```
5. Execute UPDATE:
   - Transaction ID: 1
   - New Balance: 1100
   - ✓ Lock row first (checked)
6. Click "✏️ Execute UPDATE"
   → Shows: "🔒 Row locked: balance = 1000"
   → Shows: "✅ UPDATE executed (not committed yet)"
7. WAIT - DON'T COMMIT YET
   → You are now holding an exclusive lock on row 1
```

**Instance 2 (do this IMMEDIATELY while Instance 1 is waiting):**
```
5. Execute UPDATE:
   - Transaction ID: 1 (SAME ROW!)
   - New Balance: 1200
   - ✓ Lock row first (checked)
6. Click "✏️ Execute UPDATE"
   → UI shows "Executing..." 
   → WATCH: It hangs! 🔒
   → This is BLOCKING - waiting for Instance 1's lock!
   
After ~5 seconds:
   → Shows error: "🔒 Lock wait timeout!"
   → "Another transaction is holding the lock"
```

**✅ Expected Result:**
- Instance 2 BLOCKED waiting for lock
- Lock wait timeout after 5 seconds
- Proves concurrency control is working
- Proves SERIALIZABLE prevents simultaneous writes

---

#### **Test 3B: Successful Sequential Writes**

Continue from above scenario:

**Instance 1:**
```
8. Click "✅ COMMIT"
   → Lock released
```

**Instance 2 (retry now):**
```
7. Click "✏️ Execute UPDATE" again
   → NOW succeeds immediately! (lock is free)
   → Shows: "✅ UPDATE executed"
8. Click "✅ COMMIT"
```

**Verify (either instance):**
```
9. Go to "Database View" tab
10. Check trans_id = 7
    → Balance should be 1200 (last write)
    → Both updates were applied sequentially
```

**✅ Expected Result:**
- After Instance 1 commits, Instance 2 proceeds
- Both updates applied successfully
- No lost update (serialized execution)

---

### **Test Case #4: Deadlock Detection**

**Goal:** Create and observe a deadlock scenario.

**Setup:** Need 2 different rows

**Instance 1:**
```
1. START TRANSACTION (SERIALIZABLE)
2. Execute UPDATE: trans_id = 7, balance = 5000
   → Locks row 1
3. WAIT (don't commit yet)
```

**Instance 2:**
```
1. START TRANSACTION (SERIALIZABLE)
2. Execute UPDATE: trans_id = 2, balance = 6000
   → Locks row 2
3. WAIT (don't commit yet)
```

**Now create circular dependency:**

**Instance 1:**
```
4. Execute UPDATE: trans_id = 2 (trying to lock row 2)
   → BLOCKS (Instance 2 has lock)
   → Waiting...
```

**Instance 2 (immediately):**
```
4. Execute UPDATE: trans_id = 7 (trying to lock row 1)
   → MySQL detects DEADLOCK!
   → One transaction is killed (victim)
   → Error: "🐀 Deadlock detected!"
```

**✅ Expected Result:**
- Circular wait detected by MySQL
- One transaction aborted as deadlock victim
- Other transaction can proceed
- Proves deadlock detection works

---

## Testing on Deployed Streamlit App

### **Deploy to Streamlit Cloud:**

1. **Commit and push changes:**
   ```bash
   git add app.py
   git commit -m "Add manual transaction control for concurrency testing"
   git push origin replication
   ```

2. **Deploy at:** https://share.streamlit.io/

3. **Configuration:**
   - Repository: robbie4116/MCO2_TransactionsManagement
   - Branch: replication
   - Main file: app.py

### **Testing on Deployed App:**

#### **Method A: Automated Tests (Easiest)**
```
1. Open your deployed app URL
2. Go to "Concurrency Testing" tab
3. Run all test cases
4. Works identically to local testing
```

#### **Method B: Multi-User Manual Testing**

**Setup:**
```
User A: Open app in Chrome
User B: Open app in Firefox (or Chrome Incognito)
```

**Coordination via:**
- Video call (can see each other's screens)
- Voice call + "OK, clicking NOW!"
- Chat: "I'm locking row 1 now..."

**Run same test cases as above**

---

## Testing Checklist

### **Automated Testing (Required for Report):**
```
□ Test Case #1 - READ UNCOMMITTED
□ Test Case #1 - READ COMMITTED
□ Test Case #1 - REPEATABLE READ
□ Test Case #1 - SERIALIZABLE

□ Test Case #2 - READ UNCOMMITTED
□ Test Case #2 - READ COMMITTED
□ Test Case #2 - REPEATABLE READ
□ Test Case #2 - SERIALIZABLE

□ Test Case #3 - READ UNCOMMITTED
□ Test Case #3 - READ COMMITTED
□ Test Case #3 - REPEATABLE READ
□ Test Case #3 - SERIALIZABLE

□ Document results in comparison table
□ Take screenshots of transaction logs
```

### **Manual Testing (For Understanding/Demo):**
```
□ Concurrent reads - both succeed
□ Read + write conflict - non-repeatable read
□ Read + write conflict - repeatable read
□ Write + write conflict - blocking observed
□ Write + write conflict - lock timeout
□ Sequential writes after commit
□ Deadlock scenario (optional)

□ Test with 2 local instances
□ Test on deployed app (if presenting)
□ Record video of blocking behavior (optional)
```

---

## Troubleshooting

### **Issue: Transaction won't start**
```
Error: "Failed to connect to Node X"

Solution:
- Check node is online in sidebar
- Verify database credentials in NODE_CONFIGS
- Check network connection
```

### **Issue: "Connection lost" error**
```
Appears after long idle time

Solution:
- This is normal (MySQL timeout)
- Click ROLLBACK to clear
- Start new transaction
- On deployed app: commit transactions within 60 seconds
```

### **Issue: No blocking observed in Test Case #3**
```
Both updates succeed immediately

Cause:
- Not using FOR UPDATE lock
- Auto-commit still enabled somewhere

Solution:
- Ensure "Lock row first" is CHECKED
- Verify isolation level is SERIALIZABLE
- Verify transaction is ACTIVE before operation
```

### **Issue: Can't open 2 instances locally**
```
Error: "Address already in use"

Solution:
Terminal 1: streamlit run app.py --server.port 8501
Terminal 2: streamlit run app.py --server.port 8502
```

### **Issue: Both instances share same session**
```
On deployed app, changes in one browser appear in another

Cause:
- Using same browser with shared cookies

Solution:
- Use different browsers (Chrome + Firefox)
- Use incognito/private mode
- Use different devices
```

---

## What to Document in Your Report

### **1. Test Results Table:**
```
| Test Case | Isolation Level | Threads | Success | Blocking | Anomalies | Throughput | Notes |
|-----------|----------------|---------|---------|----------|-----------|------------|-------|
| Case #1   | READ UNCOMMITTED | 3     | 100%    | No       | None      | High       | All read same data |
| ...       | ...            | ...     | ...     | ...      | ...       | ...        | ... |
```

### **2. Isolation Level Comparison:**
```
Which isolation level supports highest concurrent transaction volume 
while maintaining consistency?

Answer: READ COMMITTED
Justification:
- Prevents dirty reads (consistency ✓)
- Allows high throughput (non-repeatable reads acceptable for many use cases)
- Minimal blocking compared to REPEATABLE READ/SERIALIZABLE
- Throughput: X.X TPS vs Y.Y TPS for SERIALIZABLE
```

### **3. Screenshots to Include:**
- Transaction logs showing concurrent execution
- Blocking behavior (Instance 2 waiting)
- Lock timeout error message
- Final database state showing consistency
- Replication logs showing success

### **4. Evidence of Testing:**
- Video of 2-instance testing (optional but impressive)
- Terminal output showing both instances running
- Browser screenshots side-by-side
- Transaction operation logs from both instances

---

## Quick Reference

### **Start Transaction:**
```
SELECT Node → SELECT Isolation → START TRANSACTION
```

### **Read with Lock:**
```
SELECT (Read) → Enter trans_id → ✓ Lock row → Execute SELECT
```

### **Update with Lock:**
```
UPDATE (Write) → Enter trans_id + balance → ✓ Lock row first → Execute UPDATE
```

### **End Transaction:**
```
COMMIT (make permanent) or ROLLBACK (undo changes)
```

### **Key Isolation Levels:**
- **READ COMMITTED**: Best for high throughput (recommended)
- **REPEATABLE READ**: Consistent snapshots (good balance)
- **SERIALIZABLE**: Strictest (most blocking/deadlocks)

---

## Summary

**What Changed:**
1. ✅ Added transaction lifecycle control to DatabaseConnection
2. ✅ Added session state for manual transactions
3. ✅ Created Manual Transaction Control tab with full UI
4. ✅ Modified execute_query to support open transactions

**How to Test:**
1. ✅ Automated: Use existing "Concurrency Testing" tab (no changes needed)
2. ✅ Manual: Use new "Manual Transaction Control" tab with 2 instances
3. ✅ Deployed: Works on Streamlit Cloud with multiple users

**For Your Report:**
- Use automated tests for systematic data collection
- Use manual tests for demonstrations and understanding
- Document isolation level comparison with metrics
- Include evidence of concurrent execution and consistency

**Ready to test!** 🚀
