# Quick Start Guide - Concurrency Testing

## 🚀 Start Testing in 5 Minutes

### **Option 1: Automated Tests (Recommended for Report Data)**

```bash
# 1. Start the app
streamlit run app.py

# 2. In browser:
- Go to "Concurrency Testing" tab
- Select isolation level: REPEATABLE READ
- Select test case: Case #1, #2, or #3
- Click "Run Concurrency Test"
- Check results in "Transaction Logs" tab

# 3. Repeat for all isolation levels
- READ UNCOMMITTED
- READ COMMITTED  
- REPEATABLE READ
- SERIALIZABLE

# Done! ✅
```

---

### **Option 2: Manual 2-Instance Testing (For Demonstrations)**

**Terminal Setup:**
```bash
# Terminal 1
streamlit run app.py --server.port 8501

# Terminal 2  
streamlit run app.py --server.port 8502
```

**Browser:**
- Chrome: http://localhost:8501
- Firefox: http://localhost:8502

**Quick Test:**
```
Instance 1:
1. Go to "Manual Transaction Control" tab
2. Select Node: Node 2
3. Click "START TRANSACTION"
4. Execute SELECT: trans_id = 1
5. WAIT (don't commit)

Instance 2 (immediately):
1. Same tab
2. Select Node: Node 3
3. Click "START TRANSACTION"
4. Execute SELECT: trans_id = 1
   → Should work immediately! ✅

Both: Click "COMMIT"
```

---

## 📊 What You Need for Report

### **1. Run All Test Combinations:**
```
3 Test Cases × 4 Isolation Levels = 12 Tests Total

□ Case #1 (Concurrent Reads) - All isolation levels
□ Case #2 (Read + Write) - All isolation levels  
□ Case #3 (Write + Write) - All isolation levels
```

### **2. Document Results:**
```
For each test, record:
- Number of concurrent transactions
- Success rate (% committed)
- Number of deadlocks/timeouts
- Anomalies detected (dirty read, non-repeatable, lost update)
- Average duration
- Throughput (transactions/second)
```

### **3. Answer Key Question:**
```
"Which isolation level supports highest transaction volume 
while maintaining data consistency?"

Expected answer: READ COMMITTED
- Prevents dirty reads ✓
- High throughput ✓
- Minimal blocking ✓
- Acceptable for most use cases ✓
```

---

## 🎯 Test Each Isolation Level

### **READ UNCOMMITTED**
```
✅ Highest throughput
❌ Allows dirty reads
❌ Lost updates possible
⚠️ NOT RECOMMENDED for financial data
```

### **READ COMMITTED**
```
✅ High throughput
✅ Prevents dirty reads
⚠️ Non-repeatable reads possible
✅ RECOMMENDED - best balance
```

### **REPEATABLE READ**
```
✅ Consistent snapshots
✅ No non-repeatable reads
⚠️ Medium throughput
⚠️ Some phantom reads (MySQL InnoDB prevents most)
```

### **SERIALIZABLE**
```
✅ Strictest isolation
✅ Prevents all anomalies
❌ Lowest throughput
❌ High blocking/deadlocks
⚠️ Use only when necessary
```

---

## 🔍 Key Features to Demonstrate

### **Concurrent Reads (Case #1)**
```
✅ Show: Multiple readers access same data simultaneously
✅ Show: No blocking occurs
✅ Show: All get consistent data
```

### **Read-Write Conflict (Case #2)**  
```
✅ Show: Non-repeatable read at READ COMMITTED
✅ Show: Snapshot isolation at REPEATABLE READ
✅ Show: Reader doesn't block writer
```

### **Write-Write Conflict (Case #3)**
```
✅ Show: Row locking (FOR UPDATE)
✅ Show: Blocking behavior (one waits for other)
✅ Show: Lock timeout after 5 seconds
✅ Show: Sequential execution prevents lost updates
✅ Show: Deadlock detection (optional)
```

---

## 📸 Screenshots Needed

```
□ Sidebar showing all 3 nodes online
□ Concurrency Testing tab with test selection
□ Transaction logs showing concurrent execution
□ Manual Transaction Control - both instances side-by-side
□ Blocking behavior (Instance 2 waiting)
□ Lock timeout error message
□ Database View showing final consistent state
□ Replication logs showing SUCCESS
□ Recovery logs after failure testing
```

---

## ⚡ Pro Tips

1. **Start with automated tests** - faster, repeatable, systematic
2. **Use manual tests for demos** - more visual, interactive
3. **Test Case #3 on SERIALIZABLE** - most interesting (shows blocking)
4. **Record video** - helpful for report and presentations
5. **Test locally first** - easier to debug
6. **Deploy for presentation** - more impressive

---

## 🐛 Common Issues

**"Transaction won't start"**
```
→ Check node status in sidebar
→ Verify database connection
```

**"No blocking observed"**
```
→ Ensure "Lock row first" is CHECKED
→ Use SERIALIZABLE isolation
→ Verify transaction is ACTIVE
```

**"Can't open 2 instances"**
```
→ Use different ports: --server.port 8501 and 8502
→ Or use different browsers
```

**"Connection lost"**
```
→ Normal after idle time
→ Click ROLLBACK and start new transaction
```

---

## 📚 Full Documentation

See `TESTING_GUIDE.md` for:
- Detailed test procedures
- Expected results for each scenario  
- Troubleshooting guide
- Report documentation guidelines
- Deployment instructions

---

## ✅ Success Criteria

Your implementation is working correctly when:

1. ✅ Automated tests complete without errors
2. ✅ All 3 nodes show as online
3. ✅ Concurrent reads succeed simultaneously (Case #1)
4. ✅ Non-repeatable reads occur at READ COMMITTED (Case #2)
5. ✅ Blocking/timeout occurs in write-write conflict (Case #3)
6. ✅ Replication keeps all nodes consistent
7. ✅ Recovery restores failed nodes successfully
8. ✅ Transaction logs show proper sequencing

---

**Ready to test! Start with automated tests, then try manual 2-instance testing.** 🚀
