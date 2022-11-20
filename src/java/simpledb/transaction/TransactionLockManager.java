package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionLockManager {

    private static class LockItem {
        private final Condition cond;
        private final Lock lock;
        private final HashSet<TransactionId> sharedTransactions = new HashSet<>();
        private TransactionId exclusiveTransaction = null;
        public LockItem() {
            this.lock = new ReentrantLock();
            this.cond = this.lock.newCondition();
        }

    }
    Map<PageId, LockItem> pageId2LockItem;

    private static final int DEADLOCKTIMEOUT = 2000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    public TransactionLockManager() {
        this.pageId2LockItem = new ConcurrentHashMap<>();
    }

    public void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if(perm == Permissions.READ_ONLY) {
            acquireS(tid, pid);
        } else {
            acquireX(tid, pid);
        }
    }

    private void acquireS(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if(!this.pageId2LockItem.containsKey(pid)) {
            this.pageId2LockItem.put(pid, new LockItem());
        }
        LockItem lockItem = this.pageId2LockItem.get(pid);

        try {
            if(!lockItem.lock.tryLock(DEADLOCKTIMEOUT, TIME_UNIT)) {
                throw new TransactionAbortedException();
            }
            while (lockItem.exclusiveTransaction != null && !lockItem.exclusiveTransaction.equals(tid)) {
                lockItem.cond.await();
            }
            lockItem.sharedTransactions.add(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lockItem.lock.unlock();
        }
    }

    private void acquireX(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if(!this.pageId2LockItem.containsKey(pid)) {
            this.pageId2LockItem.put(pid, new LockItem());
        }
        LockItem lockItem = this.pageId2LockItem.get(pid);

        try {
            if(!lockItem.lock.tryLock(DEADLOCKTIMEOUT, TIME_UNIT)) {
                throw new TransactionAbortedException();
            }
            while (lockItem.exclusiveTransaction != null && !tid.equals(lockItem.exclusiveTransaction)
                    || (!lockItem.sharedTransactions.isEmpty() &&
                    !(lockItem.sharedTransactions.size() == 1 && lockItem.sharedTransactions.contains(tid)))) {
                lockItem.cond.await();
            }
            lockItem.exclusiveTransaction = tid;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lockItem.lock.unlock();
        }
    }

    public void unlock(TransactionId tid, PageId pid) {
        LockItem lockItem = this.pageId2LockItem.get(pid);
        if(lockItem == null) return;
        lockItem.lock.lock();

        try {
            if(lockItem.exclusiveTransaction != null && lockItem.exclusiveTransaction.equals(tid)) {
                lockItem.exclusiveTransaction = null;
                lockItem.sharedTransactions.remove(tid);
                lockItem.cond.signalAll();
            } else {
                lockItem.sharedTransactions.remove(tid);
                if(lockItem.sharedTransactions.size() <= 1) {
                    lockItem.cond.signalAll();
                }
            }
        } finally {
            lockItem.lock.unlock();
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        LockItem lockItem = this.pageId2LockItem.get(pid);
        lockItem.lock.lock();
        try {
            if(lockItem.exclusiveTransaction != null) {
                return lockItem.exclusiveTransaction.equals(tid);
            } else {
                return lockItem.sharedTransactions.contains(tid);
            }
        } finally {
            lockItem.lock.unlock();
        }

    }

}
