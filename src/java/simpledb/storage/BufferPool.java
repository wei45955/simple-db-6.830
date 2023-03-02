package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionLockManager;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    // use for LRU mechanism
    private static class PageNode {
        PageNode prev; PageNode next;
        PageId key; Page value;
        PageNode(PageId key, Page value) {
            this.key = key; this.value = value;
        }
        public PageNode() {}
    }
    private final PageNode head;
    private final PageNode tail;
    private void moveToTail(PageNode cur, boolean needToDeleteOriNode) {
        if(needToDeleteOriNode) {
            deleteNode(cur);
        }
        cur.next = tail;
        cur.prev = tail.prev;
        tail.prev.next = cur;
        tail.prev = cur;
    }
    private void deleteNode(PageNode toDelete) {
        toDelete.prev.next = toDelete.next;
        toDelete.next.prev = toDelete.prev;
    }
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final Map<PageId, PageNode> pagePool = new ConcurrentHashMap<>();

    private final Map<TransactionId, Set<PageId>> transactionPageMap  = new ConcurrentHashMap<>();

    private final TransactionLockManager transactionLockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.transactionLockManager = new TransactionLockManager();
        // use for LRU
        tail = new PageNode(); head = new PageNode();
        tail.prev = head; head.next = tail;
    }

    public static int getPageSize() {
      return BufferPool.pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // 获取读写锁 在事务结束的时候要释放锁
        this.transactionLockManager.lock(tid, pid, perm);

        Set<PageId> pids = this.transactionPageMap.getOrDefault(tid, new HashSet<>());
        pids.add(pid);
        this.transactionPageMap.put(tid, pids);

        if(this.pagePool.containsKey(pid)) {
            moveToTail(this.pagePool.get(pid), true);
            return this.pagePool.get(pid).value;
        }

        if(this.pagePool.size() >= this.numPages) {
            this.evictPage();
        }

        Page gotPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        PageNode toAdd = new PageNode(pid, gotPage);
        this.pagePool.put(pid, toAdd);
        moveToTail(toAdd, false);

        return gotPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // This method is primarily used for testing, and at the end of transactions.
        this.transactionLockManager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.transactionLockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
//        if(commit) {
//            Set<PageId> list = this.transactionPageMap.get(tid);
//            for(PageId id : list) {
//                if(this.pagePool.containsKey(id)) {
//                    this.pagePool.put(id, this.pagePool.get(id).getBeforeImage());
//                }
//            }
//        }

        if(commit) {
            try {
                flushPages(tid, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        // don't need now, because some page may hava flush to disk
//        else {
//            for(PageId pageId : this.transactionPageMap.get(tid)) {
//                if (pagePool.get(pageId) != null && tid.equals(pagePool.get(pageId).isDirty())) {
//                    // discardPage to impl revert page
//                    this.discardPage(pageId);
//                }
//            }
//        }

        // 因为可能创建了事务，但是没有读取过页，就提交事务
        if(this.transactionPageMap.containsKey(tid)) {
            // 必须要将锁释放，否则前面的DeleteTest 和 InsertTest都死循环
            for(PageId pageId : this.transactionPageMap.get(tid)) {
                this.transactionLockManager.unlock(tid, pageId);
            }

            // 因为事务已经结束，也就不需要保存他获取过的 pageId 集合了
            this.transactionPageMap.remove(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile toInsertDbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> addedPageList = toInsertDbFile.insertTuple(tid, t);

        // mark as dirty
        for(Page dirtiedPage : addedPageList) {
            // 下面这一句不需要，因为对应的 DbFile 对象就是从BufferPool获取的（getPage），对这个引用更新，就是同步的
            // this.pagePoll.put(page.getId() , page);
            dirtiedPage.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = file.deleteTuple(tid, t);

        // mark as dirty
        for (Page dirtiedPage : dirtiedPages)
            dirtiedPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Map.Entry<PageId, PageNode> entry : pagePool.entrySet()) {
            this.flushPage(entry.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        PageNode toDiscard = pagePool.get(pid);
        if(toDiscard != null) {
            deleteNode(toDiscard);
        }
        this.pagePool.remove(pid);
    }

    private synchronized void flushPage(PageId pid) throws IOException {
        flushPage(pid,false);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid, boolean needSetBeforeImage) throws IOException {
        if(!this.pagePool.containsKey(pid))
            return;
        Page toFlushPage = this.pagePool.get(pid).value;

        if(toFlushPage.isDirty() != null) {

            // start:add at lab 6 start, to make steal and no force
            Database.getLogFile().logWrite(toFlushPage.isDirty(), toFlushPage.getBeforeImage(), toFlushPage);
            Database.getLogFile().force();
            // add end

            DbFile tableFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            tableFile.writePage(toFlushPage);
            toFlushPage.markDirty(false, null);

            // add at lab 6 start
            if(needSetBeforeImage) {
                toFlushPage.setBeforeImage();
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid, boolean setBeforeImage) throws IOException {
        if(!this.transactionPageMap.containsKey(tid))
            return;
        Set<PageId> list = this.transactionPageMap.get(tid);
        for(PageId id : list) {
            this.flushPage(id, setBeforeImage);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // random pick
        PageNode toDelete = head.next;
        // todo flush page related, how to deal with actually?
        try {
            flushPage(toDelete.key);
        } catch (IOException e) {
            throw new DbException("flush page IOException");
        }
        deleteNode(toDelete);
        this.pagePool.remove(toDelete.key);
    }

}
