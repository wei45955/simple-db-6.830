package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionLockManager;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final Map<PageId, Page> pagePool = new ConcurrentHashMap<>();
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
            return this.pagePool.get(pid);
        }

        if(this.pagePool.size() >= this.numPages) {
            this.evictPage();
        }

        Page gotPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        this.pagePool.put(pid, gotPage);

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
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for(PageId pageId : this.transactionPageMap.get(tid)) {
                if (pagePool.get(pageId) != null && tid.equals(pagePool.get(pageId).isDirty())) {
                    // revert page
                    // 可以直接恢复的原因是
                    // 1. 写这个页只能同时只有一个事务，所以这是如果脏了，一定是本事务弄脏的
                    // 2. 我们这里的实现是事务提交后，就将页面写回硬盘，也就是说硬盘的原页面都是最干净的
                    //      （内存业内与硬盘原页不同的地方都是本事务写的）
                    Page restoredPage = Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
                    pagePool.put(pageId, restoredPage);
                }
            }
        }

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
        for(Map.Entry<PageId, Page> entry : pagePool.entrySet()) {
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
        this.pagePool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if(!this.pagePool.containsKey(pid))
            return;
        Page toFlushPage = this.pagePool.get(pid);

        if(toFlushPage.isDirty() != null) {
            DbFile tableFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            tableFile.writePage(toFlushPage);
            toFlushPage.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        if(!this.transactionPageMap.containsKey(tid))
            return;
        Set<PageId> list = this.transactionPageMap.get(tid);
        for(PageId id : list) {
            this.flushPage(id);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // random pick
        for (Page page : this.pagePool.values()) {
            if (page.isDirty() != null)
                continue;

            this.pagePool.remove(page.getId());
            // return 而不是 break
            return;
        }

        throw new DbException("Can't find a not dirty page to evict");
    }

}
