package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private int numPages;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.numPages = (int)(this.file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId id = (HeapPageId) pid;
        if (id.getPageNumber() > this.numPages()) {
            throw new NoSuchElementException();
        }
        int pageSize = BufferPool.getPageSize();
        long pageOffset = ((long)(id.getPageNumber()))  * pageSize;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            // 有必要处理当要read的pageNo为最后一个page的下一个page，也就是这时候是需要创建一个新page
            if(pid.getPageNumber() == this.numPages) {
                // 而且需要写到文件 这里还没加上
                // update: 不需要写文件，后续会加上wal 关机或者后台线程会定期将数据写回硬盘

                this.numPages ++;
                return new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            } else {
                byte[] bytes = new byte[pageSize];
                randomAccessFile.seek(pageOffset);
                randomAccessFile.read(bytes);

                return new HeapPage(id, bytes);
            }
        } catch (IOException e) {
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();

        this.numPages = Math.max(this.numPages, pid.getPageNumber() + 1);

        long pageOffset = ((long) pid.getPageNumber()) * BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        randomAccessFile.seek(pageOffset);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if(!(t.getRecordId().getPageId() instanceof HeapPageId))
            throw new DbException("HeapFile.insertTuple got tuple is Not heapPageId");

        // to look up a page to insert Tuple
        HeapPageId canInsertPageId = null;
        for(int i = 0; i < this.numPages(); ++i) {
            HeapPageId toReadPageId = new HeapPageId(this.getId(), i);
            HeapPage toReadPage = (HeapPage) Database.getBufferPool().getPage(tid, toReadPageId, Permissions.READ_ONLY);
            if(toReadPage.getNumEmptySlots() > 0) {
                canInsertPageId = toReadPageId;
                break;
            }
        }

        List<Page> modifiedPages = new ArrayList<>();
        if(canInsertPageId != null) {
            HeapPage toInsertPage = (HeapPage) Database.getBufferPool().getPage(tid, canInsertPageId, Permissions.READ_WRITE);
            toInsertPage.insertTuple(t);
            modifiedPages.add(toInsertPage);
        } else {
            HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            modifiedPages.add(newPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if(!(t.getRecordId().getPageId() instanceof HeapPageId))
            throw new DbException("HeapFile.deleteTuple got tuple is Not heapPageId");

        HeapPageId heapPageId = (HeapPageId) t.getRecordId().getPageId();
        Page gotBufferPage = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);

        HeapPage toOperatePage = (HeapPage) gotBufferPage;
        toOperatePage.deleteTuple(t);

        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(toOperatePage);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final HeapFile heapFile;

    int nextPageNo;
    private Iterator<Tuple> tupleIterator;

    /**
     * Constructor for this iterator
     * @param f - the HeapFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.heapFile = f;
        this.tid = tid;
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }

    /**
     * Open this iterator by getting an iterator on the first page
     */
    public void open() throws DbException, TransactionAbortedException {
        // should init the nextPageNo to 0, so it will iterate on the first page
        // in rewind() may call open() twice or more
        this.nextPageNo = 0;
        HeapPage firstPage = this.getNextPage();
        if(firstPage != null) {
            this.tupleIterator = firstPage.iterator();
        }
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if(this.tupleIterator == null) return null;

        if(tupleIterator.hasNext()) {
            return tupleIterator.next();
        } else {
            HeapPage nextPage = getNextPage();
            if(nextPage == null) return null;
            this.tupleIterator = nextPage.iterator();
            return readNext();
        }
    }

    private HeapPage getNextPage() throws TransactionAbortedException, DbException {
        if(nextPageNo >= this.heapFile.numPages()) {
            return null;
        }
        // HeapPageId第一个参数是tableId，其实就是 tableId 的值，等于file.getId()也就是 file 的绝对路径哈希值（Catalog.java）
        HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(), this.nextPageNo);
        this.nextPageNo ++;
        return (HeapPage) (Database.getBufferPool().getPage(this.tid, heapPageId, Permissions.READ_ONLY));
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        // must call *** super.close() *** because the parent may do sth
        super.close();
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }
}

