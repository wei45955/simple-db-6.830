package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
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
        int pageSize = BufferPool.getPageSize();
        int pageOffset = id.getPageNumber() * pageSize;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            byte[] bytes = new byte[pageSize];
            randomAccessFile.read(bytes, pageOffset, pageSize);

            return new HeapPage(id, bytes);
        } catch (IOException e) {
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final HeapFile heapFile;

    HeapPage curPage;
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
        super.close();
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }
}

