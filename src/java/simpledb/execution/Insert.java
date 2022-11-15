package simpledb.execution;

import java.io.IOException;
import java.util.ArrayList;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private OpIterator it;
    private boolean flag;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"deleted tuple count"});
        this.it = null;
        this.flag = false;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // 因为实际上 传来的child 内的Tuple 只能被insert一次，因此增加变量确保只被初始化一次
        if(this.flag) {
            assert this.it != null;
            super.open();
            this.it.open();
            return ;
        }
        this.flag = true;
        super.open();
        this.child.open();
        int cnt = 0;
        while(this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId,this.child.next());
                cnt ++;
            } catch (IOException e) {
                throw new DbException("insert tuple fail");
            }
        }
        ArrayList<Tuple> tuples = new ArrayList<>();
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0, new IntField(cnt));
        tuples.add(tuple);
        this.it = new TupleIterator(this.tupleDesc, tuples);
        this.it.open();
    }

    public void close() {
        super.close();
        // child被多次close是harmless
        this.child.close();
        this.it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(!this.flag)
            throw new DbException("not open yet");
        if(this.it.hasNext()) {
            return this.it.next();
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        throw new NotImplementedException();
    }

    @Override
    public void setChildren(OpIterator[] children) {
        throw new NotImplementedException();
    }
}