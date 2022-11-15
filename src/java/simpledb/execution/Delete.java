package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TupleDesc tupleDesc;
    private final TransactionId tid;
    private final OpIterator child;
    private OpIterator it;
    private boolean flag;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"deleted tuple count"});
        this.it = null;
        this.flag = false;
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
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
                Database.getBufferPool().deleteTuple(this.tid, this.child.next());
                cnt ++;
            } catch (IOException e) {
                throw new DbException("delete tuple fail");
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
        this.child.close();
        this.it.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
