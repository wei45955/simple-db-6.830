package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private Aggregator aggregator;
    private final OpIterator child;
    private final int gfield;
    private final int afield;
    private final Aggregator.Op aop;
    private OpIterator it;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.gfield = gfield;
        this.afield = afield;
        this.aop = aop;
        this.it = null;
        this.aggregator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if(this.gfield == Aggregator.NO_GROUPING) return null;
        return this.child.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        this.child.open();

        Type gbtype = null;
        if (gfield != Aggregator.NO_GROUPING)
            gbtype = this.child.getTupleDesc().getFieldType(this.gfield); // 必须要判断，因为gfield可能为 -1

        if(child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(this.gfield, gbtype, this.afield, aop);
        } else {
            this.aggregator = new StringAggregator(this.gfield, gbtype, this.afield, aop);
        }

        while(child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(child.next());
        }

        this.it = this.aggregator.iterator();
        this.it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (it != null && it.hasNext())
            return it.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggFieldName = nameOfAggregatorOp(this.aop) + " (" + this.aggregateFieldName() + ")";

        if (this.groupField() == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[] { this.child.getTupleDesc().getFieldType(this.aggregateField()) },
                    new String[] { aggFieldName });
        } else {
            return new TupleDesc(
                    new Type[] {
                            this.child.getTupleDesc().getFieldType(this.groupField()),
                            this.child.getTupleDesc().getFieldType(this.aggregateField())
                    },
                    new String[] {
                            this.child.getTupleDesc().getFieldName(this.groupField()),
                            aggFieldName
                    });
        }

    }

    public void close() {
        super.close();
        this.child.close();
        this.it.close();
        this.it = null;
        this.aggregator = null;
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
