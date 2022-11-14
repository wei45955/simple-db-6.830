package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Map<Field, Integer> groupMap;
    private final int gbfield;
    private final Type gbfieldtype;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if((gbfield == NO_GROUPING && gbfieldtype == null) || what != Op.COUNT)
            throw new IllegalArgumentException();
        this.groupMap = new HashMap<>();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbFieldGot = tup.getField(this.gbfield);
        if(gbFieldGot.getType() != this.gbfieldtype)
            throw new RuntimeException("gbfieldtype != tup field type");
        if(!groupMap.containsKey(gbFieldGot))
            groupMap.put(gbFieldGot, 0);
        groupMap.put(gbFieldGot, groupMap.get(gbFieldGot) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        // don't need the String field name, because the Aggregator will fill it
        if(this.gbfield == NO_GROUPING)
            td = new TupleDesc(new Type []{Type.INT_TYPE});
        else
            td = new TupleDesc(new Type []{this.gbfieldtype, Type.INT_TYPE});
        ArrayList<Tuple> list = new ArrayList<>();
        for(Map.Entry<Field, Integer> entry : this.groupMap.entrySet()) {
            Field gbFieldGot = entry.getKey();
            int val = entry.getValue();

            Tuple tup = new Tuple(td);
            if(this.gbfield == NO_GROUPING) {
                tup.setField(0, new IntField(val));
            }
            else {
                tup.setField(0, gbFieldGot);
                tup.setField(1, new IntField(val));
            }
            list.add(tup);
        }

        return new TupleIterator(td, list);
    }

}
