package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Map<Field, ArrayList<Integer>> groups;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // the big bug that confuse long time
        if((gbfield == NO_GROUPING && gbfieldtype != null))
            throw new IllegalArgumentException();

        this.groups = new HashMap<>();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbFieldGot = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        if(gbFieldGot != null && gbFieldGot.getType() != this.gbfieldtype)
            throw new RuntimeException("gbfieldtype != tup field type");

        // check the afield th field Type
        Field afieldGot = tup.getField(this.afield);
        if(afieldGot.getType() != Type.INT_TYPE)
            throw new RuntimeException("got tuple afield != Integer");

        // initialize the list
        if(!this.groups.containsKey(gbFieldGot))
            this.groups.put(gbFieldGot, new ArrayList<>());


        this.groups.get(gbFieldGot).add(((IntField) afieldGot).getValue());
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        // don't need the String field name, because the Aggregator will fill it
        if(this.gbfield == NO_GROUPING)
            td = new TupleDesc(new Type []{Type.INT_TYPE});
        else
            td = new TupleDesc(new Type []{this.gbfieldtype, Type.INT_TYPE});

        ArrayList<Tuple> tupleList = new ArrayList<>();
        for(Map.Entry<Field, ArrayList<Integer>> entry : this.groups.entrySet()) {
            Field gbFieldGot = entry.getKey();
            ArrayList<Integer> list = entry.getValue();
            Tuple tuple = new Tuple(td);
            int curRstVal = 0;
            switch (this.what) {
                case COUNT:
                    curRstVal = list.size();
                    break;
                case MAX:
                    curRstVal = list.get(0);
                    for(int num : list) {
                        curRstVal = Math.max(curRstVal, num);
                    }
                    break;
                case MIN:
                    curRstVal = list.get(0);
                    for(int num : list) {
                        curRstVal = Math.min(curRstVal, num);
                    }
                    break;
                case SUM:
                    for(int num : list) {
                        curRstVal += num;
                    }
                    break;
                case AVG:
                    for(int num : list) {
                        curRstVal += num;
                    }
                    curRstVal /= list.size();
                    break;
                case SC_AVG:
                case SUM_COUNT:
                    throw new NotImplementedException();
            }
            if(this.gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(curRstVal));
            } else {
                tuple.setField(0, gbFieldGot);
                tuple.setField(1, new IntField(curRstVal));
            }

            tupleList.add(tuple);
        }

        return new TupleIterator(td, tupleList);
    }

}
