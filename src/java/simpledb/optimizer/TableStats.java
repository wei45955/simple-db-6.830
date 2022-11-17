package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tableName) {
        return statsMap.get(tableName);
    }

    public static void setTableStats(String tableName, TableStats stats) {
        statsMap.put(tableName, stats);
    }

    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int ioCostPerPage;
    private final TupleDesc tupleDesc;
    private final int tableId;

    private final IntHistogram[] intHistograms;
    private final StringHistogram[] stringHistograms;

    private int numTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        this.numTuples = 0;
        this.ioCostPerPage = ioCostPerPage;
        this.tableId = tableid;
        this.tupleDesc = Database.getCatalog().getTupleDesc(tableid);

        int n = this.tupleDesc.numFields();

        this.intHistograms = new IntHistogram[n];
        this.stringHistograms = new StringHistogram[n];
        TransactionId tId = new TransactionId();
        SeqScan seqScan = new SeqScan(tId, tableid);


        int[] minOfEachField = new int[n];
        Arrays.fill(minOfEachField,Integer.MAX_VALUE);
        int[] maxOfEachField = new int[n];
        Arrays.fill(maxOfEachField,Integer.MIN_VALUE);

        try {
            seqScan.open();
            while(seqScan.hasNext()) {
                Tuple curTup = seqScan.next();
                this.numTuples ++;
                for(int i = 0; i < n; ++i) {
                    if(this.tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        minOfEachField[i] = Math.min(minOfEachField[i], ((IntField) curTup.getField(i)).getValue());
                        maxOfEachField[i] = Math.max(maxOfEachField[i], ((IntField) curTup.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        for(int i = 0;i < n; ++i) {
            if(this.tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                this.intHistograms[i] = new IntHistogram(NUM_HIST_BINS, minOfEachField[i], maxOfEachField[i]);
            } else {
                this.stringHistograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        try {
            seqScan.rewind();
            while(seqScan.hasNext()) {
                Tuple curTup = seqScan.next();
                for(int i = 0; i < n; ++i) {
                    if(this.tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        this.intHistograms[i].addValue(((IntField) curTup.getField(i)).getValue());
                    } else {
                        this.stringHistograms[i].addValue(((StringField) curTup.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        // String tableName = Database.getCatalog().getTableName(tableid);
        // TableStats.setTableStats(tableName, this);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO 如果不是heapfile
        int numPages = ((HeapFile) Database.getCatalog().getDatabaseFile(this.tableId)).numPages();
        return this.ioCostPerPage * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(this.tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return this.intHistograms[field].estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return this.stringHistograms[field].estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.numTuples;
    }

}
