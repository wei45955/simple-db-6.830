package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private final PageId pageId;
    private final int tupleNumber;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pageId = pid;
        this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RecordId)) return false;
        RecordId that = (RecordId) o;
        return this.pageId.equals(that.getPageId()) && that.getTupleNumber() == this.tupleNumber;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
       return this.pageId.hashCode() ^ this.tupleNumber ^ 451144541;
    }

}
