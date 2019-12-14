/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLRef;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TemporaryRowHolderOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;

import static com.splicemachine.derby.impl.sql.execute.operations.ScanOperation.deSiify;

/**
* This is a class that is used to temporarily
* (non-persistently) hold rows that are used in
* language execution.  It will store them in an
* array, or a temporary conglomerate, depending
* on the number of rows.  
* <p>
* It is used for deferred DML processing.
*
*/
public class TriggerRowHolderImpl implements TemporaryRowHolder {
    public static final int DEFAULT_OVERFLOWTHRESHOLD = 5;

    protected static final int STATE_UNINIT = 0;
    public static final int STATE_INSERT = 1;
    public static final int STATE_DRAIN = 2;


    protected ExecRow[] 	rowArray;
    public int 		lastArraySlot;
    private int			numRowsIn;
    public int		state = STATE_UNINIT;

    private	long				    CID;
    private boolean					conglomCreated;
    private ConglomerateController	cc;
    private Properties				properties;
    private ScanController			scan;
    private	ResultDescription		resultDescription;
    /** Activation object with local state information. */
    public Activation						activation;

    private boolean     isUniqueStream;

    /* beetle 3865 updateable cursor use index. A virtual memory heap is a heap that has in-memory
     * part to get better performance, less overhead. No position index needed. We read from and write
     * to the in-memory part as much as possible. And we can insert after we start retrieving results.
     * Could be used for other things too.
     */
    private boolean     isVirtualMemHeap;
    private boolean     uniqueIndexCreated;
    private boolean     positionIndexCreated;
    private long        uniqueIndexConglomId;
    private long        positionIndexConglomId;
    private ConglomerateController uniqueIndex_cc;
    private ConglomerateController positionIndex_cc;
    private DataValueDescriptor[]  uniqueIndexRow = null;
    private DataValueDescriptor[]  positionIndexRow = null;
    private RowLocation            destRowLocation; //row location in the temporary conglomerate
    private SQLLongint             position_sqllong;
    int 			overflowToConglomThreshold;
    boolean                     isSpark;  // Is the query executing on spark?

    private ExecRow execRowDefinition;

    /**
     * Create a temporary row holder with the defined overflow to conglom
     *
     * @param activation the activation
     * @param properties the properties of the original table.  Used
     *		to help the store use optimal page size, etc.
     * @param resultDescription the result description.  Relevant for the getResultDescription
     * 		call on the result set returned by getResultSet.  May be null
     * @param overflowToConglomThreshold on an attempt to insert
     * 		this number of rows, the rows will be put
     *		into a temporary conglomerate.
     */
    public TriggerRowHolderImpl
    (
            Activation              activation,
            Properties              properties,
            ResultDescription       resultDescription,
            int                     overflowToConglomThreshold,
            boolean                 isUniqueStream,
            boolean                 isVirtualMemHeap,
            ExecRow                 execRowDefinition,
            boolean                 isSpark
    )
    {
        if (SanityManager.DEBUG)
        {
                if (overflowToConglomThreshold < 0)
                {
                        SanityManager.THROWASSERT("It is assumed that "+
                                "the overflow threshold is >= 0.  "+
                                "If you you need to change this you have to recode some of "+
                                "this class.");
                }
        }

        this.activation = activation;
        this.properties = properties;
        this.resultDescription = resultDescription;
        this.isUniqueStream = isUniqueStream;
        this.isVirtualMemHeap = isVirtualMemHeap;
        rowArray = new ExecRow[overflowToConglomThreshold < 1 ? 1 : overflowToConglomThreshold];
        lastArraySlot = -1;
        this.execRowDefinition = execRowDefinition;
        rowArray[0] = execRowDefinition.getClone();
        this.overflowToConglomThreshold = overflowToConglomThreshold;
        if (overflowToConglomThreshold == 0 && execRowDefinition != null) {
            try {
                createConglomerate(execRowDefinition);
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getLastArraySlot() { return lastArraySlot; }
    public void decrementLastArraySlot() { lastArraySlot--; }
    public int getState() { return state; }
    public void setState(int state) { this.state = state; }
    public Activation getActivation() { return activation; }
    public long getConglomerateId() { return CID; }

 /* Avoid materializing a stream just because it goes through a temp table.
  * It is OK to have a stream in the temp table (in memory or spilled to
  * disk). The assumption is that one stream does not appear in two rows.
  * For "update", one stream can be in two rows and the materialization is
  * done in UpdateResultSet. Note to future users of this class who may
  * insert a stream into this temp holder:
  *   (1) As mentioned above, one un-materialized stream can't appear in two
  *       rows; you need to objectify it first otherwise.
  *   (2) If you need to retrieve an un-materialized stream more than once
  *       from the temp holder, you need to either materialize the stream
  *       the first time, or, if there's a memory constraint, in the first
  *       time create a RememberBytesInputStream with the byte holder being
  *       BackingStoreByteHolder, finish it, and reset it after usage.
  *       A third option is to create a stream clone, but this requires that
  *       the container handles are kept open until the streams have been
  *       drained.
  *
  * Beetle 4896.
  */
    private ExecRow cloneRow(ExecRow inputRow)
    {
            DataValueDescriptor[] cols = inputRow.getRowArray();
            int ncols = cols.length;
            ExecRow cloned = ((ValueRow) inputRow).cloneMe();
            for (int i = 0; i < ncols; i++)
            {
                    if (cols[i] != null)
                    {
                            /* Rows are 1-based, cols[] is 0-based */
         cloned.setColumn(i + 1, cols[i].cloneHolder());
                    }
            }
            if (inputRow instanceof IndexValueRow)
                    return new IndexValueRow(cloned);
            else
                    return cloned;
    }

    private void createConglomerate(ExecRow templateRow) throws StandardException{
        if (!conglomCreated)
        {
            TransactionController tc = activation.getTransactionController();

            // TODO-COLLATE, I think collation needs to get set always correctly
            // but did see what to get collate id when there was no result
            // description.  The problem comes if row holder is used to stream
            // row to temp disk, then row is read from disk using an interface
            // where store creates the DataValueDescriptor template itself,
            // and subsquently the returned column is used for some sort of
            // comparison.  Also could be a problem is reader of tempoary
            // table uses qualifiers, that would result in comparisons internal
            // to store.  I believe the below client is incomplete - either
            // it should always be default, or real collate_ids should be
            // passed in.

            // null collate_ids in createConglomerate call indicates to use all
            // default collate ids.
            int collation_ids[] = null;

            /*
            TODO-COLLATE - if we could count on resultDescription I think the
            following would work.

            if (resultDescription != null)
            {
             // init collation id info from resultDescription for create call
             collation_ids = new int[resultDescription.getColumnCount()];

             for (int i = 0; i < collation_ids.length; i++)
             {
                 collation_ids[i] =
                     resultDescription.getColumnDescriptor(
                         i + 1).getType().getCollationType();
             }
            }
            */


            /*
            ** Create the conglomerate with the template row.
            */
            CID =
                tc.createConglomerate(false,
                 "heap",
                 templateRow.getRowArray(),
                 null, //column sort order - not required for heap
                 collation_ids,
                 properties,
                 TransactionController.IS_TEMPORARY |
                 TransactionController.IS_KEPT);

            conglomCreated = true;

            cc = tc.openConglomerate(CID,
                 false,
                 TransactionController.OPENMODE_FORUPDATE,
                 TransactionController.MODE_TABLE,
                 TransactionController.ISOLATION_SERIALIZABLE);
            if(isUniqueStream)
               destRowLocation = cc.newRowLocationTemplate();

        }

    }

    class InMemoryTriggerRowsIterator implements Iterator<ExecRow>, Closeable {
        private TriggerRowHolderImpl holder;
        private int position = 0;
        public InMemoryTriggerRowsIterator(TriggerRowHolderImpl holder)
        {
            this.holder = holder;
        }

        @Override
        public boolean hasNext() {
            if (position < overflowToConglomThreshold)
                return true;
            return false;
        }

        @Override
        public ExecRow next() {
            if (position < overflowToConglomThreshold)
                return rowArray[position++];
            return null;
        }

        @Override
        public void close() throws IOException {
            position = 0;
        }
    }
    public InMemoryTriggerRowsIterator getCachedRowsIterator() {
        return new InMemoryTriggerRowsIterator(this);
    }
    /**
     * Insert a row
     *
     * @param inputRow the row to insert
     *
     * @exception StandardException on error
     */
    public void insert(ExecRow inputRow)
            throws StandardException
    {

        if (SanityManager.DEBUG)
        {
                if(!isUniqueStream && !isVirtualMemHeap)
                        SanityManager.ASSERT(state != STATE_DRAIN, "you cannot insert rows after starting to drain");
        }
        if (! isVirtualMemHeap)
                state = STATE_INSERT;

        if(uniqueIndexCreated)
        {
                if(isRowAlreadyExist(inputRow))
                        return;
        }

        numRowsIn++;

        if (lastArraySlot + 1 < overflowToConglomThreshold)
        {
            rowArray[++lastArraySlot] = cloneRow(inputRow);

            //In case of unique stream we push every thing into the
            // conglomerates for time being, we keep one row in the array for
            // the template.
            if (!isUniqueStream) {
                return;
            }
        }

        if (!conglomCreated)
            createConglomerate(inputRow);

        int status = 0;
        if(isUniqueStream)
        {
            cc.insertAndFetchLocation(inputRow, destRowLocation);
            insertToPositionIndex(numRowsIn -1, destRowLocation);
            //create the unique index based on input row ROW Location
            if(!uniqueIndexCreated)
                    isRowAlreadyExist(inputRow);

        }else
        {
            status = cc.insert(inputRow);
            if (isVirtualMemHeap)
                    state = STATE_INSERT;
        }

        if (SanityManager.DEBUG)
        {
            if (status != 0)
            {
                    SanityManager.THROWASSERT("got funky status ("+status+") back from "+
                                    "ConglomerateConstroller.insert()");
            }
        }
    }


    /**
     * Maintain an unique index based on the input row's row location in the
     * base table, this index make sures that we don't insert duplicate rows
     * into the temporary heap.
     * @param inputRow  the row we are inserting to temporary row holder
     * @exception StandardException on error
     */


    private boolean isRowAlreadyExist(ExecRow inputRow) throws  StandardException
    {
            DataValueDescriptor		rlColumn;
            RowLocation	baseRowLocation;
            rlColumn = inputRow.getColumn(inputRow.nColumns());

            if(CID!=0 && rlColumn instanceof SQLRef)
            {
                    baseRowLocation =
                            (RowLocation) (rlColumn).getObject();

                    if(!uniqueIndexCreated)
                    {
                            TransactionController tc =
                                    activation.getTransactionController();
                            int numKeys = 2;
                            uniqueIndexRow = new DataValueDescriptor[numKeys];
                            uniqueIndexRow[0] = baseRowLocation;
                            uniqueIndexRow[1] = baseRowLocation;
                            Properties props = makeIndexProperties(uniqueIndexRow, CID);
                            uniqueIndexConglomId =
                                    tc.createConglomerate(false,
                 "BTREE",
                 uniqueIndexRow,
                 null,
                 null, // no collation needed for index on row locations.
                 props,
                 (TransactionController.IS_TEMPORARY |
                  TransactionController.IS_KEPT));

                            uniqueIndex_cc = tc.openConglomerate(
                                                            uniqueIndexConglomId,
                                                            false,
                                                            TransactionController.OPENMODE_FORUPDATE,
                                                            TransactionController.MODE_TABLE,
                                                            TransactionController.ISOLATION_SERIALIZABLE);
                            uniqueIndexCreated = true;
                    }

                    uniqueIndexRow[0] = baseRowLocation;
                    uniqueIndexRow[1] = baseRowLocation;
                    // Insert the row into the secondary index.
                    int status;
                    ExecRow unIRow = new ValueRow();
                    unIRow.setRowArray(uniqueIndexRow);
                    if ((status = uniqueIndex_cc.insert(unIRow))!= 0)
                    {
                            if(status == ConglomerateController.ROWISDUPLICATE)
                            {
                                    return true ; // okay; we don't insert duplicates
                            }
                            else
                            {
                                    if (SanityManager.DEBUG)
                                    {
                                            if (status != 0)
                                            {
                                                    SanityManager.THROWASSERT("got funky status ("+status+") back from "+
                                                                                                      "Unique Index insert()");
                                            }
                                    }
                            }
                    }
            }

            return false;
    }


    /**
     * Maintain an index that will allow us to read  from the
     * temporary heap in the order we inserted.
     * @param position - the number of the row we are inserting into heap
     * @param rl the row to Location in the temporary heap
     * @exception StandardException on error
     */

    private void insertToPositionIndex(int position, RowLocation rl ) throws  StandardException
    {
            if(!positionIndexCreated)
            {
                    TransactionController tc = activation.getTransactionController();
                    int numKeys = 2;
                    position_sqllong = new SQLLongint();
                    positionIndexRow = new DataValueDescriptor[numKeys];
                    positionIndexRow[0] = position_sqllong;
                    positionIndexRow[1] = rl;
                    Properties props = makeIndexProperties(positionIndexRow, CID);
                    positionIndexConglomId =
         tc.createConglomerate(false,
             "BTREE",
             positionIndexRow,
             null,
             null, // no collation needed for index on row locations.
             props,
             (TransactionController.IS_TEMPORARY |
              TransactionController.IS_KEPT));

                    positionIndex_cc =
         tc.openConglomerate(
             positionIndexConglomId,
             false,
             TransactionController.OPENMODE_FORUPDATE,
             TransactionController.MODE_TABLE,
             TransactionController.ISOLATION_SERIALIZABLE);

                    positionIndexCreated = true;
            }
            ExecRow vRow = new ValueRow();
            vRow.setRowArray(positionIndexRow);
            position_sqllong.setValue(position);
            positionIndexRow[0] = position_sqllong;
            positionIndexRow[1] = rl;
            //insert the row location to position index
            positionIndex_cc.insert(vRow);
    }

    /**
     * Get a result set for scanning what has been inserted
     * so far.
     *
     * @return a result set to use
     */
    public CursorResultSet getResultSet()
    {
            state = STATE_DRAIN;
            TransactionController tc = activation.getTransactionController();
            if(isUniqueStream)
            {
                    return new TemporaryRowHolderResultSet(tc, rowArray,
                                                               resultDescription, isVirtualMemHeap,
                                                               true, positionIndexConglomId, this);
            }
            else
            {
                    return new TemporaryRowHolderResultSet(tc, rowArray, resultDescription, isVirtualMemHeap, this);

            }
    }

    /**
     * Purge the row holder of all its rows.
     * Resets the row holder so that it can
     * accept new inserts.  A cheap way to
     * recycle a row holder.
     *
     * @exception StandardException on error
     */
    public void truncate() throws StandardException
    {
            close();
    if (SanityManager.DEBUG) {
     SanityManager.ASSERT(lastArraySlot == -1);
     SanityManager.ASSERT(state == STATE_UNINIT);
     SanityManager.ASSERT(!conglomCreated);
     SanityManager.ASSERT(CID == 0);
    }
            for (int i = 0; i < rowArray.length; i++)
            {
                    rowArray[i] = null;
            }

            numRowsIn = 0;
    }

    public long getTemporaryConglomId()
    {
        return getConglomerateId();
    }

    public long getPositionIndexConglomId()
    {
            return positionIndexConglomId;
    }



    private Properties makeIndexProperties(DataValueDescriptor[] indexRowArray, long conglomId ) throws StandardException {
            int nCols = indexRowArray.length;
            Properties props = new Properties();
            props.put("allowDuplicates", "false");
            // all columns form the key, (currently) required
            props.put("nKeyFields", String.valueOf(nCols));
            props.put("nUniqueColumns", String.valueOf(nCols-1));
            props.put("rowLocationColumn", String.valueOf(nCols-1));
            props.put("baseConglomerateId", String.valueOf(conglomId));
            return props;
    }

    public void setRowHolderTypeToUniqueStream()
    {
            isUniqueStream = true;
    }

    private void dropTable(long conglomID) throws StandardException {
        try {
            SIDriver driver = SIDriver.driver();
            PartitionFactory partitionFactory = driver.getTableFactory();
            PartitionAdmin partitionAdmin = partitionFactory.getAdmin();
            partitionAdmin.deleteTable(Long.toString(conglomID));
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
    /**
     * Clean up
     *
     * @exception StandardException on error
     */
    public void close() throws StandardException
    {
        if (scan != null)
        {
                scan.close();
                scan = null;
        }

        if (cc != null)
        {
                cc.close();
                cc = null;
        }

        if (uniqueIndex_cc != null)
        {
                uniqueIndex_cc.close();
                uniqueIndex_cc = null;
        }

        if (positionIndex_cc != null)
        {
                positionIndex_cc.close();
                positionIndex_cc = null;
        }

        TransactionController tc = activation.getTransactionController();

        if (uniqueIndexCreated)
        {
                tc.dropConglomerate(uniqueIndexConglomId);
                dropTable(uniqueIndexConglomId);
                uniqueIndexCreated = false;
        }

        if (positionIndexCreated)
        {
                tc.dropConglomerate(positionIndexConglomId);
                dropTable(positionIndexConglomId);
                positionIndexCreated = false;
        }

        if (conglomCreated)
        {
                tc.dropConglomerate(CID);
                dropTable(CID);

                conglomCreated = false;
                CID = 0;
        }
        else
        {
             if (SanityManager.DEBUG) {
                 SanityManager.ASSERT(CID == 0, "CID(" + CID + ")==0");
        }
     }
     state = STATE_UNINIT;
     lastArraySlot = -1;
    }
}


