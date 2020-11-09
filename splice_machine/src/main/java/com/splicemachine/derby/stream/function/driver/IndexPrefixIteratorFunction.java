/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function.driver;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexPrefixIteratorOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.Iterators;
import splice.com.google.common.collect.PeekingIterator;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import splice.com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

import static com.splicemachine.EngineDriver.isMemPlatform;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

public class IndexPrefixIteratorFunction extends SpliceFlatMapFunction<SpliceOperation,Iterator<ExecRow>, ExecRow> {
    boolean initialized;
    protected IndexPrefixIteratorOperation driverOperation;
    protected SpliceOperation leftSide;
    protected SpliceOperation rightSide;
    private PeekingIterator<ExecRow> leftPeekingIterator;
    private Iterator<ExecRow> finalIterator;
    private static final boolean IS_MEM_PLATFORM = isMemPlatform();
    private final SIDriver driver = SIDriver.driver();
    private ExecRow currentRow;
    private ScanSetBuilder scanSetBuilder;
    private DataSetProcessor dsp;

    public IndexPrefixIteratorFunction() {
        super();
    }

    public IndexPrefixIteratorFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    protected class BufferedIterator implements PeekingIterator<ExecRow> {
        private Queue<Iterator<ExecRow>> rowIteratorQueue = new LinkedList<>();
        Iterator<ExecRow> currentRowIterator;
        private PeekingIterator<ExecRow> sourceIterator;
        private boolean hasPeeked;
        private boolean firstTime = true;

        // A pointer to the next row to return when next() is called.
        private int bufferPosition;

        private void startNewScan() throws StandardException {
            bufferPosition = 0;

            if (firstTime) {
                rowIteratorQueue.clear();
            }

            {

            }
            firstTime = false;
            if (!rowIteratorQueue.isEmpty())
                startNewRightSideScan(this);
        }

        protected BufferedIterator(Iterator<ExecRow> sourceIterator) throws StandardException {
            this.sourceIterator = Iterators.peekingIterator(sourceIterator);
            initialized = true;
            startNewScan();
            if (rowIteratorQueue.isEmpty()) {
                finalIterator = Collections.emptyIterator();
            }
        }

        public ExecRow peek() {
//            hasPeeked = true;
//            if (bufferHasNextRow())
//                return rowIteratorQueue.get(bufferPosition);
//            else
//                return sourceIterator.peek();
            return currentRow;
        }

        public void remove() {
            Preconditions.checkState(!this.hasPeeked, "Can't remove after you've peeked at next");
            if (bufferHasNextRow())
                rowIteratorQueue.remove(bufferPosition);
            else
                sourceIterator.remove();
        }

        private boolean bufferHasNextRow() {
            return bufferPosition < rowIteratorQueue.size();
        }

        @Override
        public boolean hasNext() {
//            if (bufferHasNextRow())
//                return true;
//            else
            return sourceIterator.hasNext();
        }

        @Override
        public ExecRow next() {
//            hasPeeked = false;
//            if (bufferHasNextRow())
//                return bufferedRowList.get(bufferPosition++);
//
//            try {
//                fillBuffer();
//            }
//            catch (StandardException e) {
//                throw new RuntimeException(e);
//            }
//            ExecRow retval = null;
//            if (bufferHasNextRow())
//                retval = bufferedRowList.get(bufferPosition++);
            currentRow = sourceIterator.next().getClone();
            return currentRow;
        }
    }

    private void startNewRightSideScan(PeekingIterator<ExecRow> leftRows) throws StandardException {
        Iterator<ExecRow> rightIterator;

        ArrayList<Pair<ExecRow, ExecRow>> keyRows = null;

        boolean skipRightSideRead = false;

        // The mem platform doesn't support the HBase MultiRangeRowFilter.
        if (!IS_MEM_PLATFORM && leftRows instanceof BufferedIterator) {
            BufferedIterator mjIter = (BufferedIterator)leftRows;
            skipRightSideRead = (keyRows == null);
        }

        // If there are no join keys to look up in the right table,
        // don't even read the right table.
        if (skipRightSideRead)
            rightIterator = Collections.emptyIterator();
        else {
            rightSide = driverOperation.getRightOperation();
            if (rightSide.isClosed())
                rightSide.reOpen();
            DataSetProcessor dsp = EngineDriver.driver().processorFactory().localProcessor(getOperation().getActivation(), rightSide);
            rightIterator = Iterators.transform(rightSide.getDataSet(dsp).toLocalIterator(), new Function<ExecRow, ExecRow>() {
                @Override
                public ExecRow apply(@Nullable ExecRow locatedRow) {
                    operationContext.recordJoinedRight();
                    return locatedRow;
                }
            });
        }
        ((BaseActivation) driverOperation.getActivation()).setScanStartOverride(null); // reset to null to avoid any side effects
        ((BaseActivation) driverOperation.getActivation()).setScanKeys(null);
        ((BaseActivation) driverOperation.getActivation()).setScanStopOverride(null);
        ((BaseActivation) driverOperation.getActivation()).setKeyRows(null);
        if (finalIterator == null) {
            leftSide = driverOperation.getLeftOperation();
//            finalIterator =
//                createMergeJoinIterator(leftRows,
//                                        Iterators.peekingIterator(rightIterator),
//                                        driverOperation.getLeftHashKeys(),
//                                        driverOperation.getRightHashKeys(),
//                driverOperation, operationContext);  // msirek-temp
            ((AbstractMergeJoinIterator) finalIterator).registerCloseable(new Closeable() {
                @Override
                public void close() throws IOException {
                    try {
                        if (leftSide != null && !leftSide.isClosed())
                            leftSide.close();
                        if (rightSide != null && !rightSide.isClosed())
                            rightSide.close();
                        initialized = false;
                        leftSide = null;
                        rightSide = null;
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        else
            ((AbstractMergeJoinIterator) finalIterator).reInitRightRS(Iterators.peekingIterator(rightIterator));
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {
        if (leftPeekingIterator == null) {
                leftPeekingIterator = new BufferedIterator(locatedRows);

                leftPeekingIterator = Iterators.peekingIterator(locatedRows);
                driverOperation = (IndexPrefixIteratorOperation) getOperation();
                dsp = EngineDriver.driver().processorFactory().localProcessor(driverOperation.getActivation(), driverOperation);
                initialized = true;
                if (!leftPeekingIterator.hasNext())
                    return Collections.EMPTY_LIST.iterator();
                // startNewRightSideScan(leftPeekingIterator);

            scanSetBuilder = driverOperation.createTableScannerBuilder(dsp);
        }
        return leftPeekingIterator;
    }

    private int[] getColumnOrdering(SpliceOperation op) throws StandardException {
        SpliceOperation operation = op;
        while (operation != null && !(operation instanceof ScanOperation)) {
            operation = operation.getLeftOperation();
        }
        assert operation != null;

        return ((ScanOperation)operation).getColumnOrdering();
    }

    private boolean isKeyColumn(int[] columnOrdering, int col) {
        for (int keyCol:columnOrdering) {
            if (col == keyCol)
                return true;
        }

        return false;
    }

    private static boolean isNullDataValue(DataValueDescriptor dvd) {
        return dvd == null || dvd.isNull();
    }
    private static boolean isAscendingKey(int[] rightHashKeySortOrders, int keyPos) {
        boolean retval = rightHashKeySortOrders == null ||
                         rightHashKeySortOrders.length <= keyPos ||
                         rightHashKeySortOrders[keyPos] == 1;
        return retval;
    }

//    protected abstract AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<ExecRow> leftPeekingIterator,
//                                                                         PeekingIterator<ExecRow> rightPeekingIterator,
//                                                                         int[] leftHashKeys, int[] rightHashKeys, JoinOperation joinOperation, OperationContext<JoinOperation> operationContext);

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}
