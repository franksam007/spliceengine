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

package com.splicemachine.access.client;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import static com.splicemachine.access.client.ClientRegionConstants.FLUSH;

public class MemstoreKeyValueScanner implements KeyValueScanner, InternalScanner{
    protected static final Logger LOG=Logger.getLogger(MemstoreKeyValueScanner.class);
    protected ResultScanner resultScanner;
    protected Result currentResult;
    protected Result previousResult;
    protected KeyValue peakKeyValue;
    protected Cell[] cells;
    int cellScannerIndex=0;
    private boolean closed=false;
    private long rows = 0;

    public MemstoreKeyValueScanner(ResultScanner resultScanner) throws IOException{
        assert resultScanner!=null:"Passed Result Scanner is null";
        this.resultScanner=resultScanner;
        nextResult();
    }

    public Cell current(){
        if(cells==null) return null;
        return (cellScannerIndex<0)?null:this.cells[cellScannerIndex];
    }

    public boolean advance(){
        return cells!=null && ++cellScannerIndex<this.cells.length;
    }

    public boolean nextResult() throws IOException{
        cellScannerIndex=0;
        previousResult = currentResult;
        currentResult = this.resultScanner.next();
        if(currentResult == null) {
            // This shouldn't happen, throw exception and re-init the scanner
            throw new DoNotRetryIOException("Memstore scanner shouldn't end prematurely");
        }
        if (currentResult.rawCells() != null && CellUtil.matchingFamily(currentResult.rawCells()[0], FLUSH) ) {
            if (LOG.isTraceEnabled())
                LOG.trace("CURRENT CELL: " + currentResult.rawCells()[0]);
            assert currentResult.rawCells().length == 1;
            if (previousResult != null && previousResult.rawCells() != null) {
                assert previousResult.rawCells().length > 0;
                KeyValue previousCell = (KeyValue) previousResult.rawCells()[previousResult.rawCells().length - 1];
                if (LOG.isTraceEnabled())
                    LOG.trace("PREVIOUS CELL: " + previousCell);
                if (CellUtil.matchingRow(currentResult.rawCells()[0], HConstants.EMPTY_START_ROW)) {
                    byte[] rowId = Bytes.add(CellUtil.cloneRow(previousCell), new byte[]{0});
                    currentResult = Result.create(new KeyValue[]{new KeyValue(rowId, FLUSH, FLUSH, previousCell.getTimestamp(), new byte[0])});
                    if (LOG.isTraceEnabled())
                        LOG.trace("NEW CELL: " + currentResult.rawCells()[0]);
                }
            }
        }
        cells=currentResult.rawCells();
        peakKeyValue = (KeyValue) current();
        rows++;
        return true;
    }


    @Override
    public KeyValue peek(){
        return peakKeyValue;
    }

    @Override
    public KeyValue next() throws IOException{
        KeyValue returnValue=peakKeyValue;
        if(currentResult!=null && advance())
            peakKeyValue=(KeyValue)current();
        else{
            nextResult();
            returnValue=peakKeyValue;
        }
        return returnValue;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        if(currentResult!=null){
            // If we already have results we have to sort after adding ours, see SPLICE-1463
            boolean needsSorting = results.size() > 0;
            results.addAll(currentResult.listCells());
            if (needsSorting) {
                Collections.sort(results, SpliceKVComparator.INSTANCE);
            }
            nextResult();
            return true;
        }
        return false;
    }

    @Override
    public boolean seekToLastRow() throws IOException{
        return false;
    }

    @Override
    public boolean seek(Cell key) throws IOException{
        while(KeyValue.COMPARATOR.compare(peakKeyValue,key)>0 && peakKeyValue!=null){
            next();
        }
        return peakKeyValue!=null;
    }

    @Override
    public boolean reseek(Cell key) throws IOException{
        return seek(key);
    }

    @Override
    public boolean requestSeek(Cell kv,boolean forward,boolean useBloom) throws IOException{
        if(!forward)
            throw new UnsupportedOperationException("Backward scans not supported");
        return seek(kv);
    }

    @Override
    public boolean backwardSeek(Cell key) throws IOException{
        throw new UnsupportedOperationException("Backward scans not supported");
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException{
        throw new UnsupportedOperationException("Backward scans not supported");
    }

    @Override
    public long getSequenceID(){
        return Long.MAX_VALUE; // Set the max value - we have the most recent data
    }

    @Override
    public void close(){
        if(closed) return;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Closed MemstoreScanner after reading " + rows + " rows.");
        resultScanner.close();
        closed=true;
    }

    /**
     *
     * Different signature between 1.0 and 1.1
     *
    */

    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> bytes, long l) {
        return true;
    }

    @Override
    public boolean realSeekDone(){
        return true;
    }

    @Override
    public void enforceSeek() throws IOException{
    }

    @Override
    public boolean isFileScanner(){
        return false;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public Cell getNextIndexedKey() {
        return null;
    }
}