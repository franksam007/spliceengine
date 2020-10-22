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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

class ResultHelper {
    public VarcharColumn addVarchar(String name, int length) {
        columnDescriptors.add(new GenericColumnDescriptor(name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, length)));
        VarcharColumn c = new VarcharColumn(columnDescriptors.size());
        columns.add(c);
        return c;
    }

    public BigintColumn addBigint(String name, int length)
    {
        columnDescriptors.add(new GenericColumnDescriptor(name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, length)));
        BigintColumn c = new BigintColumn(columnDescriptors.size());
        columns.add(c);
        return c;
    }

    ArrayList<GenericColumnDescriptor> columnDescriptors = new ArrayList<>();
    public int numColumns() {
        return columnDescriptors.size();
    }

    public ResultColumnDescriptor[] getColumnDescriptorsArray() {
        return columnDescriptors.toArray(new ResultColumnDescriptor[columnDescriptors.size()]);
    }

    void newRow()
    {
        finishRow();
        row = new ValueRow(numColumns());
    }
    void finishRow() {
        if( row == null ) return;
        for(Column c : columns) c.finishRow();
        rows.add(row);
        row = null;
    }

    public ResultSet getResultSet() throws SQLException {
        finishRow();

        EmbedConnection conn = (EmbedConnection) BaseAdminProcedures.getDefaultConn();
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();

        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, getColumnDescriptorsArray(), lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
        return new EmbedResultSet40(conn, resultsToWrap, false, null, true);
    }

    class Column {
        int index;
        boolean set = false;
        public Column(int index) {
            this.index = index;
        }

        public void finishRow() {
            if( !set ) init();
            set = false;
        }

        void init() {}
    }

    class VarcharColumn extends Column {
        public VarcharColumn(int index) {
            super(index);
        }

        public void set(String value) {
            assert row != null;
            row.setColumn(index, new SQLVarchar(value));
            set = true;
        }
        @Override
        public void init() {
            set("");
        }
    }
    class BigintColumn extends Column {
        public BigintColumn(int index) {
            super(index);
        }

        public void set(long value) {
            assert row != null;
            row.setColumn(index, new SQLLongint(value));
            set = true;
        }
        @Override
        public void init() {
            set(0);
        }
    }
    List<ExecRow> rows = new ArrayList<>();
    ArrayList<Column> columns = new ArrayList<>();
    ExecRow row;
}
