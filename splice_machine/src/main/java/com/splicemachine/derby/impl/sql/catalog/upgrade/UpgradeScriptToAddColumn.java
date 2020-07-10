package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

public class UpgradeScriptToAddColumn extends UpgradeScriptBase {
    public UpgradeScriptToAddColumn(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        SpliceLogUtils.info(LOG, "Adding catalog versions to system tables");
        sdd.addNewColumnToSystables(tc);
    }
}