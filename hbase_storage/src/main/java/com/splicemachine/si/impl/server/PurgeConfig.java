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

package com.splicemachine.si.impl.server;

import java.util.EnumSet;

public enum PurgeConfig {
    PURGE, // Purge deleted rows if no active transaction is using them
    KEEP_MOST_RECENT_TOMBSTONE, // Prevent purging of the most recent tombstone, unless first write token is present
    FORCE_PURGE; // Purge deleted rows even if transactions are still using some of them

    public static EnumSet<PurgeConfig> NO_PURGE = EnumSet.noneOf(PurgeConfig.class);
}
