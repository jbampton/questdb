/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

public final class PgTablesRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int SCHEMA_NAME_ID = 0;
    private static final int TABLE_NAME_ID = 1;
    private static final int TABLE_OWNER_ID = 2;
    private static final int TABLE_SPACE_ID = 3;
    private static final int HAS_INDEX_ID = 4;
    private static final int HAS_RULES_ID = 5;
    private static final int HAS_TRIGGERS_ID = 6;
    private static final int ROW_SECURITY_ID = 7;

    private final PgTablesCursor cursor = new PgTablesCursor();
    private final FilesFacade ff;
    private final boolean hideTelemetryTables;
    private Path path;

    public PgTablesRecordCursorFactory(FilesFacade filesFacade, CharSequence dbRoot, boolean hideTelemetryTables) {
        super(METADATA);
        path = new Path().of(dbRoot).$();
        this.ff = filesFacade;
        this.hideTelemetryTables = hideTelemetryTables;
    }

    @Override
    protected void _close() {
        path = Misc.free(path);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(SCHEMA_NAME_ID, new TableColumnMetadata("schemaname", 1, ColumnType.STRING));
        metadata.add(TABLE_NAME_ID, new TableColumnMetadata("tablename", 2, ColumnType.STRING));
        metadata.add(TABLE_OWNER_ID, new TableColumnMetadata("tableowner", 3, ColumnType.STRING));
        metadata.add(TABLE_SPACE_ID, new TableColumnMetadata("tablespace", 4, ColumnType.STRING));
        metadata.add(HAS_INDEX_ID, new TableColumnMetadata("hasindexes", 5, ColumnType.BOOLEAN));
        metadata.add(HAS_RULES_ID, new TableColumnMetadata("hasrules", 7, ColumnType.BOOLEAN));
        metadata.add(HAS_TRIGGERS_ID, new TableColumnMetadata("hastriggers", 7, ColumnType.BOOLEAN));
        metadata.add(ROW_SECURITY_ID, new TableColumnMetadata("rowsecurity", 8, ColumnType.BOOLEAN));
        METADATA = metadata;
    }

    private final class PgTablesCursor implements RecordCursor {
        private long findPtr = 0;
        private final PgTablesRecord record = new PgTablesRecord();
        private final StringSink tableNameSink = new StringSink();

        @Override
        public void close() {
            findPtr = ff.findClose(findPtr);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            //  todo: should this return also virtual tables?
            //  like other catalogue functions, etc? Postgres does that!

            //  todo: this code to iterate on-disk tables is duplicated in 3 different places!
            //  it should probably be re-used
            while (true) {
                if (findPtr == 0) {
                    findPtr = ff.findFirst(path);
                    if (findPtr <= 0) {
                        return false;
                    }
                } else {
                    if (ff.findNext(findPtr) <= 0) {
                        return false;
                    }
                }
                if (Files.isDir(ff.findName(findPtr), ff.findType(findPtr), tableNameSink)) {
                    if (record.open(tableNameSink)) {
                        return true;
                    }
                }
            }
        }


        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException("recordB not supported");
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("random access not supported");
        }

        @Override
        public void toTop() {
            close();
        }

        @Override
        public long size() {
            return -1;
        }
    }

    private class PgTablesRecord implements Record {
        @Override
        public CharSequence getStr(int col) {
            switch (col) {
                case SCHEMA_NAME_ID: return "public";
                case TABLE_NAME_ID: return cursor.tableNameSink;
                case TABLE_OWNER_ID: return "admin";
                case TABLE_SPACE_ID: return null;
                default: throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
            }
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public boolean getBool(int col) {
            switch (col) {
                //  todo: has_index should probably return true for tables with either
                //  designated timestamp (the timestamp is an index) or with an indexed symbol column
                case HAS_INDEX_ID: return false;
                case HAS_RULES_ID: return false;
                case HAS_TRIGGERS_ID: return false;
                case ROW_SECURITY_ID: return false;
                default: throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
            }
        }

        @Override
        public int getStrLen(int col) {
            switch (col) {
                case SCHEMA_NAME_ID: return "public".length();
                case TABLE_NAME_ID: return cursor.tableNameSink.length();
                //  todo: what should the owner return? is 'admin' the right thing?
                case TABLE_OWNER_ID: return "admin".length();
                case TABLE_SPACE_ID: return -1;
                default: throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
            }
        }

        public boolean open(StringSink  tableName) {
            //todo: filter out the telemetry table when requested so
            return true;
        }
    }
}
