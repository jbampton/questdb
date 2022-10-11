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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.SymbolLookup;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.wal.TableWriterBackend;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8BytesToString;
import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16;

public class TableUpdateDetails implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableUpdateDetails.class);
    private static final SymbolLookup NOT_FOUND_LOOKUP = value -> SymbolTable.VALUE_NOT_FOUND;
    private final DefaultColumnTypes defaultColumnTypes;
    private final String tableNameUtf16;
    private final ThreadLocalDetails[] localDetailsArray;
    private final int timestampIndex;
    private final CairoEngine engine;
    private final MillisecondClock millisecondClock;
    private final long writerTickRowsCountMod;
    private final long defaultCommitInterval;
    private final long defaultMaxUncommittedRows;
    private int writerThreadId;
    // Number of rows processed since the last reshuffle, this is an estimate because it is incremented by
    // multiple threads without synchronisation
    private long eventsProcessedSinceReshuffle = 0;
    private TableWriterFrontend writerFrontend;
    private TableWriterBackend writerBackend;
    private boolean assignedToJob = false;
    private long lastMeasurementMillis = Long.MAX_VALUE;
    private long nextCommitTime;
    private int networkIOOwnerCount = 0;
    private volatile boolean writerInError;

    TableUpdateDetails(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            TableWriterFrontend writer,
            int writerThreadId,
            NetworkIOJob[] netIoJobs,
            DefaultColumnTypes defaultColumnTypes
    ) {
        this.writerThreadId = writerThreadId;
        this.engine = engine;
        this.defaultColumnTypes = defaultColumnTypes;
        final int n = netIoJobs.length;
        CairoConfiguration cairoConfiguration = engine.getConfiguration();
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.writerTickRowsCountMod = cairoConfiguration.getWriterTickRowsCountMod();
        this.defaultCommitInterval = configuration.getCommitIntervalDefault();
        this.defaultMaxUncommittedRows = cairoConfiguration.getMaxUncommittedRows();
        this.writerFrontend = writer;
        BaseRecordMetadata metadata = writer.getMetadata();
        this.timestampIndex = metadata.getTimestampIndex();
        this.tableNameUtf16 = Chars.toString(writer.getTableName());
        if (writer instanceof TableWriterBackend) {
            writerBackend = (TableWriterBackend) writer;
            writerBackend.updateCommitInterval(configuration.getCommitIntervalFraction(), configuration.getCommitIntervalDefault());
            this.nextCommitTime = millisecondClock.getTicks() + writerBackend.getCommitInterval();
        } else {
            writerBackend = null;
            this.nextCommitTime = millisecondClock.getTicks() + defaultCommitInterval;
        }
        this.localDetailsArray = new ThreadLocalDetails[n];
        for (int i = 0; i < n; i++) {
            this.localDetailsArray[i] = new ThreadLocalDetails(
                    configuration,
                    netIoJobs[i].getUnusedSymbolCaches(),
                    writer.getMetadata().getColumnCount(),
                    // WalWriters have their own local symbol tables, so we enable
                    // symbol lookup for non-WAL tables only to avoid key clashes.
                    (writerBackend != null)
            );
        }
    }

    public void addReference(int workerId) {
        networkIOOwnerCount++;
        LOG.info()
                .$("network IO thread using table [workerId=").$(workerId)
                .$(", tableName=").$(tableNameUtf16)
                .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                .$(']').$();
    }

    public boolean isWriterInError() {
        return writerInError;
    }

    public void setWriterInError() {
        writerInError = true;
    }

    @Override
    public void close() {
        synchronized (this) {
            closeNoLock();
        }
    }

    public void closeLocals() {
        for (int n = 0; n < localDetailsArray.length; n++) {
            LOG.info().$("closing table parsers [tableName=").$(tableNameUtf16).$(']').$();
            localDetailsArray[n] = Misc.free(localDetailsArray[n]);
        }
    }

    public void closeNoLock() {
        if (writerThreadId != Integer.MIN_VALUE) {
            LOG.info().$("closing table writer [tableName=").$(tableNameUtf16).$(']').$();
            closeLocals();
            if (null != writerFrontend) {
                try {
                    if (!writerInError) {
                        writerFrontend.commit();
                    }
                } catch (Throwable ex) {
                    LOG.error().$("cannot commit writer transaction, rolling back before releasing it [table=").$(tableNameUtf16).$(",ex=").$(ex).I$();
                } finally {
                    // returning to pool rolls back the transaction
                    writerFrontend = Misc.free(writerFrontend);
                    writerBackend = null;
                }
            }
            writerThreadId = Integer.MIN_VALUE;
        }
    }

    public long getEventsProcessedSinceReshuffle() {
        return eventsProcessedSinceReshuffle;
    }

    public long getLastMeasurementMillis() {
        return lastMeasurementMillis;
    }

    public int getNetworkIOOwnerCount() {
        return networkIOOwnerCount;
    }

    public String getTableNameUtf16() {
        return tableNameUtf16;
    }

    public int getWriterThreadId() {
        return writerThreadId;
    }

    public void incrementEventsProcessedSinceReshuffle() {
        ++eventsProcessedSinceReshuffle;
    }

    public boolean isAssignedToJob() {
        return assignedToJob;
    }

    public void setAssignedToJob(boolean assignedToJob) {
        this.assignedToJob = assignedToJob;
    }

    public void removeReference(int workerId) {
        networkIOOwnerCount--;
        localDetailsArray[workerId].clear();
        LOG.info()
                .$("network IO thread released table [workerId=").$(workerId)
                .$(", tableName=").$(tableNameUtf16)
                .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                .I$();
    }

    public void tick() {
        if (writerBackend != null) {
            writerBackend.tick();
        }
    }

    private long getCommitInterval() {
        if (writerBackend != null) {
            return writerBackend.getCommitInterval();
        }
        return defaultCommitInterval;
    }

    private long getMetaMaxUncommittedRows() {
        if (writerBackend != null) {
            return writerBackend.getMetaMaxUncommittedRows();
        }
        return defaultMaxUncommittedRows;
    }

    private void commit(boolean withLag) throws CommitFailedException {
        if (writerFrontend.getUncommittedRowCount() > 0) {
            try {
                LOG.debug().$("time-based commit " + (withLag ? "with lag " : "") + "[rows=").$(writerFrontend.getUncommittedRowCount()).$(", table=").$(tableNameUtf16).I$();
                if (withLag) {
                    writerFrontend.commitWithLag();
                } else {
                    writerFrontend.commit();
                }
            } catch (Throwable ex) {
                setWriterInError();
                LOG.error().$("could not commit [table=").$(tableNameUtf16).$(", e=").$(ex).I$();
                try {
                    writerFrontend.rollback();
                } catch (Throwable th) {
                    LOG.error().$("could not perform emergency rollback [table=").$(tableNameUtf16).$(", e=").$(th).I$();
                }
                throw CommitFailedException.instance(ex);
            }
        }
    }

    long commitIfIntervalElapsed(long wallClockMillis) throws CommitFailedException {
        if (wallClockMillis < nextCommitTime) {
            return nextCommitTime;
        }
        if (writerFrontend != null) {
            final long commitInterval = getCommitInterval();
            long start = millisecondClock.getTicks();
            commit(wallClockMillis - lastMeasurementMillis < commitInterval);
            // Do not commit row by row if the commit takes longer than commitInterval.
            // Exclude time to commit from the commit interval.
            nextCommitTime += commitInterval + millisecondClock.getTicks() - start;
        }
        return nextCommitTime;
    }

    void commitIfMaxUncommittedRowsCountReached() throws CommitFailedException {
        final long rowsSinceCommit = writerFrontend.getUncommittedRowCount();
        if (rowsSinceCommit < getMetaMaxUncommittedRows()) {
            if ((rowsSinceCommit & writerTickRowsCountMod) == 0) {
                // Tick without commit. Some tick commands may force writer to commit though.
                tick();
            }
            return;
        }
        LOG.debug().$("max-uncommitted-rows commit with lag [").$(tableNameUtf16).I$();
        nextCommitTime = millisecondClock.getTicks() + getCommitInterval();

        try {
            writerFrontend.commitWithLag();
        } catch (Throwable th) {
            LOG.error()
                    .$("could not commit line protocol measurement [tableName=").$(writerFrontend.getTableName())
                    .$(", message=").$(th.getMessage())
                    .$(th)
                    .I$();
            writerFrontend.rollback();
            throw CommitFailedException.instance(th);
        }

        // Tick after commit.
        tick();
    }

    ThreadLocalDetails getThreadLocalDetails(int workerId) {
        lastMeasurementMillis = millisecondClock.getTicks();
        return localDetailsArray[workerId];
    }

    int getTimestampIndex() {
        return timestampIndex;
    }

    TableWriterFrontend getWriter() {
        return writerFrontend;
    }

    void releaseWriter(boolean commit) {
        if (writerFrontend != null) {
            try {
                if (commit) {
                    LOG.debug().$("release commit [table=").$(tableNameUtf16).I$();
                    writerFrontend.commit();
                }
            } catch (Throwable ex) {
                LOG.error().$("writer commit fails, force closing it [table=").$(tableNameUtf16).$(",ex=").$(ex).I$();
            } finally {
                // writer or FS can be in a bad state
                // do not leave writer locked
                writerFrontend = Misc.free(writerFrontend);
                writerBackend = null;
            }
        }
    }

    public class ThreadLocalDetails implements Closeable {
        static final int COLUMN_NOT_FOUND = -1;
        static final int DUPLICATED_COLUMN = -2;
        private final Path path = new Path();
        // maps column names to their indexes
        // keys are mangled strings created from the utf-8 encoded byte representations of the column names
        private final CharSequenceIntHashMap columnIndexByNameUtf8 = new CharSequenceIntHashMap();
        // maps column names to their types
        // will be populated for dynamically added columns only
        private final CharSequenceIntHashMap columnTypeByNameUtf8 = new CharSequenceIntHashMap();
        private final ObjList<SymbolCache> symbolCacheByColumnIndex = new ObjList<>();
        private final ObjList<SymbolCache> unusedSymbolCaches;
        // indexed by colIdx + 1, first value accounts for spurious, new cols (index -1)
        private final IntList columnTypeMeta = new IntList();
        private final IntList columnTypes = new IntList();
        private final StringSink tempSink = new StringSink();
        // tracking of processed columns by their index, duplicates will be ignored
        private final BoolList processedCols = new BoolList();
        // tracking of processed columns by their name, duplicates will be ignored
        // columns end up in this set only if their index cannot be resolved, i.e. new columns
        private final LowerCaseCharSequenceHashSet addedColsUtf16 = new LowerCaseCharSequenceHashSet();
        private final LineTcpReceiverConfiguration configuration;
        private final boolean enableSymbolLookup;
        private int columnCount;
        private String colName;
        private TxReader txReader;
        private boolean clean = true;
        private String symbolNameTemp;

        ThreadLocalDetails(
                LineTcpReceiverConfiguration configuration,
                ObjList<SymbolCache> unusedSymbolCaches,
                int columnCount,
                boolean enableSymbolLookup
        ) {
            this.configuration = configuration;
            // symbol caches are passed from the outside
            // to provide global lifecycle management for when ThreadLocalDetails cease to exist
            // the cache continue to live
            this.unusedSymbolCaches = unusedSymbolCaches;
            this.columnCount = columnCount;
            this.enableSymbolLookup = enableSymbolLookup;
            columnTypeMeta.add(0);
        }

        @Override
        public void close() {
            Misc.freeObjList(symbolCacheByColumnIndex);
            Misc.free(path);
            txReader = Misc.free(txReader);
        }

        private SymbolCache addSymbolCache(int colWriterIndex) {
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                int symIndex = resolveSymbolIndexAndName(reader.getMetadata(), colWriterIndex);
                if (symbolNameTemp == null || symIndex < 0) {
                    throw CairoException.critical(0).put(reader.getMetadata().getColumnName(colWriterIndex)).put(" cannot find symbol column name by writer index ").put(colWriterIndex);
                }
                path.of(engine.getConfiguration().getRoot()).concat(tableNameUtf16);
                SymbolCache symCache;
                final int lastUnusedSymbolCacheIndex = unusedSymbolCaches.size() - 1;
                if (lastUnusedSymbolCacheIndex > -1) {
                    symCache = unusedSymbolCaches.get(lastUnusedSymbolCacheIndex);
                    unusedSymbolCaches.remove(lastUnusedSymbolCacheIndex);
                } else {
                    symCache = new SymbolCache(configuration);
                }
                FilesFacade filesFacade = engine.getConfiguration().getFilesFacade();

                if (this.clean) {
                    if (this.txReader == null) {
                        this.txReader = new TxReader(filesFacade);
                    }
                    int pathLen = path.length();
                    this.txReader.ofRO(path.concat(TXN_FILE_NAME).$(), reader.getPartitionedBy());
                    path.trimTo(pathLen);
                    this.clean = false;
                }

                long columnNameTxn = reader.getColumnVersionReader().getDefaultColumnNameTxn(colWriterIndex);
                assert symIndex <= colWriterIndex;
                symCache.of(engine.getConfiguration(), path, symbolNameTemp, symIndex, txReader, columnNameTxn);
                symbolCacheByColumnIndex.extendAndSet(colWriterIndex, symCache);
                return symCache;
            }
        }

        void clear() {
            columnIndexByNameUtf8.clear();
            columnTypeByNameUtf8.clear();
            for (int n = 0, sz = symbolCacheByColumnIndex.size(); n < sz; n++) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuick(n);
                if (null != symCache) {
                    symCache.close();
                    unusedSymbolCaches.add(symCache);
                }
            }
            symbolCacheByColumnIndex.clear();
            columnTypes.clear();
            columnTypeMeta.clear();
            columnTypeMeta.add(0);
            if (txReader != null) {
                txReader.clear();
            }
            this.clean = true;
        }

        String getColName() {
            assert colName != null;
            return colName;
        }

        // returns the column index for column name passed in colNameUtf8,
        // or COLUMN_NOT_FOUND if column index cannot be resolved (i.e. new column),
        // or DUPLICATED_COLUMN if the column has already been processed on the current event
        int getColumnIndex(DirectByteCharSequence colNameUtf8, boolean hasNonAsciiChars) {
            int colWriterIndex = columnIndexByNameUtf8.get(colNameUtf8);
            if (colWriterIndex < 0) {
                // lookup was unsuccessful we have to check whether the column can be passed by name to the writer
                final CharSequence colNameUtf16 = utf8ToUtf16(colNameUtf8, tempSink, hasNonAsciiChars);
                final int index = addedColsUtf16.keyIndex(colNameUtf16);
                if (index > -1) {
                    // column has not been sent to the writer by name on this line before
                    // we can try to resolve column index using table reader
                    colWriterIndex = getColumnWriterIndexFromReader(colNameUtf16);
                    if (colWriterIndex > -1) {
                        // keys of this map will be checked against DirectByteCharSequence when get() is called
                        // DirectByteCharSequence.equals() compares chars created from each byte, basically it
                        // assumes that each char is encoded on a single byte (ASCII)
                        // utf8BytesToString() is used here instead of a simple toString() call to make sure
                        // column names with non-ASCII chars are handled properly
                        columnIndexByNameUtf8.put(utf8BytesToString(colNameUtf8, tempSink), colWriterIndex);
                    } else {
                        // cannot not resolve column index even from the reader
                        // column will be passed to the writer by name
                        colName = colNameUtf16.toString();
                        addedColsUtf16.addAt(index, colName);
                        return COLUMN_NOT_FOUND;
                    }
                } else {
                    // column has been passed by name earlier on this event, duplicate should be skipped
                    return DUPLICATED_COLUMN;
                }
            }

            if (processedCols.extendAndReplace(colWriterIndex, true)) {
                // column has been passed by index earlier on this event, duplicate should be skipped
                return DUPLICATED_COLUMN;
            }
            return colWriterIndex;
        }

        private int getColumnWriterIndexFromReader(CharSequence colNameUtf16) {
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                TableReaderMetadata metadata = reader.getMetadata();
                int colIndex = metadata.getColumnIndexQuiet(colNameUtf16);
                if (colIndex < 0) {
                    return colIndex;
                }
                int writerColIndex = metadata.getWriterIndex(colIndex);
                updateColumnTypeCache(colIndex, writerColIndex, metadata);
                return writerColIndex;
            }
        }

        int getColumnType(int colIndex) {
            return columnTypes.getQuick(colIndex);
        }

        int getColumnType(String colName, byte entityType) {
            int colType = columnTypeByNameUtf8.get(colName);
            if (colType < 0) {
                colType = defaultColumnTypes.DEFAULT_COLUMN_TYPES[entityType];
                columnTypeByNameUtf8.put(colName, colType);
            }
            return colType;
        }

        private int resolveSymbolIndexAndName(TableReaderMetadata metadata, int colWriterIndex) {
            symbolNameTemp = null;
            int symIndex = -1;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getWriterIndex(i) == colWriterIndex) {
                    if (!ColumnType.isSymbol(metadata.getColumnType(i))) {
                        return -1;
                    }
                    symIndex++;
                    symbolNameTemp = metadata.getColumnName(i);
                    break;
                }
                if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                    symIndex++;
                }
            }
            return symIndex;
        }

        int getColumnTypeMeta(int colIndex) {
            return columnTypeMeta.getQuick(colIndex + 1); // first val accounts for new cols, index -1
        }

        SymbolLookup getSymbolLookup(int columnIndex) {
            if (enableSymbolLookup && columnIndex > -1) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuiet(columnIndex);
                if (symCache != null) {
                    return symCache;
                }
                return addSymbolCache(columnIndex);
            }
            return NOT_FOUND_LOOKUP;
        }

        void resetProcessedColumnsTracking() {
            processedCols.setAll(columnCount, false);
            addedColsUtf16.clear();
        }

        private void updateColumnTypeCache(int colIndex, int writerColIndex, TableReaderMetadata metadata) {
            columnCount = metadata.getColumnCount();
            final int colType = metadata.getColumnType(colIndex);
            final int geoHashBits = ColumnType.getGeoHashBits(colType);
            columnTypes.extendAndSet(writerColIndex, colType);
            columnTypeMeta.extendAndSet(writerColIndex + 1,
                    geoHashBits == 0 ? 0 : Numbers.encodeLowHighShorts((short) geoHashBits, ColumnType.tagOf(colType)));
        }
    }
}
