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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DatabaseSnapshotAgent implements Closeable {

    private final static Log LOG = LogFactory.getLog(DatabaseSnapshotAgent.class);

    private final CairoEngine engine;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final ReentrantLock lock = new ReentrantLock(); // protects below fields
    private final SnapshotStorage snapshotStorage;
    // List of readers kept around to lock partitions while a database snapshot is being made.
    private final ObjList<TableReader> snapshotReaders = new ObjList<>();

    public DatabaseSnapshotAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        snapshotStorage = new SnapshotStorage(configuration);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            Misc.free(snapshotStorage);
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public void clear() {
        lock.lock();
        try {
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    private void unsafeReleaseReaders() {
        Misc.freeObjList(snapshotReaders);
        snapshotReaders.clear();
    }

    public void prepareSnapshot(SqlExecutionContext executionContext) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.type == Os.WINDOWS) {
            throw SqlException.position(0).put("Snapshots are not supported on Windows");
        }

        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            if (snapshotReaders.size() > 0) {
                throw SqlException.position(0).put("Waiting for SNAPSHOT COMPLETE to be called");
            }

            snapshotStorage.createNewSnapshot();
            try (
                    TableListRecordCursorFactory factory = new TableListRecordCursorFactory(configuration.getFilesFacade(), configuration.getRoot())
            ) {
                final int tableNameIndex = factory.getMetadata().getColumnIndex(TableListRecordCursorFactory.TABLE_NAME_COLUMN);
                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                    final Record record = cursor.getRecord();
                    // Copy metadata files for all tables.
                    while (cursor.hasNext()) {
                        CharSequence tableName = record.getStr(tableNameIndex);
                        TableReader reader = engine.getReaderForStatement(executionContext, tableName, "snapshot");
                        snapshotReaders.add(reader);

                        snapshotStorage.writeTable(tableName);

                        // Copy _meta file.
                        MemoryCMARW mem = snapshotStorage.startNewEntry(TableUtils.META_FILE_NAME);
                        reader.getMetadata().dumpTo(mem);
                        snapshotStorage.finishEntry();

                        // Copy _txn file.
                        mem = snapshotStorage.startNewEntry(TableUtils.TXN_FILE_NAME);
                        reader.getTxFile().dumpTo(mem);
                        snapshotStorage.finishEntry();

                        // Copy _cv file.
                        mem = snapshotStorage.startNewEntry(TableUtils.COLUMN_VERSION_FILE_NAME);
                        reader.getColumnVersionReader().dumpTo(mem);
                        snapshotStorage.finishEntry();

                        LOG.info().$("snapshot copied [table=").$(tableName).$(']').$();
                    }
                    // Write instance id to the snapshot metadata file.
                    snapshotStorage.finishSnapshot();

                    // Flush dirty pages and filesystem metadata to disk
                    if (ff.sync() != 0) {
                        throw CairoException.instance(ff.errno()).put("Could not sync");
                    }

                    LOG.info().$("snapshot copying finished").$();
                } catch (Throwable e) {
                    unsafeReleaseReaders();
                    LOG.error()
                            .$("snapshot prepare error [e=").$(e)
                            .I$();
                    throw e;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void completeSnapshot() throws SqlException {
        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            if (snapshotReaders.size() == 0) {
                LOG.info().$("Snapshot has no tables, SNAPSHOT COMPLETE is ignored.").$();
                return;
            }

            // Delete snapshot directory.
            snapshotStorage.deleteExistingSnapshot();

            // Release locked readers if any.
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    public static void recoverSnapshot(CairoEngine engine) {
        try(SnapshotStorageReader snapshotStorageReader = new SnapshotStorageReader(engine.getConfiguration())) {
            recoverSnapshot(engine, snapshotStorageReader);
        }
    }

    public static void recoverSnapshot(CairoEngine engine, SnapshotReader snapshotStorage) {
        final CairoConfiguration configuration = engine.getConfiguration();
        if (!configuration.isSnapshotRecoveryEnabled()) {
            return;
        }

        final FilesFacade ff = configuration.getFilesFacade();

        // Check if the snapshot dir exists.
        if (!snapshotStorage.snapshotExists()) {
            return;
        }


        // Check if the snapshot metadata file exists.
        var snapshotInstanceId = snapshotStorage.readSnapshotMetadata();
        if (snapshotInstanceId == null) {
            return;
        }

        // Check if the snapshot instance id is different from what's in the snapshot.
        final CharSequence currentInstanceId = configuration.getSnapshotInstanceId();
        if (!Chars.nonEmpty(currentInstanceId) || !Chars.nonEmpty(snapshotInstanceId) || Chars.equals(currentInstanceId, snapshotInstanceId)) {
            return;
        }

        LOG.info()
                .$("starting snapshot recovery [currentId=`").$(currentInstanceId)
                .$("`, previousId=`").$(snapshotInstanceId)
                .$("`]").$();

        // OK, we need to recover from the snapshot.
        int recoveredMetaFiles = 0;
        int recoveredTxnFiles = 0;
        int recoveredCVFiles = 0;

        var snapshotCursor = snapshotStorage.getCursor();
        while (snapshotCursor.hasNext()) {
            if (snapshotCursor.copyMetadataFile()) {
                recoveredMetaFiles++;
            }
            if (snapshotCursor.copyTxnFile()) {
                recoveredTxnFiles++;
            }
            if (snapshotCursor.copyCvFile()) {
                recoveredCVFiles++;
            }
        }

        LOG.info()
                .$("snapshot recovery finished [metaFilesCount=").$(recoveredMetaFiles)
                .$(", txnFilesCount=").$(recoveredTxnFiles)
                .$(", cvFilesCount=").$(recoveredCVFiles)
                .$(']').$();

        // Delete snapshot directory to avoid recovery on next restart.
        snapshotStorage.finishRestore();
    }
}
