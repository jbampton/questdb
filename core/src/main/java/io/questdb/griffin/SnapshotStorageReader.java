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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class SnapshotStorageReader implements SnapshotReader {
    private final static Log LOG = LogFactory.getLog(SnapshotStorageReader.class);
    private final Path path = new Path();
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MemoryCMARW mem;
    private Path copyPath;
    private int snapshotRootLen;
    private int rootLen;
    private final SnapshotCursor snapshotCursor = new SnapshotCursor();

    public SnapshotStorageReader(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        this.configuration = configuration;
        this.mem = Vm.getCMARWInstance();
    }

    @Override
    public void close() {
        copyPath = Misc.free(copyPath);
        Misc.free(path);
        Misc.free(mem);
        Misc.free(snapshotCursor);
    }

    @Override
    public void finishRestore() {
        path.trimTo(snapshotRootLen).$();
        if (ff.rmdir(path) != 0) {
            throw CairoException.instance(ff.errno())
                    .put("could not remove snapshot dir [dir=").put(path)
                    .put(", errno=").put(ff.errno())
                    .put(']');
        }
    }

    @Override
    public SnapshotCursor getCursor() {
        snapshotCursor.init();
        return snapshotCursor;
    }

    @Override
    public CharSequence readSnapshotMetadata() {
        path.trimTo(snapshotRootLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
        if (!ff.exists(path)) {
            return null;
        }

        // Check if the snapshot instance id is different from what's in the snapshot.
        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        return mem.getStr(0);
    }

    @Override
    public boolean snapshotExists() {
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();
        final CharSequence root = configuration.getRoot();

        path.of(snapshotRoot);
        snapshotRootLen = path.length();

        if (copyPath == null) {
            copyPath = new Path();
        }
        copyPath.of(root);
        rootLen = copyPath.length();

        return ff.exists(path.slash$());
    }

    public class SnapshotCursor implements SnapshotTableCursor {
        private long findHandle;
        private int plen;
        private int cplen;
        private boolean hasNext;

        @Override
        public void close() {
            if (findHandle > 0) {
                ff.findClose(findHandle);
                findHandle = 0;
            }
        }

        @Override
        public boolean copyCvFile() {
            path.trimTo(plen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            copyPath.trimTo(cplen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            if (ff.exists(path) && ff.exists(copyPath)) {
                if (ff.copy(path, copyPath) < 0) {
                    LOG.error()
                            .$("could not copy _cv file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(", errno=").$(ff.errno())
                            .$(']').$();
                    return false;
                } else {
                    LOG.info()
                            .$("recovered _cv file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(']').$();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean copyMetadataFile() {
            path.concat(TableUtils.META_FILE_NAME).$();
            copyPath.concat(TableUtils.META_FILE_NAME).$();

            if (ff.exists(path) && ff.exists(copyPath)) {
                if (ff.copy(path, copyPath) < 0) {
                    LOG.error()
                            .$("could not copy _meta file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(", errno=").$(ff.errno())
                            .$(']').$();
                } else {
                    LOG.info()
                            .$("recovered _meta file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(']').$();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean copyTxnFile() {
            path.trimTo(plen).concat(TableUtils.TXN_FILE_NAME).$();
            copyPath.trimTo(cplen).concat(TableUtils.TXN_FILE_NAME).$();
            if (ff.exists(path) && ff.exists(copyPath)) {
                if (ff.copy(path, copyPath) < 0) {
                    LOG.error()
                            .$("could not copy _txn file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(", errno=").$(ff.errno())
                            .$(']').$();
                } else {
                    LOG.info()
                            .$("recovered _txn file [src=").$(path)
                            .$(", dst=").$(copyPath)
                            .$(']').$();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            while (hasNext) {
                long pUtf8NameZ = ff.findName(findHandle);
                long type = ff.findType(findHandle);
                hasNext = ff.findNext(findHandle) > 0;

                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(snapshotRootLen).concat(configuration.getDbDirectory()).concat(pUtf8NameZ);
                    copyPath.trimTo(rootLen).concat(pUtf8NameZ);
                    plen = path.length();
                    cplen = copyPath.length();

                    return true;
                }
            }

            return false;
        }

        @Override
        public void init() {
            path.trimTo(snapshotRootLen).concat(configuration.getDbDirectory()).$();
            findHandle = ff.findFirst(path);
            hasNext = findHandle > 0;
        }
    }
}
