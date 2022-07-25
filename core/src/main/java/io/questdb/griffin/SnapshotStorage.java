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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class SnapshotStorage implements Closeable {
    private final Path path = new Path();
    private Path copyPath;

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MemoryCMARW mem;
    private int snapshotLen;
    private int rootLen;

    public SnapshotStorage(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        this.configuration = configuration;
        this.mem = Vm.getCMARWInstance();
    }

    @Override
    public void close() {
        copyPath = Misc.free(copyPath);
        Misc.free(path);
        Misc.free(mem);
    }

    public void createNewSnapshot() {
        path.of(configuration.getSnapshotRoot());
        this.snapshotLen = path.length();
        int snapshotLen = path.length();
        // Delete all contents of the snapshot dir.
        if (ff.exists(path.slash$())) {
            path.trimTo(snapshotLen).$();
            if (ff.rmdir(path) != 0) {
                throw CairoException.instance(ff.errno()).put("Could not remove snapshot dir [dir=").put(path).put(']');
            }
        }
        // Recreate the snapshot dir.
        path.trimTo(snapshotLen).slash$();
        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
        }
    }

    public void deleteExistingSnapshot() {
        path.of(configuration.getSnapshotRoot()).$();
        ff.rmdir(path); // it's fine to ignore errors here
    }

    public void finishEntry() {
        mem.close(false);
    }

    public void finishRestore() {
        path.trimTo(snapshotLen).$();
        if (ff.rmdir(path) != 0) {
            throw CairoException.instance(ff.errno())
                    .put("could not remove snapshot dir [dir=").put(path)
                    .put(", errno=").put(ff.errno())
                    .put(']');
        }
    }

    public void finishSnapshot() {
        path.trimTo(snapshotLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        mem.putStr(configuration.getSnapshotInstanceId());
        mem.close();
    }

    public boolean snapshotExists() {
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();
        final CharSequence root = configuration.getRoot();

        path.of(snapshotRoot);
        snapshotLen = path.length();

        if (copyPath == null) {
            copyPath = new Path();
        }
        copyPath.of(root);
        rootLen = copyPath.length();

        return ff.exists(path.slash$());
    }


    public MemoryCMARW readSnapshotMetadata() {
        path.trimTo(snapshotLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
        if (!ff.exists(path)) {
            return null;
        }

        // Check if the snapshot instance id is different from what's in the snapshot.
        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        return mem;
    }

    public void writeTable(CharSequence tableName) {
        path.trimTo(snapshotLen).concat(configuration.getDbDirectory()).concat(tableName).slash$();
        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
        }

        this.rootLen = path.length();
    }

    public MemoryCMARW startNewEntry(String fileName) {
        // Copy _meta file.
        path.trimTo(rootLen).concat(fileName).$();
        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        return mem;
    }
}
