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

package org.questdb;

import io.questdb.client.Sender;
import io.questdb.std.Rnd;

public class LineTCPSenderMainDupCols {

    public static void main(String[] args) {
        try (Sender sender = Sender.builder()
                .address("alex-support-toy-43467d67.ilp.b04c.questdb.net:32243")
                .enableTls()
                .enableAuth("admin").authToken("spgdXkZUbvg12R6MYme8ikmZPlpByL-1qXNZKvdh_zg")
                .build()) {

            final long count = 30_000_000;
            Rnd rnd = new Rnd();

            String[] tags = new String[1000];
            for(int i = 0; i < tags.length; i++) {
                tags[i] = "tag" + rnd.nextString(30);
            }

            for (int i = 0; i < count; i++) {
                sender.table("inventors")
                        .symbol("tag", tags[i % tags.length])
                        .symbol("tag2", "tag2" + i % 10000)
                        .longColumn("id", i)
                        .timestampColumn("ts", System.nanoTime() / 1000)
                        .at(System.nanoTime());
            }
        }
    }
}
