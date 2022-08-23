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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class MinDateVecGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        pageFrameMaxRows = 10_000;
        assertQuery(
                "avg\n" +
                        "5261.376146789\n",
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                "alter table tab add column b date",
                "avg\n" +
                        "5261.376146789\n",
                false,
                true,
                true
        );

        assertQuery(
                "avg\tmin\n" +
                        "14.792007\t1970-01-01T00:00:16.772Z\n",
                "select round(avg(f),6) avg, min(b) min from tab",
                "insert into tab select rnd_int(2, 10, 2), rnd_long(16772, 88965, 4) from long_sequence(78057)",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery(
                "min\n" +
                        "\n",
                "select min(f) from tab",
                "create table tab as (select cast(null as date) f from long_sequence(33))",
                null,
                "insert into tab select 99999999999999999L from long_sequence(1)",
                "min\n" +
                        "3170843-11-07T09:46:39.999Z\n",
                false,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "min\n" +
                        "1969-12-31T23:59:59.956Z\n",
                "select min(f) from tab",
                "create table tab as (select cast(rnd_long(-55, 9009, 2) as date) f from long_sequence(131))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testMaxDateOrNullThenMaxLong() throws Exception {
        assertQuery(
                "min\n" +
                        "\n",
                "select min(f) from tab",
                "create table tab as (select cast(null as date) f from long_sequence(33))",
                null,
                "insert into tab select 9223372036854775807L from long_sequence(1)",
                "min\n" +
                        "292278994-08-17T07:12:55.807Z\n",
                false,
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxDateOrNullThenMaxLong() throws Exception {
        assertQuery(
                "i\tmin\n" +
                        "1\t\n",
                "select i, min(f) from tab",
                "create table tab as (select cast(1 as int) i, cast(null as date) f from long_sequence(33))",
                null,
                "insert into tab select 1, 9223372036854775807L from long_sequence(1)",
                "i\tmin\n" +
                        "1\t292278994-08-17T07:12:55.807Z\n",
                true,
                true,
                true
        );
    }
}