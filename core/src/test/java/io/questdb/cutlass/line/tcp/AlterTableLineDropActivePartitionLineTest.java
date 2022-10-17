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

import io.questdb.AbstractBootstrapTest;
import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AlterTableLineDropActivePartitionLineTest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(AlterTableLineDropActivePartitionLineTest.class);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testServerMainPgWire() throws Exception {

        String tableName = "PaulinaRubioConcertTickets";

        try (final ServerMain serverMain = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
            serverMain.start();

            // create table over PGWire
            try (
                    Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    PreparedStatement stmt = connection.prepareStatement(
                            "CREATE TABLE " + tableName + "( " +
                                    "favourite_colour symbol index capacity 16, " +
                                    "country symbol index capacity 16, " +
                                    "orderId long, " +
                                    "quantity int, " +
                                    "ppu double, " +
                                    "addressId string, " +
                                    "timestamp timestamp" +
                                    ") timestamp(timestamp) partition by day " +
                                    "with maxUncommittedRows=20, commitLag=200000us" // 200 millis
                    )
            ) {
                stmt.execute();
            }

            // setup a thread that will send ILP/TCP for today
            final SOCountDownLatch ilpAgentHalted = new SOCountDownLatch(1);
            final AtomicBoolean keepSending = new AtomicBoolean(true);
            final AtomicLong orderId = new AtomicLong(0L);
            final String[] country = {
                    "Ukraine",
                    "Poland",
                    "Lithuania",
                    "USA",
                    "Germany",
                    "Czechia",
                    "England",
                    "Spain",
                    "Singapore",
                    "Taiwan",
                    "Romania",
            };
            final String[] colour = {
                    "Yellow",
                    "Blue",
                    "Green",
                    "Red",
                    "Gray",
                    "Orange",
                    "Black",
                    "White",
                    "Pink",
                    "Brown",
                    "Purple",
            };

            final Thread ilpAgent = new Thread(() -> {
                final Rnd rnd = new Rnd();
                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, 4 * 1024)) {
                    while (keepSending.get()) {
                        sender.metric(tableName)
                                .tag("favourite_colour", colour[rnd.nextPositiveInt() % colour.length])
                                .tag("country", country[rnd.nextPositiveInt() % country.length])
                                .field("orderId", orderId.getAndIncrement())
                                .field("quantity", rnd.nextPositiveInt())
                                .field("ppu", rnd.nextFloat())
                                .field("addressId", rnd.nextString(50))
                                .$();
                        sender.flush();
                    }
                } finally {
                    ilpAgentHalted.countDown();
                }
            });
            ilpAgent.start();

            // give the agent some time
            while (orderId.get() < 1_000_000) {
                Os.pause();
            }

            long beforeDropSize;
            try (TableReader reader = serverMain.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                beforeDropSize = reader.size();
                Assert.assertTrue(beforeDropSize > 0L);
            }

            try (
                    Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    PreparedStatement stmt = connection.prepareStatement("ALTER TABLE " + tableName + " DROP PARTITION WHERE timestamp > 0")
            ) {
                stmt.execute();
            }

            Os.sleep(100L); // allow a few millis more to the agent
            keepSending.set(false);
            ilpAgentHalted.await();

            try (TableReader reader = serverMain.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                Assert.assertTrue(beforeDropSize > reader.size());
            }

            try (TableWriter ignore = serverMain.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "drop-active-partition")) {
                Assert.fail("should fail");
            } catch (EntryUnavailableException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "table busy [reason=tcpIlp]");
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        PreparedStatement stmt = connection.prepareStatement("ALTER TABLE " + tableName + " DROP PARTITION WHERE timestamp > 0")
                ) {
                    stmt.execute();
                }
            }
            try (TableReader reader = serverMain.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                Assert.assertEquals(0, reader.size());
            }
        }
    }
}
