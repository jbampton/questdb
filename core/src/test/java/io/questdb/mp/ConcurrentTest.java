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

package io.questdb.mp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentTest {
    private final static Log LOG = LogFactory.getLog(ConcurrentTest.class);

    @Test
    public void testConcurrentFanOutAnd() {
        int cycle = 1024;
        SPSequence pubSeq = new SPSequence(cycle);
        FanOut fout = new FanOut();
        pubSeq.then(fout).then(pubSeq);

        int threads = 2;
        CyclicBarrier start = new CyclicBarrier(threads);
        SOCountDownLatch latch = new SOCountDownLatch(threads);
        int iterations = 30;

        AtomicInteger doneCount = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < iterations; j++) {
                        SCSequence consumer = new SCSequence();
                        FanOut fout2 = fout.and(consumer);
                        fout2.remove(consumer);
                    }
                    doneCount.addAndGet(iterations);
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        Assert.assertEquals(threads * iterations, doneCount.get());
    }

    @Test
    public void testDoneMemBarrier() throws InterruptedException {
        int cycle = 128;
        RingQueue<LongLongMsg> pingQueue = new RingQueue<>(LongLongMsg::new, cycle);
        MCSequence sub = new MCSequence(cycle);
        MPSequence pub = new MPSequence(cycle);

        pub.then(sub).then(pub);
        final int total = 10_000;
        int subThreads = 4;
        int pubThreads = 1;

        CyclicBarrier latch = new CyclicBarrier(subThreads + pubThreads);
        ObjList<Thread> pubThreadList = new ObjList<>();
        AtomicInteger busyCount = new AtomicInteger();
        AtomicLong anomalies = new AtomicLong();
        AtomicLong dummy = new AtomicLong();
        AtomicInteger done = new AtomicInteger();

        for (int th = 0; th < pubThreads; th++) {

            Thread pubTh = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < total; i++) {
                    long seq;
                    while (true) {
                        seq = pub.next();
                        if (seq > -1) {
                            LongLongMsg msg = pingQueue.get(seq);

//                            msg.f33 = i + 32;
//                            msg.f32 = i + 31;
//                            msg.f31 = i + 30;
//                            msg.f30 = i + 29;
//                            msg.f29 = i + 28;
//                            msg.f28 = i + 27;
//                            msg.f27 = i + 26;
//                            msg.f26 = i + 25;
//                            msg.f25 = i + 24;
//                            msg.f24 = i + 23;
//                            msg.f23 = i + 22;
//                            msg.f22 = i + 21;
//                            msg.f21 = i + 20;
//                            msg.f20 = i + 19;
//                            msg.f19 = i + 18;
//                            msg.f18 = i + 17;
//                            msg.f17 = i + 16;
//                            msg.f16 = i + 15;
//                            msg.f15 = i + 14;
//                            msg.f14 = i + 13;
                            if (i % 2 == 0) {
//                                msg.f13 = i + 12;
//                                msg.f12 = i + 11;
//                                msg.f11 = i + 10;
                                msg.f1 = i;
                                msg.f2 = i + 1;
//                                msg.f3 = i + 2;
//                                msg.f4 = i + 3;
//                                msg.f5 = i + 4;
//                                msg.f6 = i + 5;
//                                msg.f7 = i + 6;
//                                msg.f8 = i + 7;
//                                msg.f9 = i + 8;
                                msg.f10 = i + 9;
                            } else {
                                msg.f8 = i + 7;
                                msg.f9 = i + 8;
                                msg.f10 = i + 9;
//                                msg.f13 = i + 12;
//                                msg.f12 = i + 11;
//                                msg.f11 = i + 10;

                                msg.f1 = i;
                                msg.f2 = i + 1;
//                                msg.f3 = i + 2;
//                                msg.f4 = i + 3;
//                                msg.f5 = i + 4;
//                                msg.f6 = i + 5;
//                                msg.f7 = i + 6;
                            }

                            pub.done(seq);

                            break;
                        } else if (seq == -1) {
                            busyCount.incrementAndGet();
                            seq = sub.next();
                            if (seq > -1) {
                                processMsg(pingQueue, sub, subThreads, anomalies, dummy, seq, i);
                                break;
                            }
                        }
                    }
                }
                done.incrementAndGet();
            });
            pubTh.start();
            pubThreadList.add(pubTh);
        }

        ObjList<Thread> threads = new ObjList<>();

        for (int th = 0; th < subThreads; th++) {
            Thread subTh = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                long seq;
                int i = 0;
                while (done.get() < pubThreads) {
                    seq = sub.next();
                    if (seq > -1) {
                        processMsg(pingQueue, sub, subThreads, anomalies, dummy, seq, i++);
                        break;
                    }
                }
            });
            subTh.start();
            threads.add(subTh);
        }


        for (int i = 0; i < pubThreadList.size(); i++) {
            pubThreadList.getQuick(i).join();
        }

        for (int i = 0; i < threads.size(); i++) {
            Thread subTh = threads.get(i);
            subTh.join();
        }

        Assume.assumeTrue(busyCount.get() > total / 1000);
        Assert.assertEquals("Anomalies detected", 0, anomalies.get());
        Assert.assertTrue("dummy", dummy.get() > 0);
    }

    @Test
    public void testFanOutChain() {
        LOG.info().$("testFanOutChain").$();
        int cycle = 1024;
        Sequence a = new SPSequence(cycle);
        Sequence b = new SCSequence();
        Sequence c = new SCSequence();
        Sequence d = new SCSequence();
        Sequence e = new SCSequence();

        a.then(
                FanOut
                        .to(
                                FanOut.to(d).and(e)
                        )
                        .and(
                                b.then(c)
                        )
        ).then(a);
    }

    @Test
    public void testFanOutDoesNotProcessQueueFromStart() {
        // Main thread is going to publish K events
        // and then give green light to the relay thread,
        // which should not be processing first K-1 events
        //
        // There is also a generic consumer thread that's processing
        // all messages

        final RingQueue<LongMsg> queue = new RingQueue<>(ConcurrentTest.LongMsg::new, 64);
        final SPSequence pubSeq = new SPSequence(queue.getCycle());
        final FanOut subFo = new FanOut();
        pubSeq.then(subFo).then(pubSeq);

        final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
        int N = 20;
        int K = 12;

        // The relay sequence is created by publisher to
        // make test deterministic. To pass this sequence to
        // the relay thread we use the atomic reference and countdown latch
        final AtomicReference<SCSequence> relaySubSeq = new AtomicReference<>();
        final SOCountDownLatch relayLatch = new SOCountDownLatch(1);

        // this barrier is to make sure threads are ready before publisher starts
        final SOCountDownLatch threadsRunBarrier = new SOCountDownLatch(2);

        new Thread(() -> {
            final SCSequence subSeq = new SCSequence();
            subFo.and(subSeq);

            // thread is ready to consume
            threadsRunBarrier.countDown();

            int count = N;
            while (count > 0) {
                long cursor = subSeq.nextBully();
                subSeq.done(cursor);
                count--;
            }
            subFo.remove(subSeq);
            haltLatch.countDown();
        }).start();

        // indicator showing relay thread did not process messages before K
        final AtomicBoolean relayThreadSuccess = new AtomicBoolean();

        new Thread(() -> {

            // thread is ready to wait :)
            threadsRunBarrier.countDown();

            relayLatch.await();

            final SCSequence subSeq = relaySubSeq.get();
            int count = N - K;
            boolean success = true;
            while (count > 0) {
                long cursor = subSeq.nextBully();
                if (success) {
                    success = queue.get(cursor).correlationId >= K;
                }
                subSeq.done(cursor);
                count--;
            }
            relayThreadSuccess.set(success);
            subFo.remove(subSeq);
            haltLatch.countDown();
        }).start();

        // wait for threads to get ready
        threadsRunBarrier.await();

        // publish
        for (int i = 0; i < N; i++) {
            if (i == K) {
                final SCSequence sub = new SCSequence();
                subFo.and(sub);
                relaySubSeq.set(sub);
                relayLatch.countDown();
            }

            long cursor = pubSeq.nextBully();
            queue.get(cursor).correlationId = i;
            pubSeq.done(cursor);
        }

        haltLatch.await();
        Assert.assertTrue(relayThreadSuccess.get());
    }

    @Test
    public void testFanOutPingPong() {
        final int threads = 2;
        final int iterations = 30;

        // Requests inbox
        final RingQueue<LongMsg> pingQueue = new RingQueue<>(ConcurrentTest.LongMsg::new, threads);
        final MPSequence pingPubSeq = new MPSequence(threads);
        final FanOut pingSubFo = new FanOut();
        pingPubSeq.then(pingSubFo).then(pingPubSeq);

        // Request inbox hook for processor
        final SCSequence pingSubSeq = new SCSequence();
        pingSubFo.and(pingSubSeq);

        // Response outbox
        final RingQueue<LongMsg> pongQueue = new RingQueue<>(ConcurrentTest.LongMsg::new, threads);
        final SPSequence pongPubSeq = new SPSequence(threads);
        final FanOut pongSubFo = new FanOut();
        pongPubSeq.then(pongSubFo).then(pongPubSeq);

        CyclicBarrier start = new CyclicBarrier(threads);
        SOCountDownLatch latch = new SOCountDownLatch(threads + 1);
        AtomicLong doneCount = new AtomicLong();
        AtomicLong idGen = new AtomicLong();

        // Processor
        new Thread(() -> {
            try {
                int i = 0;
                while (i < threads * iterations) {
                    long seq = pingSubSeq.next();
                    if (seq > -1) {
                        // Get next request
                        LongMsg msg = pingQueue.get(seq);
                        long requestId = msg.correlationId;
                        pingSubSeq.done(seq);

                        // Uncomment this and the following lines when in need for debugging
                        // LOG.info().$("ping received ").$(requestId).$();

                        long resp;
                        while ((resp = pongPubSeq.next()) < 0) {
                            Os.pause();
                        }
                        pongQueue.get(resp).correlationId = requestId;
                        pongPubSeq.done(resp);

                        // LOG.info().$("pong sent ").$(requestId).$();
                        i++;
                    } else {
                        Os.pause();
                    }
                }
                doneCount.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }).start();

        // Request threads
        for (int th = 0; th < threads; th++) {
            new Thread(() -> {
                try {
                    SCSequence pongSubSeq = new SCSequence();
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        // Put local response sequence into response FanOut
                        pongSubFo.and(pongSubSeq);

                        // Send next request
                        long requestId = idGen.incrementAndGet();
                        long reqSeq;
                        while ((reqSeq = pingPubSeq.next()) < 0) {
                            Os.pause();
                        }
                        pingQueue.get(reqSeq).correlationId = requestId;
                        pingPubSeq.done(reqSeq);
                        // LOG.info().$(threadId).$(", ping sent ").$(requestId).$();

                        // Wait for response
                        long responseId, respCursor;
                        do {
                            while ((respCursor = pongSubSeq.next()) < 0) {
                                Os.pause();
                            }
                            responseId = pongQueue.get(respCursor).correlationId;
                            pongSubSeq.done(respCursor);
                        } while (responseId != requestId);

                        // LOG.info().$(threadId).$(", pong received ").$(requestId).$();

                        // Remove local response sequence from response FanOut
                        pongSubFo.remove(pongSubSeq);
                        pongSubSeq.clear();
                    }
                    doneCount.incrementAndGet();
                } catch (BrokenBarrierException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        Assert.assertEquals(threads + 1, doneCount.get());
    }

    @Test
    public void testFanOutPingPongStableSequences() {
        final int threads = 2;
        final int iterations = 30;
        final int cycle = Numbers.ceilPow2(threads * iterations);

        // Requests inbox
        RingQueue<LongMsg> pingQueue = new RingQueue<>(ConcurrentTest.LongMsg::new, cycle);
        MPSequence pingPubSeq = new MPSequence(cycle);
        FanOut pingSubFo = new FanOut();
        pingPubSeq.then(pingSubFo).then(pingPubSeq);

        // Request inbox hook for processor
        SCSequence pingSubSeq = new SCSequence();
        pingSubFo.and(pingSubSeq);

        // Response outbox
        RingQueue<LongMsg> pongQueue = new RingQueue<>(ConcurrentTest.LongMsg::new, cycle);
        SPSequence pongPubSeq = new SPSequence(threads);
        FanOut pongSubFo = new FanOut();
        pongPubSeq.then(pongSubFo).then(pongPubSeq);

        CyclicBarrier start = new CyclicBarrier(threads);
        SOCountDownLatch latch = new SOCountDownLatch(threads + 1);
        AtomicLong doneCount = new AtomicLong();
        AtomicLong idGen = new AtomicLong();

        // Processor
        new Thread(() -> {
            try {
                LongList pingPong = new LongList();
                int i = 0;
                while (i < threads * iterations) {
                    long pingCursor = pingSubSeq.next();
                    if (pingCursor > -1) {
                        // Get next request
                        LongMsg msg = pingQueue.get(pingCursor);
                        long requestId = msg.correlationId;
                        pingSubSeq.done(pingCursor);

                        pingPong.add(pingCursor);

                        // Uncomment this and the following lines when in need for debugging
                        // System.out.println("* ping " + requestId);

                        long pongCursor;
                        while ((pongCursor = pongPubSeq.next()) < 0) {
                            Os.pause();
                        }
                        pongQueue.get(pongCursor).correlationId = requestId;
                        pongPubSeq.done(pongCursor);
                        pingPong.add(pongCursor);

                        // System.out.println("* pong " + requestId);
                        i++;
                    } else {
                        Os.pause();
                    }
                }
                doneCount.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }).start();

        final SCSequence[] sequences = new SCSequence[threads];
        for (int i = 0; i < threads; i++) {
            SCSequence pongSubSeq = new SCSequence();
            pongSubFo.and(pongSubSeq);
            sequences[i] = pongSubSeq;
        }

        // Request threads
        for (int th = 0; th < threads; th++) {
            final int threadId = th;
            new Thread(() -> {
                final LongList pingPong = new LongList();
                try {
                    start.await();

                    final SCSequence pongSubSeq = sequences[threadId];

                    for (int i = 0; i < iterations; i++) {
                        // Put local response sequence into response FanOut
                        // System.out.println("thread:" + threadId + ", added at " + pingPubSeq.value);

                        // Send next request
                        long requestId = idGen.incrementAndGet();
                        long pingCursor;
                        while ((pingCursor = pingPubSeq.next()) < 0) {
                            Os.pause();
                        }
                        pingQueue.get(pingCursor).correlationId = requestId;
                        pingPubSeq.done(pingCursor);
                        pingPong.add(pingCursor);

                        // System.out.println("thread:" + threadId + ", ask: " + requestId);

                        // Wait for response
                        long responseId, pongCursor;
                        do {
                            while ((pongCursor = pongSubSeq.next()) < 0) {
                                Os.pause();
                            }
                            pingPong.add(pongCursor);
                            responseId = pongQueue.get(pongCursor).correlationId;
                            pongSubSeq.done(pongCursor);
                            pingPong.add(pongCursor);
                            // System.out.println("thread:" + threadId + ", ping: " + responseId + ", expected: " + requestId);
                        } while (responseId != requestId);

                        // System.out.println("thread " + threadId + ", pong " + requestId);
                        // Remove local response sequence from response FanOut
                    }
                    pongSubFo.remove(pongSubSeq);
                    doneCount.incrementAndGet();
                } catch (BrokenBarrierException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        Assert.assertEquals(threads + 1, doneCount.get());
    }

    @Test
    public void testManyHybrid() throws Exception {
        LOG.info().$("testManyHybrid").$();
        int threadCount = 4;
        int cycle = 4;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        MPSequence pubSeq = new MPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        CountDownLatch latch = new CountDownLatch(threadCount);

        BusyProducerConsumer[] threads = new BusyProducerConsumer[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new BusyProducerConsumer(size, pubSeq, subSeq, queue, barrier, latch);
            threads[i].start();
        }

        barrier.await();
        latch.await();

        int totalProduced = 0;
        int totalConsumed = 0;
        for (int i = 0; i < threadCount; i++) {
            totalProduced += threads[i].produced;
            totalConsumed += threads[i].consumed;
        }
        Assert.assertEquals(threadCount * size, totalProduced);
        Assert.assertEquals(threadCount * size, totalConsumed);
    }

    @Test
    public void testManyToManyBusy() throws Exception {
        LOG.info().$("testManyToManyBusy").$();
        int cycle = 128;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        MPSequence pubSeq = new MPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(5);
        CountDownLatch latch = new CountDownLatch(4);

        BusyProducer[] producers = new BusyProducer[2];
        producers[0] = new BusyProducer(size / 2, pubSeq, queue, barrier, latch);
        producers[1] = new BusyProducer(size / 2, pubSeq, queue, barrier, latch);

        producers[0].start();
        producers[1].start();

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (int i = 0; i < buf.length / 2; i++) {
            Assert.assertEquals(i, buf[2 * i]);
            Assert.assertEquals(i, buf[2 * i + 1]);
        }
    }

    /**
     * <pre>
     *                    +--------+
     *               +--->| worker |
     *     +-----+   |    +--------+
     *     | pub |-->|
     *     +-----+   |    +--------+
     *               +--->| worker |
     *                    +--------+
     * </pre>
     */
    @Test
    public void testOneToManyBusy() throws Exception {
        LOG.info().$("testOneToManyBusy").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle);
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(2);

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);

            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToManyWaiting() throws Exception {
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        MCSequence subSeq = new MCSequence(cycle, new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        SOCountDownLatch latch = new SOCountDownLatch(2);

        WaitingConsumer[] consumers = new WaitingConsumer[2];
        consumers[0] = new WaitingConsumer(size, subSeq, queue, barrier, latch);
        consumers[1] = new WaitingConsumer(size, subSeq, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        do {
            long cursor = pubSeq.nextBully();
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
        } while (i != size);

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = new int[size];
        System.arraycopy(consumers[0].buf, 0, buf, 0, consumers[0].finalIndex);
        System.arraycopy(consumers[1].buf, 0, buf, consumers[0].finalIndex, consumers[1].finalIndex);
        Arrays.sort(buf);
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToOneBatched() throws BrokenBarrierException, InterruptedException {
        final int cycle = 1024;
        final int size = 1024 * cycle;
        final RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        final SPSequence pubSeq = new SPSequence(cycle);
        final SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(2);

        new Thread(() -> {
            try {
                barrier.await();
                Rnd rnd = new Rnd();
                for (int i = 0; i < size; ) {
                    long cursor = pubSeq.next();
                    if (cursor > -1) {
                        long available = pubSeq.available();
                        while (cursor < available && i < size) {
                            Event event = queue.get(cursor++);
                            event.value = rnd.nextInt();
                            i++;
                        }
                        pubSeq.done(cursor - 1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        barrier.await();
        int consumed = 0;
        final Rnd rnd2 = new Rnd();
        while (consumed < size) {
            long cursor = subSeq.next();
            if (cursor > -1) {
                long available = subSeq.available();
                while (cursor < available) {
                    Assert.assertEquals(rnd2.nextInt(), queue.get(cursor++).value);
                    consumed++;
                }
                subSeq.done(available - 1);
            }
        }
    }

    @Test
    public void testOneToOneBusy() throws Exception {
        LOG.info().$("testOneToOneBusy").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        Sequence pubSeq = new SPSequence(cycle);
        Sequence subSeq = new SCSequence(new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(1);

        BusyConsumer consumer = new BusyConsumer(size, subSeq, queue, barrier, latch);
        consumer.start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = consumer.buf;
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToOneWaiting() throws Exception {
        LOG.info().$("testOneToOneWaiting").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        Sequence pubSeq = new SPSequence(cycle);
        Sequence subSeq = new SCSequence(new YieldingWaitStrategy());
        pubSeq.then(subSeq).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(2);
        SOCountDownLatch latch = new SOCountDownLatch(1);

        WaitingConsumer consumer = new WaitingConsumer(size, subSeq, queue, barrier, latch);
        consumer.start();

        barrier.await();
        int i = 0;
        do {
            long cursor = pubSeq.nextBully();
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);
        } while (i != size);

        publishEOE(queue, pubSeq);

        latch.await();

        int[] buf = consumer.buf;
        for (i = 0; i < buf.length; i++) {
            Assert.assertEquals(i, buf[i]);
        }
    }

    @Test
    public void testOneToParallelMany() throws Exception {
        LOG.info().$("testOneToParallelMany").$();
        int cycle = 1024;
        int size = 1024 * cycle;
        RingQueue<Event> queue = new RingQueue<>(Event.FACTORY, cycle);
        SPSequence pubSeq = new SPSequence(cycle);
        Sequence sub1 = new SCSequence();
        Sequence sub2 = new SCSequence();
        pubSeq.then(FanOut.to(sub1).and(sub2)).then(pubSeq);

        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(2);

        BusyConsumer[] consumers = new BusyConsumer[2];
        consumers[0] = new BusyConsumer(size, sub1, queue, barrier, latch);
        consumers[1] = new BusyConsumer(size, sub2, queue, barrier, latch);

        consumers[0].start();
        consumers[1].start();

        barrier.await();
        int i = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                Os.pause();
                continue;
            }
            queue.get(cursor).value = i++;
            pubSeq.done(cursor);

            if (i == size) {
                break;
            }
        }

        publishEOE(queue, pubSeq);
        publishEOE(queue, pubSeq);

        latch.await();

        for (int k = 0; k < 2; k++) {
            for (i = 0; i < consumers[k].buf.length; i++) {
                Assert.assertEquals(i, consumers[k].buf[i]);
            }
        }
    }

    static void publishEOE(RingQueue<Event> queue, Sequence sequence) {
        long cursor = sequence.nextBully();
        queue.get(cursor).value = Integer.MIN_VALUE;
        sequence.done(cursor);
    }

    private void processMsg(RingQueue<LongLongMsg> pingQueue, MCSequence sub, int subThreads, AtomicLong anomalies, AtomicLong dummy, long seq, int i) {
        LongLongMsg msg = pingQueue.get(seq);

//        if (
//                msg.f2 != msg.f1 + 1 ||
////                                            msg.f3 != msg.f1 + 2 ||
////                                            msg.f4 != msg.f1 + 3 ||
////                                            msg.f5 != msg.f1 + 4 ||
////                                            msg.f6 != msg.f1 + 5 ||
////                                            msg.f7 != msg.f1 + 6 ||
////                                            msg.f8 != msg.f1 + 7 ||
////                                            msg.f9 != msg.f1 + 8 ||
//                        msg.f10 != msg.f1 + 9 //||
////                                            msg.f11 != msg.f1 + 10 ||
////                                            msg.f12 != msg.f1 + 11 ||
////                                            msg.f13 != msg.f1 + 12 ||
////                                            msg.f14 != msg.f1 + 13 ||
////                                            msg.f15 != msg.f1 + 14 ||
////                                            msg.f16 != msg.f1 + 15 ||
////                                            msg.f17 != msg.f1 + 16 ||
////                                            msg.f18 != msg.f1 + 17 ||
////                                            msg.f19 != msg.f1 + 18 ||
////                                            msg.f20 != msg.f1 + 19 ||
////                                            msg.f21 != msg.f1 + 20 ||
////                                            msg.f22 != msg.f1 + 21 ||
////                                            msg.f23 != msg.f1 + 22 ||
////                                            msg.f24 != msg.f1 + 23 ||
////                                            msg.f25 != msg.f1 + 24 ||
////                                            msg.f26 != msg.f1 + 25 ||
////                                            msg.f27 != msg.f1 + 26 ||
////                                            msg.f28 != msg.f1 + 27 ||
////                                            msg.f29 != msg.f1 + 28 ||
////                                            msg.f30 != msg.f1 + 29 ||
////                                            msg.f31 != msg.f1 + 30 ||
////                                            msg.f32 != msg.f1 + 31 ||
////                                            msg.f33 != msg.f1 + 32
//        ) {
//            anomalies.incrementAndGet();
//        }

//        if (i % subThreads == 0) {
//            sub.done(seq);
//            return;
//        }

        long f1 = msg.f1;
        long f2 = msg.f2;
        long f3 = msg.f3;
        long f4 = msg.f4;
        long f5 = msg.f5;
        long f6 = msg.f6;
        long f7 = msg.f7;
        long f8 = msg.f8;
        long f9 = msg.f9;
        long f10 = msg.f10;
//                        long f11 = msg.f11;
//                        long f12 = msg.f12;
//                        long f13 = msg.f13;
//                        long f14 = msg.f14;
//                        long f15 = msg.f15;
//                        long f16 = msg.f16;
//                        long f17 = msg.f17;
//                        long f18 = msg.f18;
//                        long f19 = msg.f19;
//                        long f20 = msg.f20;
//                        long f21 = msg.f21;
//                        long f22 = msg.f22;
//                        long f23 = msg.f23;
//                        long f24 = msg.f24;
//                        long f25 = msg.f25;
//                        long f26 = msg.f26;
//                        long f27 = msg.f27;
//                        long f28 = msg.f28;
//                        long f29 = msg.f29;
//                        long f30 = msg.f30;
//                        long f31 = msg.f31;
//                        long f32 = msg.f32;
//                        long f33 = msg.f33;

        sub.done(seq);

        if (
                f2 != f1 + 1 ||
//                        f3 != f1 + 2 ||
//                        f4 != f1 + 3 ||
//                        f5 != f1 + 4 ||
//                        f6 != f1 + 5 ||
//                        f7 != f1 + 6 ||
//                        f8 != f1 + 7 ||
//                        f9 != f1 + 8 ||
                        f10 != f1 + 9
            //||
//                                f11 != f1 + 10 ||
////                                f12 != f1 + 11 ||
//                                f13 != f1 + 12 ||
////                                f14 != f1 + 13 ||
//                                f15 != f1 + 14 ||
////                                f16 != f1 + 15 ||
//                                f17 != f1 + 16 ||
////                                f18 != f1 + 17 ||
//                                f19 != f1 + 18 ||
////                                f20 != f1 + 19 ||
//                                f21 != f1 + 20 ||
////                                f22 != f1 + 21 ||
//                                f23 != f1 + 22 ||
////                                f24 != f1 + 23 ||
//                                f25 != f1 + 24 ||
////                                f26 != f1 + 25 ||
//                                f27 != f1 + 26 ||
////                                f28 != f1 + 27 ||
//                                f29 != f1 + 28 ||
////                                f30 != f1 + 29 ||
//                                f31 != f1 + 30 ||
////                                f32 != f1 + 31 ||
//                                f33 != f1 + 32
        ) {
            anomalies.incrementAndGet();
        }

        dummy.set((long) (Math.exp(f1) + Math.log(f2) + Math.sin(f10)));
        return;
    }

    private static class LongMsg {
        public long correlationId;
    }

    private static class LongLongMsg {
        public long f1;
        public long f2;
        public long f3;
        public long f4;
        public long f5;
        public long f6;
        public long f7;
        public long f8;
        public long f9;
        public long f10;
//        public long f11;
//        public long f12;
//        public long f13;
//        public long f14;
//        public long f15;
//        public long f16;
//        public long f17;
//        public long f18;
//        public long f19;
//        public long f20;
//        public long f21;
//        public long f22;
//        public long f23;
//        public long f24;
//        public long f25;
//        public long f26;
//        public long f27;
//        public long f28;
//        public long f29;
//        public long f30;
//        public long f31;
//        public long f32;
//        public long f33;
    }

    private static class BusyProducerConsumer extends Thread {
        private final Sequence producerSequence;
        private final Sequence consumerSequence;
        private final int cycle;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch doneLatch;
        private int produced;
        private int consumed;

        BusyProducerConsumer(int cycle, Sequence producerSequence, Sequence consumerSequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.producerSequence = producerSequence;
            this.consumerSequence = consumerSequence;
            this.cycle = cycle;
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();

                while (produced < cycle) {
                    long producerCursor;
                    do {
                        producerCursor = producerSequence.next();
                        if (producerCursor < 0) {
                            consume();
                        } else {
                            break;
                        }
                    } while (true);
                    assert queue.get(producerCursor).value == 0;
                    queue.get(producerCursor).value = 42;
                    producerSequence.done(producerCursor);
                    produced++;
                }
                // Consume the remaining messages.
                consume();

                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void consume() {
            while (true) {
                final long cursor = consumerSequence.next();
                if (cursor > -1) {
                    assert queue.get(cursor).value == 42;
                    queue.get(cursor).value = 0;
                    consumerSequence.done(cursor);
                    consumed++;
                } else {
                    break;
                }
            }
        }
    }

    private static class BusyProducer extends Thread {
        private final Sequence sequence;
        private final int cycle;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch doneLatch;

        BusyProducer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.sequence = sequence;
            this.cycle = cycle;
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (p < cycle) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        Os.pause();
                        continue;
                    }
                    queue.get(cursor).value = ++p == cycle ? Integer.MIN_VALUE : p;
                    sequence.done(cursor);
                }

                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class BusyConsumer extends Thread {
        private final Sequence sequence;
        private final int[] buf;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final CountDownLatch doneLatch;
        private volatile int finalIndex = 0;

        BusyConsumer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, CountDownLatch doneLatch) {
            this.sequence = sequence;
            this.buf = new int[cycle];
            this.queue = queue;
            this.barrier = barrier;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (true) {
                    long cursor = sequence.next();
                    if (cursor < 0) {
                        Os.pause();
                        continue;
                    }
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                    buf[p++] = v;
                }

                finalIndex = p;
                doneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class WaitingConsumer extends Thread {
        private final Sequence sequence;
        private final int[] buf;
        private final RingQueue<Event> queue;
        private final CyclicBarrier barrier;
        private final SOCountDownLatch latch;
        private volatile int finalIndex = 0;

        WaitingConsumer(int cycle, Sequence sequence, RingQueue<Event> queue, CyclicBarrier barrier, SOCountDownLatch latch) {
            this.sequence = sequence;
            this.buf = new int[cycle];
            this.queue = queue;
            this.barrier = barrier;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                int p = 0;
                while (true) {
                    long cursor = sequence.waitForNext();
                    int v = queue.get(cursor).value;
                    sequence.done(cursor);

                    if (v == Integer.MIN_VALUE) {
                        break;
                    }
                    buf[p++] = v;
                }

                finalIndex = p;
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}