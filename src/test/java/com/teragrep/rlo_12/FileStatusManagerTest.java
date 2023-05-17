/*
   Java Record File Event Notifier rlo_12
   Copyright (C) 2023  Suomen Kanuuna Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.teragrep.rlo_12;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class FileStatusManagerTest {

    @Test
    public void deleteStateTest() throws InterruptedException {

        AtomicLong deleteCallCounter = new AtomicLong();
        AtomicLong readCallCounter = new AtomicLong();

        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();
        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("deleted-path"), monitoredFile.getPath());

            switch (monitoredFile.getStatus()) {
                case SYNC_NEW:
                    readCallCounter.addAndGet(1);
                    break;
                case SYNC_DELETED:
                    deleteCallCounter.addAndGet(1);
                    break;
                default:
                    break;
            }
        };


        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("deleted-path"), MonitoredFile.Status.FILE_MODIFIED));
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("deleted-path"), MonitoredFile.Status.FILE_DELETED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(1, deleteCallCounter.get());
        Assertions.assertEquals(1, readCallCounter.get());
    }

    @Test
    public void readStateTest() throws InterruptedException {

        AtomicLong callCounter = new AtomicLong();

        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();

        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("read-path"), monitoredFile.getPath());
            if (monitoredFile.getStatus() == MonitoredFile.Status.SYNC_NEW) {
                callCounter.addAndGet(1);
            }
        };

        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier
        );

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-path"), MonitoredFile.Status.FILE_MODIFIED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(1, callCounter.get());
    }

    @Test
    public void readMoreStateTest() throws InterruptedException {
        AtomicLong callCounter = new AtomicLong();

        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();

        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("read-more-path"), monitoredFile.getPath());

            switch (monitoredFile.getStatus()) {
                case SYNC_NEW:
                case SYNC_MODIFIED:
                    callCounter.addAndGet(1);
                    break;
                default:
                    break;
            }

        };


        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier
        );

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-more-path"), MonitoredFile.Status.FILE_MODIFIED));

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-more-path"), MonitoredFile.Status.FILE_MODIFIED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(2, callCounter.get());
    }

    @Test
    public void readDeleteStateTest() throws InterruptedException {
        AtomicLong readCallCounter = new AtomicLong();
        AtomicLong deleteCallCounter = new AtomicLong();


        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();

        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("read-delete-path"), monitoredFile.getPath());
            switch (monitoredFile.getStatus()) {
                case SYNC_NEW:
                    readCallCounter.addAndGet(1);
                    break;
                case SYNC_DELETED:
                    deleteCallCounter.addAndGet(1);
                    break;
                default:
                    break;
            }
        };


        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-delete-path"), MonitoredFile.Status.FILE_MODIFIED));

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-delete-path"), MonitoredFile.Status.FILE_DELETED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(1, readCallCounter.get());
        Assertions.assertEquals(1, deleteCallCounter.get());

    }


    @Test
    public void readReadDeleteStateTest() throws InterruptedException {
        AtomicLong readCallCounter = new AtomicLong();
        AtomicLong deleteCallCounter = new AtomicLong();


        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();

        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("read-read-delete-path"), monitoredFile.getPath());
            switch (monitoredFile.getStatus()) {
                case SYNC_NEW:
                case SYNC_MODIFIED:
                    readCallCounter.addAndGet(1);
                    break;
                case SYNC_DELETED:
                    deleteCallCounter.addAndGet(1);
                    break;
            }
        };


        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        // this should get processed
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-read-delete-path"), MonitoredFile.Status.FILE_MODIFIED));
        // this should not get processed
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-read-delete-path"), MonitoredFile.Status.FILE_MODIFIED));
        // this should get processed
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-read-delete-path"), MonitoredFile.Status.FILE_DELETED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(1, readCallCounter.get()); // other modify will not have the chance to hit here
        Assertions.assertEquals(1, deleteCallCounter.get());
    }

    @Test
    public void readDeleteReadStateTest() throws InterruptedException {
        AtomicLong readCallCounter = new AtomicLong();
        AtomicLong recreateCallCounter = new AtomicLong();
        AtomicLong deleteCallCounter = new AtomicLong();


        TransferQueue<MonitoredFile> monitoredFileTransferQueue = new LinkedTransferQueue<>();

        Supplier<Consumer<MonitoredFile>> readConsumerSupplier = () -> monitoredFile -> {
            Assertions.assertEquals(Paths.get("read-delete-read-path"), monitoredFile.getPath());
            switch (monitoredFile.getStatus()) {
                case SYNC_NEW:
                    readCallCounter.addAndGet(1);
                    break;
                case SYNC_RECREATED:
                    recreateCallCounter.addAndGet(1);
                    break;
                case SYNC_DELETED:
                    deleteCallCounter.addAndGet(1);
                    break;
                default:
                    break;
            }
        };


        FileStatusManager fileStatusManager = new FileStatusManager(
                monitoredFileTransferQueue,
                readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-delete-read-path"), MonitoredFile.Status.FILE_MODIFIED));
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-delete-read-path"), MonitoredFile.Status.FILE_DELETED));
        monitoredFileTransferQueue.transfer(new MonitoredFile(Paths.get("read-delete-read-path"), MonitoredFile.Status.FILE_MODIFIED));

        fileStatusManager.stop();

        fileStatusManagerThread.join();

        Assertions.assertEquals(1, readCallCounter.get());
        Assertions.assertEquals(1, recreateCallCounter.get());
        Assertions.assertEquals(0, deleteCallCounter.get());
    }
}
