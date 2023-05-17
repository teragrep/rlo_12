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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

class FileStatusManager implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStatusManager.class);

    private final Map<Path, MonitoredFile.Status> pathCurrentStateMap = new HashMap<>();

    private final TransferQueue<MonitoredFile> transferQueue;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final AtomicBoolean run = new AtomicBoolean();

    private final Supplier<Consumer<MonitoredFile>> consumerSupplier;

    FileStatusManager(TransferQueue<MonitoredFile> transferQueue, Supplier<Consumer<MonitoredFile>> consumerSupplier) {
        this.transferQueue = transferQueue;
        this.consumerSupplier = consumerSupplier;
        this.threadPoolExecutor = new ThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    public void run() {
        run.set(true);
        while (run.get()) {
            try {
                MonitoredFile changedFile = transferQueue.poll(200, TimeUnit.MILLISECONDS);
                if (changedFile != null) {
                    if (!pathCurrentStateMap.containsKey(changedFile.getPath())) {
                        // new file
                        if (changedFile.getStatus() != MonitoredFile.Status.FILE_MODIFIED) {
                            // only from NONE to UNSYNCHRONIZED is allowed
                            throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected from <NONE> because <" + changedFile.getStatus() + ">");
                        }
                        // read it
                        pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZING);
                        LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <NONE> to <" + MonitoredFile.Status.MODIFIED_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");

                        threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_NEW), consumerSupplier.get()));
                        continue;
                    }

                    MonitoredFile.Status currentState = pathCurrentStateMap.get(changedFile.getPath());

                    switch (changedFile.getStatus()) {
                        case FILE_MODIFIED: // file state from DEW
                            switch (currentState) {
                                case MODIFIED_SYNCHRONIZED:
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZING);
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.MODIFIED_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_MODIFIED), consumerSupplier.get()));
                                    break;
                                case MODIFIED_SYNCHRONIZING:
                                    // still being read, not resubmitting, but indicating here that it needs to be re-read once reading completes
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.SYNC_MODIFIED);
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.SYNC_MODIFIED + "> because <" + changedFile.getStatus() + "> (modified while reading)");
                                    break;
                                case SYNC_DELETED:
                                case DELETE_SYNCHRONIZING:
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.SYNC_RECREATED);
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.SYNC_RECREATED + "> because <" + changedFile.getStatus() + ">");
                                    break;
                                case SYNC_MODIFIED:
                                case SYNC_RECREATED:
                                    // no-op
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state not changed from <" + currentState + "> because <" + changedFile.getStatus() + ">");
                                    break;
                                case FILE_DELETED:
                                case FILE_MODIFIED:
                                case SYNC_NEW:
                                case DELETE_SYNCHRONIZED:
                                    throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected from <" + currentState + "> because <" + changedFile.getStatus() + ">");
                            }
                            break;
                        case FILE_DELETED: // file state from DEW
                            switch (currentState) {
                                case MODIFIED_SYNCHRONIZED:
                                    // removed when everything (to our understanding) was read
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.DELETE_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.DELETE_SYNCHRONIZING);
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_DELETED), consumerSupplier.get()));
                                    break;
                                case SYNC_RECREATED:
                                case SYNC_MODIFIED:
                                case MODIFIED_SYNCHRONIZING:
                                    // task is already on this, indicate that it was removed
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.SYNC_DELETED + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.SYNC_DELETED);
                                    break;
                                case DELETE_SYNCHRONIZING: // must not happen, delete after delete is not possible
                                case FILE_MODIFIED:
                                case FILE_DELETED:
                                case SYNC_DELETED:
                                case SYNC_NEW:
                                case DELETE_SYNCHRONIZED:
                                    throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected from <" + currentState + "> because <" + changedFile.getStatus() + ">");
                            }
                            break;
                        case MODIFIED_SYNCHRONIZED:
                            // file has been read
                            switch (currentState) {
                                case MODIFIED_SYNCHRONIZING:
                                    // file has been read and not modified since reading started
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZED);
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.MODIFIED_SYNCHRONIZED + "> because <" + changedFile.getStatus() + ">");
                                    break;
                                case SYNC_RECREATED:
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.MODIFIED_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZING);
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_RECREATED), consumerSupplier.get()));
                                    break;
                                case SYNC_MODIFIED:
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.MODIFIED_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZING);
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_MODIFIED), consumerSupplier.get()));
                                    break;
                                case SYNC_DELETED:
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.DELETE_SYNCHRONIZING + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.DELETE_SYNCHRONIZING);
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_DELETED), consumerSupplier.get()));
                                    break;
                                case DELETE_SYNCHRONIZING:
                                case DELETE_SYNCHRONIZED:
                                case MODIFIED_SYNCHRONIZED:
                                case FILE_MODIFIED:
                                case SYNC_NEW:
                                case FILE_DELETED:
                                    throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected from <" + currentState + "> because <" + changedFile.getStatus() + ">");
                            }
                            break;
                        case DELETE_SYNCHRONIZED:
                            switch (currentState) {
                                case SYNC_DELETED:
                                case DELETE_SYNCHRONIZING:
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <NONE> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.remove(changedFile.getPath());
                                    break;
                                case SYNC_RECREATED:
                                    LOGGER.trace("Path <[" + changedFile.getPath() + "]> state changed from <" + currentState + "> to <" + MonitoredFile.Status.SYNC_RECREATED + "> because <" + changedFile.getStatus() + ">");
                                    pathCurrentStateMap.put(changedFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZING);
                                    threadPoolExecutor.execute(new FileTask(transferQueue, new MonitoredFile(changedFile.getPath(), MonitoredFile.Status.SYNC_RECREATED), consumerSupplier.get()));
                                    break;
                                case SYNC_MODIFIED:
                                case MODIFIED_SYNCHRONIZING:
                                case DELETE_SYNCHRONIZED:
                                case MODIFIED_SYNCHRONIZED:
                                case FILE_MODIFIED:
                                case SYNC_NEW:
                                case FILE_DELETED:
                                    throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected from <" + currentState + "> because <" + changedFile.getStatus() + ">");
                            }
                            break;
                        case MODIFIED_SYNCHRONIZING: // internal state
                        case DELETE_SYNCHRONIZING: // internal state
                        case SYNC_DELETED: // out-going state
                        case SYNC_NEW: // out-going state
                        case SYNC_MODIFIED: // out-going state
                        case SYNC_RECREATED: // out-going state
                            throw new IllegalStateException("Path <[" + changedFile.getPath() + "]> state change rejected because <" + changedFile.getStatus() + "> is engine internal state");
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void stop() {

        boolean cleanshutDown;
        try {
            cleanshutDown = threadPoolExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (!cleanshutDown) {
            threadPoolExecutor.shutdownNow();
        }

        run.set(false);
    }
}
