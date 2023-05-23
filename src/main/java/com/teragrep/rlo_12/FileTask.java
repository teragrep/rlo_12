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

import java.util.concurrent.TransferQueue;
import java.util.function.Consumer;

class FileTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileTask.class);


    private final TransferQueue<MonitoredFile> resultQueue;

    private final MonitoredFile monitoredFile;

    private final Consumer<MonitoredFile> readConsumer;

    FileTask(TransferQueue<MonitoredFile> resultQueue, MonitoredFile monitoredFile, Consumer<MonitoredFile> readConsumer) {
        this.resultQueue = resultQueue;
        this.monitoredFile = monitoredFile;
        this.readConsumer = readConsumer;

        switch (this.monitoredFile.getStatus()) {
            case SYNC_NEW:
            case SYNC_MODIFIED:
            case SYNC_DELETED:
            case SYNC_RECREATED:
                break;
            default:
                throw new IllegalStateException("Invalid MonitoredFile state: <" + this.monitoredFile.getStatus() + ">");
        }


    }

    @Override
    public void run() {
        if(LOGGER.isTraceEnabled()) {
            LOGGER.trace("FileTask on path <[" + monitoredFile.getPath() + "]> for <" + monitoredFile.getStatus() + ">");
        }
        readConsumer.accept(monitoredFile);

        MonitoredFile result;

        switch (monitoredFile.getStatus()) {
            case SYNC_NEW:
            case SYNC_MODIFIED:
            case SYNC_RECREATED:
                result = new MonitoredFile(monitoredFile.getPath(), MonitoredFile.Status.MODIFIED_SYNCHRONIZED);
                break;
            case SYNC_DELETED:
                result = new MonitoredFile(monitoredFile.getPath(), MonitoredFile.Status.DELETE_SYNCHRONIZED);
                break;
            default:
                throw new IllegalStateException("Invalid result");
        }
        try {
            resultQueue.transfer(result);
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("Successfully processed <[" + result.getPath() + "]> returned with status <" + result.getStatus() + ">");
            }
        } catch (InterruptedException e) {
            // if service is shutdown, it's ok to discard this?
            throw new RuntimeException(e);
        }
    }
}
