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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class WatcherManualTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatcherManualTest.class);


    @Test
    @EnabledIfSystemProperty(named = "runManualTmpWatcher", matches = "true" )
    public void testRun() throws IOException, InterruptedException {
        Path target = Paths.get("/tmp/");
        Supplier<Consumer<MonitoredFile>> consumerSupplier = () -> monitoredFile -> {
            LOGGER.info("ReadConsumer was called upon <[" + monitoredFile.getPath() + "]> for state <" + monitoredFile.getStatus() + ">");
        };

        DirectoryEventWatcher directoryEventWatcher = new DirectoryEventWatcher(
                target,
                true,
                Pattern.compile("^.*$"),
                consumerSupplier
        );
        directoryEventWatcher.watch();
    }
}
