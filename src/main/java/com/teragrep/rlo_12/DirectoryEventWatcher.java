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

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * gief config
 * String directory = "/path/to/dir"
 * boolean recurse = true;
 * filePattern = "^foo-(.*)$";
 * -&gt; /path/to/dir subscribe
 * -&gt; /path/to/dir/foo/fii/faa subscribe as well (any dir under /path/to/dir
 * -&gt; match any file under /path/to/dir or it's sub-directories for pattern
 */
public class DirectoryEventWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryEventWatcher.class);

    private final Path initialDirectory;
    private final WatchService directoryWatcher;
    private final Matcher filePatternMatcher;

    private final boolean recursive;

    private final FileVisitor<Path> visitor = new ScavengingFileVisitor();

    private final Map<WatchKey, Path> watchKeyPathMap = new ConcurrentHashMap<>();

    // map for storing files in a directory, this is for invoking delete when overflow is encountered
    private final Map<Path, Set<Path>> directoryPathSetMap = new HashMap<>();

    private final TransferQueue<MonitoredFile> transferQueue = new LinkedTransferQueue<>();

    private final long pollingInterval;
    private final TimeUnit pollingIntervalTimeUnit;

    /**
     * FileEventWatcher
     *
     * @param directory                Directory to monitor for changes
     * @param recursive                Recurse to sub-directories
     * @param filePattern              Filename pattern to match for from (sub-)directories
     * @param readConsumerSupplier     MonitoredFile Consumer Supplier which processes the events
     * @throws IOException Path.register throws IOException on initial directory
     */
    public DirectoryEventWatcher(Path directory,
                                 boolean recursive,
                                 Pattern filePattern,
                                 Supplier<Consumer<MonitoredFile>> readConsumerSupplier
    ) throws IOException {
        this.pollingInterval = Long.MAX_VALUE;
        this.pollingIntervalTimeUnit = TimeUnit.DAYS;
        this.initialDirectory = directory.toAbsolutePath();
        this.filePatternMatcher = filePattern.matcher("");
        this.recursive = recursive;

        this.directoryWatcher = initialDirectory.getFileSystem().newWatchService();

        FileStatusManager fileStatusManager = new FileStatusManager(transferQueue, readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        initialScan(initialDirectory);

    }

    /**
     * FileEventWatcher
     *
     * @param directory                Directory to monitor for changes
     * @param recursive                Recurse to sub-directories
     * @param filePattern              Filename pattern to match for from (sub-)directories
     * @param readConsumerSupplier     MonitoredFile Consumer Supplier which processes the events
     * @param interval                 Polling interval
     * @param intervalTimeUnit         Polling interval TimeUnit
     * @throws IOException Path.register throws IOException on initial directory
     */
    public DirectoryEventWatcher(Path directory,
                                 boolean recursive,
                                 Pattern filePattern,
                                 Supplier<Consumer<MonitoredFile>> readConsumerSupplier,
                                 long interval,
                                 TimeUnit intervalTimeUnit
    ) throws IOException {
        this.pollingInterval = interval;
        this.pollingIntervalTimeUnit = intervalTimeUnit;
        this.initialDirectory = directory.toAbsolutePath();
        this.filePatternMatcher = filePattern.matcher("");
        this.recursive = recursive;

        this.directoryWatcher = initialDirectory.getFileSystem().newWatchService();

        FileStatusManager fileStatusManager = new FileStatusManager(transferQueue, readConsumerSupplier);

        Thread fileStatusManagerThread = new Thread(fileStatusManager);
        fileStatusManagerThread.start();

        initialScan(initialDirectory);
    }

    /**
     * overflow handler will
     * 1) redirect all procesing threads to the scan-all-functionality
     * 2) after scan-all is depleted resume processing of the events
     * 3) yes
     */
    private void initialScan(Path directory) throws IOException {
        Files.walkFileTree(
                directory,
                EnumSet.noneOf(FileVisitOption.class),
                1,
                visitor
        );
    }

    /**
     * watch keys, keys are one per directory, in case of overflow, that dir
     * gets reprocessed only.
     * NOTE multiple threads on poll need multiple directories watched because
     * of the one key per dir fact
     *
     * @throws IOException Directory walking failed
     * @throws InterruptedException Directory polling was interrupted
     */
    public void watch() throws IOException, InterruptedException {
        while (true) {

            // wait for key to be signaled
            WatchKey key;
            try {
                key = directoryWatcher.poll(pollingInterval, pollingIntervalTimeUnit);
            } catch (InterruptedException x) {
                return;
            }

            if (key == null) {
                // very crude polling
                for (Path directory : directoryPathSetMap.keySet()) {
                    Set<Path> knownFilesSet = directoryPathSetMap.get(directory);
                    for (Path fileName : knownFilesSet) {
                        transferQueue.transfer(new MonitoredFile(directory.resolve(fileName), MonitoredFile.Status.FILE_MODIFIED));
                    }
                }
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                Path directory = watchKeyPathMap.get(key);

                if (directory == null) {
                    // has become invalid and was removed already
                    break;
                }

                // check the directory
                if (kind == OVERFLOW) {
                    LOGGER.trace("OVERFLOW triggered, resynchronizing.");

                    Set<Path> previousKnownFilesSet = directoryPathSetMap.get(directory);
                    directoryPathSetMap.put(directory, new HashSet<>());

                    Files.walkFileTree(
                            directory,
                            EnumSet.noneOf(FileVisitOption.class),
                            1,
                            visitor
                    );

                    // invoke delete on all files no longer known from the directory
                    if (previousKnownFilesSet.removeAll(directoryPathSetMap.get(directory))) {
                        for (Path path : previousKnownFilesSet) {
                            transferQueue.transfer(new MonitoredFile(path, MonitoredFile.Status.FILE_DELETED));
                        }
                    }
                    continue;
                }


                // The filename is the
                // context of the event.
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();

                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                            "ev <{}> on <[{}]>",
                            kind,
                            directory.resolve(filename));
                }

                if (kind == ENTRY_DELETE) {
                    // remove file from known files set
                    // invoke deletion
                    if (directoryPathSetMap.get(directory).contains(directory.resolve(filename))) {
                        transferQueue.transfer(new MonitoredFile(directory.resolve(filename), MonitoredFile.Status.FILE_DELETED));
                        directoryPathSetMap.get(directory).remove(directory.resolve(filename));
                    }
                    continue;
                }

                try {
                    LOGGER.trace("normal processing triggered");
                    Files.walkFileTree(
                            directory.resolve(filename),
                            EnumSet.noneOf(FileVisitOption.class),
                            1,
                            visitor
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // returns false if watch has become invalid
            boolean valid = key.reset();
            if (!valid) {
                watchKeyPathMap.remove(key);
            }
        }
    }

    private class ScavengingFileVisitor implements FileVisitor<Path> {
        ScavengingFileVisitor() {
        }

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
            LOGGER.trace("visitFile <[{}]>", path);

            if (path.toFile().isDirectory()) {
                if (path.toAbsolutePath().equals(initialDirectory) || recursive) {
                    if (!path.toFile().canRead()) {
                        // non-readables are skipped
                        if(LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Directory <[{}]> is not readable, skipping.", path.toAbsolutePath());
                        }
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    register(path);
                } else {
                    if(LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Path skipped <[{}]> due to non-recursive processing", path.toAbsolutePath());
                    }
                }
            } else if (path.toFile().isFile()) {
                if (filePatternMatcher.reset(path.getFileName().toString()).matches()) {
                    LOGGER.trace("visitFile filePatternMatcher matches <[{}]> adding!", path);
                    try {
                        transferQueue.transfer(new MonitoredFile(path.toAbsolutePath(), MonitoredFile.Status.FILE_MODIFIED));

                        // add file to known files map
                        Set<Path> fileSet = directoryPathSetMap.get(path.toAbsolutePath().getParent());
                        fileSet.add(path.toAbsolutePath());
                    } catch (InterruptedException e) {
                        // TODO exception handling
                        throw new RuntimeException(e);
                    }
                }
            }
            return FileVisitResult.CONTINUE;

        }

        @Override
        public FileVisitResult visitFileFailed(Path path, IOException exc) {
            LOGGER.warn("visitFileFailed <[{}]> is not accessible, skipping due to:", path, exc);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            LOGGER.trace("postVisitDirectory <[{}]>", dir);
            if (exc != null) {
                LOGGER.warn("Directory <[{}]> caused:", dir, exc);
                return FileVisitResult.SKIP_SUBTREE;
            } else {
                return FileVisitResult.CONTINUE;
            }
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("preVisitDirectory <[{}]>", dir.toAbsolutePath());
            }
            if (dir.toAbsolutePath().equals(initialDirectory) || recursive) {
                if (!dir.toFile().canRead()) {
                    // non-readables are skipped
                    if(LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Directory <[{}]> is not readable, skipping.", dir.toAbsolutePath());
                    }
                    return FileVisitResult.SKIP_SUBTREE;
                }
                register(dir.toAbsolutePath());
            } else {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Directory skipped <[{}]> due to non-recursive processing", dir.toAbsolutePath());
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }

    private void register(Path directory) throws IOException {
        Path absolutePath = directory.toAbsolutePath();
        LOGGER.trace("register <[{}]> resolved to <[{}]>", directory, absolutePath);

        // FIXME select proper watcher
        WatchKey key = absolutePath.register(directoryWatcher,
                ENTRY_CREATE,
                ENTRY_MODIFY,
                ENTRY_DELETE,
                OVERFLOW
        );

        // add to paths hashmap for tracing known files
        if (!directoryPathSetMap.containsKey(directory.toAbsolutePath())) {
            directoryPathSetMap.put(directory.toAbsolutePath(), new HashSet<>());
        }

        /*
        New directory found, processing it
         */
        if (!watchKeyPathMap.containsKey(key)) {
            LOGGER.trace("found unregistered directory <[{}]>", directory);
            watchKeyPathMap.put(key, directory);

            Files.walkFileTree(
                    directory,
                    EnumSet.noneOf(FileVisitOption.class),
                    1,
                    visitor
            );

        }
    }
}
