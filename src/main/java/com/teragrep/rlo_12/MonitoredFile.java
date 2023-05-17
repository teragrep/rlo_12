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

import java.nio.file.Path;

public class MonitoredFile {

    public enum Status {
        SYNC_NEW, // new files
        FILE_DELETED, // deleted files
        FILE_MODIFIED, // modified files, new files
        SYNC_MODIFIED, // modified while reading
        SYNC_RECREATED, // deleted and created again
        SYNC_DELETED,
        MODIFIED_SYNCHRONIZING,
        MODIFIED_SYNCHRONIZED, // read up-to-date files
        DELETE_SYNCHRONIZING,
        DELETE_SYNCHRONIZED
    }

    private final Path path;
    private final Status status;

    MonitoredFile(Path path, Status status) {
        assert(path != null);

        this.path = path;
        this.status = status;
    }

    public Status getStatus() {
        return this.status;
    }

    public Path getPath() {
        return path;
    }
}
