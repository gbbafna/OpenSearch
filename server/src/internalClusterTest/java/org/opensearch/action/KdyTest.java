/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class KdyTest {
    public static void main(final String... args) throws IOException {
        // Print PID
        long pid = ProcessHandle.current().pid();
        System.out.println("PID: " + pid);

        // Define Lucene directory
        String dirPath = "/Users/gbbafna/Desktop";
        String fileName1 = "_5_Lucene101_0.pos";
        String fileName2 = "_5_Lucene101_0.doc";
        MMapDirectory dir = new MMapDirectory(Path.of(dirPath));

        // Load one file. This will create a shared arena inside.
        IndexInput in = dir.openInput(fileName1, IOContext.DEFAULT);
        //IndexInput in = dir.openInput(fileName1, IOContext.READONCE);

        touchAll(in);

        // Now, try to open another file, load all then close index input.
        // Just one glance, this code looks like mapping one file at the beginning
        // and when `close` is called, it unmap the file.
        // BUT IT IS NOT!! Because, a shared arena is being used inside,
        // unless `in` is closed, the shared arena being used is still alive!
        // Therefore, even `fileName2` is closed, this process will maintain a list of mapped entries.
        for (int i = 0 ; i < Integer.MAX_VALUE ; ++i) {
            try (IndexInput in2 = dir.openInput(fileName2, IOContext.DEFAULT)) {
                touchAll(in2);
            }
        }
        in.close();
        dir.close();
    }

    private static void touchAll(IndexInput in) throws IOException {
        in.seek(0);
        long len = in.length();
        while (len > 0) {
            in.readByte();
            --len;
        }
    }
}
