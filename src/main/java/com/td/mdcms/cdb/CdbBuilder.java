/*
 * Copyright (C) 2019 by Teradata Corporation. All Rights Reserved. TERADATA CORPORATION
 * CONFIDENTIAL AND TRADE SECRET
 */
package com.td.mdcms.cdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.td.mdcms.cdb.db.CdbWriter;
import com.td.mdcms.cdb.dump.CdbDumpReader;
import com.td.mdcms.cdb.exception.CdbException;
import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.internal.IntPair;
import com.td.mdcms.cdb.internal.Key;
import com.td.mdcms.cdb.model.ByteArrayPair;
import com.td.mdcms.cdb.model.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a constant database (cdb) as per the cdb spec.  From
 * https://cr.yp.to/cdb/cdb.txt:
 * <pre>
 *A cdb contains 256 pointers to linearly probed open hash tables. The
 * hash tables contain pointers to (key,data) pairs. A cdb is stored in
 * a single file on disk:
 *
 *     +----------------+---------+-------+-------+-----+---------+
 *     | p0 p1 ... p255 | records | hash0 | hash1 | ... | hash255 |
 *     +----------------+---------+-------+-------+-----+---------+
 * </pre>
 * There's more, but that's the gist of it.
 *
 * This class translates a dump file ...
 * <pre>
 *     A record is encoded for cdbmake as +klen,dlen:key->data followed by a
 *     newline. Here klen is the number of bytes in key and dlen is the number
 *     of bytes in data. The end of data is indicated by an extra newline.
 * </pre>
 * ... into a cdb file.
 *
 * Although it is more of a translator/bridge/adapter, it is named {@code Make}
 * for historical purposes.
 */
public final class CdbBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CdbBuilder.class);

    /**
     * The default temp cdb file path, in case it is not explicit.
     */
    private static final Path DEFAULT_CDB_TMP_PATH =
            Path.of(System.getProperty("java.io.tmpdir"));

    /**
     * The default temp cdb file prefix, in case it is not explicit.
     */
    private static final String DEFAULT_CDB_TMP_PREFIX = "tmp-";

    private final Path cdbTmpPath;
    private final CdbDumpReader dumpReader;
    private final CdbWriter cdbWriter;

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database.  If the cdb file does not exist, create an empty one.
     * If it does exist, use it as the basis for creating a new one.
     */
    public static void build(Path cdbPath, Path cdbDumpPath)
            throws CdbException {
        build(cdbPath, cdbDumpPath,
                DEFAULT_CDB_TMP_PATH.resolve(DEFAULT_CDB_TMP_PREFIX + cdbPath.getFileName()));
    }

    /**
     * Constructs a CdbBuilder object and prepares it for the creation of a
     * constant database.  If the cdb file does not exist, create an empty one.
     * If it does exist, use it as the basis for creating a new one.
     */
    public static void build(Path cdbPath, Path cdbDumpPath, Path cdbTmpPath)
            throws CdbException {
        if (!Files.exists(cdbDumpPath)) {
            throw new CdbException("cdb dump file '" +
                    cdbDumpPath.toString() +
                    "' does not exist.");
        }
        CdbBuilder builder = new CdbBuilder(cdbDumpPath, cdbTmpPath);
        try {
            builder.processCdbDumpFile();
            builder.closeFiles();
            Files.move(cdbTmpPath, cdbPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (CdbException ex) {
            builder.cleanUp();
            throw ex;
        } catch (IOException ex) {
            builder.cleanUp();
            throw new CdbIOException("Could not move cdb file.", ex);
        }
    }

    private CdbBuilder(Path cdbDumpPath, Path cdbTmpPath) {
        this.cdbTmpPath = cdbTmpPath;
        dumpReader = new CdbDumpReader(cdbDumpPath);
        cdbWriter = new CdbWriter(cdbTmpPath);
    }

    private void cleanUp() {
        closeFiles();
        try { Files.deleteIfExists(cdbTmpPath); } catch (IOException ex) {}
    }

    private void closeFiles() {
        try { dumpReader.close(); } catch (CdbIOException ex) {}
        try { cdbWriter.close(); } catch (CdbIOException ex) {}
    }

    private void processCdbDumpFile()
            throws CdbFormatException, CdbIOException {
       dumpReader.forEach(cdbWriter::add);
    }
}