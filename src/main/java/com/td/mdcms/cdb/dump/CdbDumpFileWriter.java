package com.td.mdcms.cdb.dump;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.model.ByteArrayPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdbDumpFileWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CdbDumpFileWriter.class);

    private static final String TMP_DUMP_PREFIX = "tmp-";
    private static final String TMP_DUMP_SUFFIX = ".cdb-dump";

    private Path cdbDumpPath;
    private OutputStream cdbDumpOutputStream;

    public CdbDumpFileWriter(Path cdbDumpPath) throws CdbIOException {
        this.cdbDumpPath = cdbDumpPath;
        try {
            if ((Files.exists(cdbDumpPath) && Files.isWritable(cdbDumpPath)) ||
                    (Files.notExists(cdbDumpPath) && Files.isWritable(cdbDumpPath.getParent()))) {
                this.cdbDumpOutputStream =
                        Files.newOutputStream(cdbDumpPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.TRUNCATE_EXISTING
                        );
            } else {
                this.cdbDumpOutputStream = null;
                throw new IOException("CDB dump file is not writeable.");
            }
        } catch (IOException ex) {
            this.cdbDumpOutputStream = null;
            throw new CdbIOException("IOException opening cdb dump file.", ex);
        }
    }

    public CdbDumpFileWriter writeDumpElement(byte[] key, byte[] value) throws CdbIOException {
        return writeDumpElement(new ByteArrayPair(key, value));
    }

    public CdbDumpFileWriter writeDumpElement(ByteArrayPair kv) throws CdbIOException {
        try {
            LOG.debug("dumping kv '{}'", kv);
            cdbDumpOutputStream.write('+');
            cdbDumpOutputStream.write(Integer.toString(kv.first.length).getBytes());
            cdbDumpOutputStream.write(',');
            cdbDumpOutputStream.write(Integer.toString(kv.second.length).getBytes());
            cdbDumpOutputStream.write(':');
            cdbDumpOutputStream.write(kv.first);
            cdbDumpOutputStream.write("->".getBytes());
            cdbDumpOutputStream.write(kv.second);
            cdbDumpOutputStream.write('\n');
        } catch (IOException ioe) {
            cleanUp();
            throw new CdbIOException("Problem dumping key/value.", ioe);
        }
        return this;
    }

    public void close() throws CdbIOException {
        if (cdbDumpOutputStream != null) {
            try {
                cdbDumpOutputStream.write('\n');
                cdbDumpOutputStream.close();
            } catch (IOException ioe) {
                cleanUp();
                throw new CdbIOException("Problem dumping finale.", ioe);
            } finally {
                cdbDumpOutputStream = null;
            }
        }
    }

    private void cleanUp() {
        try {
            cdbDumpOutputStream.close();
            Files.deleteIfExists(cdbDumpPath);
        } catch (IOException ex) {
            LOG.debug("Ignoring IOException while closing dump input stream.");
        } finally {
            cdbDumpOutputStream = null;
        }
    }
 }
