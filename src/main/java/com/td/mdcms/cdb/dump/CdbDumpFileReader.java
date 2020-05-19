package com.td.mdcms.cdb.dump;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;

import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.exception.CdbIOException;
import com.td.mdcms.cdb.internal.IntPair;
import com.td.mdcms.cdb.model.ByteArrayPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdbDumpFileReader implements AutoCloseable, Iterable<ByteArrayPair> {
    private static final Logger LOG = LoggerFactory.getLogger(CdbDumpFileReader.class);

    /**
     * The max data length should be
     * <pre>
     *     <max integer> - <main table size> - <some slot table size>
     * </pre>
     *
     * We know main table size (2048), but have no idea the slot table size, and
     * the slot table entries <em>must</em> be within the 4GB limit.  As an
     * arbitrary limit, restrict the key and data values to 268435455 (24 bits).
     * This is smaller than the original code, but should be sufficient.
     */
    public static final int MAX_DATA_LENGTH = 0x0fffffff; // 268_435_455

    private InputStream cdbDumpInputStream;

    public CdbDumpFileReader(Path cdbDumpPath) throws CdbIOException {
        try {
            if (Files.exists(cdbDumpPath) &&
                    Files.isReadable(cdbDumpPath)) {
                this.cdbDumpInputStream = new BufferedInputStream(
                        Files.newInputStream(cdbDumpPath, StandardOpenOption.READ)
                );
            } else {
                this.cdbDumpInputStream = null;
                throw new CdbIOException("CDB dump file does not exist or is not readable.");
            }
        } catch (IOException ex) {
            this.cdbDumpInputStream = null;
            throw new CdbIOException("IOException opening cdb dump file.", ex);
        }
    }

    public void close() throws CdbIOException {
        if (cdbDumpInputStream != null) {
            try {
                cdbDumpInputStream.close();
            } catch (IOException ex) {
                throw new CdbIOException("IOException closing cdb dump file.", ex);
            } finally {
                cdbDumpInputStream = null;
            }
        }
    }

    public Iterator<ByteArrayPair> iterator() {
        return new CdbDumpFileIterator();
    }

    private ByteArrayPair readCdbDumpElement()
            throws CdbFormatException, CdbIOException {
        try {
            IntPair lengths = new IntPair(
                    readIntFromStream(cdbDumpInputStream, ','),
                    readIntFromStream(cdbDumpInputStream, ':')
            );
            if (lengths.first > MAX_DATA_LENGTH || lengths.second > MAX_DATA_LENGTH) {
                throw new CdbFormatException("Key or data is too large for cdb.");
            }
            return new ByteArrayPair(
                    readBytesFromStream(cdbDumpInputStream, lengths.first, "->".getBytes()),
                    readBytesFromStream(cdbDumpInputStream, lengths.second, "\n".getBytes())
            );
        } catch (IOException ex) {
            throw new CdbIOException("IOException while reading dump file.", ex);
        }
    }

    private int readIntFromStream(InputStream is, char terminator)
            throws CdbFormatException, IOException {
        int intRead = 0;
        int chr;
        while ((chr = is.read()) >= 0 && Character.isDigit(chr)) {
            intRead = (intRead * 10) + Character.getNumericValue(chr);
        }
        if (chr != terminator) {
            safeClose();
            throw new CdbFormatException(
                    "Incorrect cdb dump file format.  " +
                            "Expected '" + terminator +
                            "', read '" + (char)chr + "'.");
        }
        return intRead;
    }

    private byte[] readBytesFromStream(InputStream is, int count, byte[] terminator)
            throws IOException {
        byte[] ba = new byte[count];
        int actual = is.read(ba);
        if (actual != count) {
            throw new CdbFormatException("Expected " + count + " bytes but read " + actual);
        }
        byte[] trm = new byte[terminator.length];
        if (!(is.read(trm) == terminator.length && Arrays.equals(terminator, trm))) {
            safeClose();
            throw new CdbFormatException(
                    "Incorrect cdb dump file format.  " +
                            "Expected '" + new String(terminator) +
                            "', read '" + new String(trm) + "'.");
        }
        return ba;
    }

    private void safeClose() {
        try {
            close();
        } catch (CdbIOException ex) {
            LOG.debug("Ignoring IOException while closing dump input stream.");
        }
    }

    private class CdbDumpFileIterator implements Iterator<ByteArrayPair> {

        private CdbDumpFileIterator() {
        }

        @Override
        public boolean hasNext() {
            try {
                cdbDumpInputStream.mark(2);
                int chr = cdbDumpInputStream.read();
                cdbDumpInputStream.reset();
                return chr != '\n';
            } catch (IOException ex) {
                safeClose();
                throw new CdbIOException("hasNext available check failed.", ex);
            }
        }

        @Override
        public ByteArrayPair next() {
            try {
                int chr = cdbDumpInputStream.read();
                if (chr != '+') {
                    throw new CdbFormatException(
                            "Incorrect cdb dump file format.  " +
                                    "Expected '+' (0x" +
                                    Integer.toHexString('+') +
                                    "), read '" + (char)chr + "' (0x" +
                                    Integer.toHexString(chr)  + ").");
                }
                return readCdbDumpElement();
            } catch (IOException ex) {
                safeClose();
                throw new CdbIOException("next failed.", ex);
            }
        }
    }
}
