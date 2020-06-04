package com.td.mdcms.cdb.dump;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.td.mdcms.cdb.TestResources.COMPLEX_DUMP_LIST;
import static com.td.mdcms.cdb.TestResources.HAPPY_COMPLEX;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_INLINE_CR;
import static com.td.mdcms.cdb.TestResources.HAPPY_SIMPLE_INLINE_NL;
import static com.td.mdcms.cdb.TestResources.HAPPY_VERY_COMPLEX;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_DATA_LENGTH;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_EOF;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_KEY_LENGTH;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_KEY_TERMINATOR;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_LINE_TERMINATOR;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_PLUS;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_SIZE_SEPARATOR;
import static com.td.mdcms.cdb.TestResources.SAD_FORMAT_SIZE_TERMINATOR;
import static com.td.mdcms.cdb.TestResources.SIMPLE_CR_DUMP_LIST;
import static com.td.mdcms.cdb.TestResources.SIMPLE_DUMP_LIST;
import static com.td.mdcms.cdb.TestResources.SIMPLE_NL_DUMP_LIST;
import static com.td.mdcms.cdb.TestResources.VERY_COMPLEX_DUMP_LIST;
import static com.td.mdcms.cdb.TestResources.getDumpResourceUri;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.td.mdcms.cdb.exception.CdbFormatException;
import com.td.mdcms.cdb.model.ByteArrayPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdbDumpFileReaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(CdbDumpFileReaderTest.class);

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void cleanUp() {
    }

    @Test
    public void doubleClose() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            cdfr.close();
        }
   }

    @Test
    public void readDumpSimpleHappy() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            List<ByteArrayPair> dumpList = new ArrayList<>();
            cdfr.forEach(dumpList::add);
            assertEquals(SIMPLE_DUMP_LIST, dumpList);
        }
    }

    @Test
    public void readDumpSimpleInlineCrHappy() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_CR));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            List<ByteArrayPair> dumpList = new ArrayList<>();
            cdfr.forEach(dumpList::add);
            assertEquals(SIMPLE_CR_DUMP_LIST, dumpList);
        }
    }

    @Test
    public void readDumpSimpleInlineNlHappy() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_SIMPLE_INLINE_NL));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            List<ByteArrayPair> dumpList = new ArrayList<>();
            cdfr.forEach(dumpList::add);
            assertEquals(SIMPLE_NL_DUMP_LIST, dumpList);
        }
    }

    @Test
    public void readDumpComplexHappy() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_COMPLEX));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            List<ByteArrayPair> dumpList = new ArrayList<>();
            cdfr.forEach(dumpList::add);
            assertEquals(COMPLEX_DUMP_LIST, dumpList);
        }
    }

    @Test
    public void readDumpVeryComplexHappy() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(HAPPY_VERY_COMPLEX));
        try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
            List<ByteArrayPair> dumpList = new ArrayList<>();
            cdfr.forEach(dumpList::add);
            assertEquals(VERY_COMPLEX_DUMP_LIST, dumpList);
        }
    }

    @Test
    public void readDumpDataLengthSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_DATA_LENGTH));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpNoEOFSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_EOF));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpKeyLengthSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_KEY_LENGTH));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpKeyTerminatorSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_KEY_TERMINATOR));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpLineTerminatorSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_LINE_TERMINATOR));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpLinePrefixSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_PLUS));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpSizeSeparatorSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_SIZE_SEPARATOR));
        assertSadFormatException(cdbDumpPath);
    }

    @Test
    public void readDumpSizeTerminatorSad() throws URISyntaxException {
        Path cdbDumpPath = Path.of(getDumpResourceUri(SAD_FORMAT_SIZE_TERMINATOR));
        assertSadFormatException(cdbDumpPath);
    }

    private void assertSadFormatException(Path cdbDumpPath) {
        assertThrows(CdbFormatException.class, () -> {
            try (CdbDumpReader cdfr = new CdbDumpReader(cdbDumpPath)) {
                List<ByteArrayPair> dumpList = new ArrayList<>();
                cdfr.forEach(dumpList::add);
            }
        });
    }
}
