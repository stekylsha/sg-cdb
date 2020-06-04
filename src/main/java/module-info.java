module com.td.mdcms.cdb {
    requires java.base;
    requires org.slf4j;

    exports com.td.mdcms.cdb;
    exports com.td.mdcms.cdb.db;
    exports com.td.mdcms.cdb.dump;
    exports com.td.mdcms.cdb.exception;
    exports com.td.mdcms.cdb.model;
}
