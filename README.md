# jcdb

## Overview

jcdb is a reboot of [Michael Alyn Miller's](http://www.strangegizmo.com/products/sg-cdb/)
Java version of [D.J. Bernstein's](http://cr.yp.to/cdb.html) constant database
(cdb) package. It is vastly different and retains little (read: none) of the sg-cdb code.

## Interesting Things

### Same things

This library follows the cdb spec where it is defined.  The tests were generated
with the `cdbmake` executable to verify the ones created match exactly.  They
do.

### Different things

There is no spec for the `cdbmake`, `cdbget`, and `cdbdump` utilities, and
haven't been implemeted.  Then again, if you need that functionality you should
probably be using them and not Java versions of them.

The interface deviates from the utilities to give a more Java like API to CDB.
Creating a CDB can be done through a dump file or by creating a `CdbBuilder` and
adding elements to it through its `add(byte[], byte[])` method. The `Cdb` class
provides the methods to query the CDB, getting a single value for a key
(`find(byte[])`), all values for a key (`findAll(byte[])`), iterating over
values for a key (`iterator(byte[])`), or iterating over all key/value pairs
(`iterator()`).

### Other things

While you can use binary values (everything is a byte array), the spec very
clearly says "strings" for the keys and values.  It also makes the dump files
pretty messy.

The JavaDoc should provide enough information to use the API.

## Change List

### 1.0.2
-----

- Rewrite of CDB to use classes and structures from recent JDK versions.
