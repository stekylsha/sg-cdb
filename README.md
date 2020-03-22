# sg-cdb

## Overview

sg-cdb is a pure Java version of `D.J. Bernstein`'s constant database
(cdb) package.  Its API is the same as the one used in the original cdb
library.  Files created with sg-cdb can be read with the cdb library and
vice versa.  My goal was to provide a seamless way of porting cdb code
to a Java environment without the need for a JNI library.

More cdb information can be found at [D.J. Bernstein's home page](http://cr.yp.to/cdb.html).
Please do **not** send sg-cdb questions to Mr. Bernstein.  He is not the
maintainer of this implementation.

See the [sg-cdb product page](http://www.strangegizmo.com/products/sg-cdb/)
on the strangeGizmo.com site for more information.

[D.J. Bernstein](http://cr.yp.to/)
[http://cr.yp.to/cdb.html](http://cr.yp.to/cdb.html)
[sg-cdb product page](http://www.strangeGizmo.com/products/sg-cdb/)



## Change List

### 1.0.4
-----

-   Fixed cdb.dump to avoid problems with charset conversion issues on
    some platforms (fixed by Ito Kazumitsu).
-   Fixed cdb.dump so that it outputs \\n as the record terminator on
    Windows platforms, which would otherwise output \\r\\n (fixed by Ito
    Kazumitsu).

### 1.0.3
-----

-   Fixed sign bit problem with binary keys (fixed by Eric Kampf).
-   Fixed findNext to correctly deal with multiple keys mapping to the
    same value (fixed by Eric Kampf).

### 1.0.2
-----

-   First public release.
