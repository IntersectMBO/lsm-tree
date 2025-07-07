# blockio

Perform batches of disk I/O operations. Performing batches of disk I/O can lead
to performance improvements over performing each disk I/O operation
individually. Performing batches of disk I/O *concurrently* can lead to an even
bigger performance improvement depending on the implementation of batched I/O.

The batched I/O functionality in the library is separated into an *abstract
interface* and *implementations* of that abstract interface. The advantage of
programming against an abstract interface is that code can be agnostic to the
implementation of the interface, allowing implementations to be freely swapped
out. The library provides multiple implementations of batched I/O:
platform-dependent implementations using the *real* file system (using
asynchronous I/O), and a simulated implementation for testing purposes.

See the `System.FS.BlockIO` module for an example of how to use the library.

On Linux systems the *real* implementation is backed by
[blockio-uring](https://hackage.haskell.org/package/blockio-uring), a library
for asynchronous I/O that achieves good performance when performing batches
concurrently. On Windows and MacOS systems the *real* implementation currently
simply performs each I/O operation sequentially, which should achieve about the
same performance as using non-batched I/O, but the library could be extended
with asynchronous I/O implementations for Windows and MacOS as well. The
simulated implementation also performs each I/O operation sequentially.

As mentioned before, the batched I/O functionality is separated into an
*abstract interface* and *implementations* of that abstract interface. The
advantage of programming against an abstract interface is that code can be
agnostic to the implementation of the interface. For example, we could run code
in production using the real file system, but we could also run the same code in
a testing environment using a simulated file system. We could even switch from a
default implementation to a more performant implementation in production if the
performant implementation is available. Lastly, the abstract interface allows us
to program against the file system in a uniform manner across different
platforms, i.e., operating systems.

The `blockio` library defines the abstract interface for batched I/O. The
library is an extension of the
[fs-api](https://hackage.haskell.org/package/fs-api) library, which defines an
abstract interface for (basic) file system I/O. Both `blockio` and `fs-api`
provide an implementation of their interfaces using the real file system in
`IO`.

The `blockio:sim` sub-library defines an implementation of the abstract
interface from `blockio` that *simulates* batched I/O. This sub-library is an
extension of the [fs-sim](https://hackage.haskell.org/package/fs-sim) library,
which defines an implementation of the abstract interface from `fs-api` that
simulates (basic) file system I/O.