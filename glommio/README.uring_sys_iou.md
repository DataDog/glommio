This crate includes imported code for
[uring-sys](https://github.com/ringbahn/uring-sys) and
[iou](https://github.com/ringbahn/iou)
from the ringbahn project.

It is my intention that those imports are temporary and I don't
mean this as a hard fork. On the other hand those crates haven't been
as actively maintained as we would like. They are very central for
what we do, as uring evolves very rapidly, so I have decided to
soft fork them.

Every patch that touches code in those directories should make at
least a good faith effort to merge the code back into iou and uring-sys.
Hopefully with time they will, at their own pace, have all the code
that we need in which case we can revert back to using them externally.
