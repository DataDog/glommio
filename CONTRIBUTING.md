# Contributing

The best way to interact with this project is and will be a work in progress. However, there are some core tenets based
on my ([@glommer](https://github.com/glommer)) previous experiences (two decades of Linux and Linux-like projects) that
may be summarized as follows:

1. Code review is paramount but not enough to block progress on anyone's end. **What is more important is architectural
   review and unit tests.**
2. Splitting your changes in easy-to-consume, self-contained pieces is important but not enough to impose a constant
   burden on the code author.

Writing system-level software is challenging enough as it is; the last thing we want is to add an undue burden on
contributors.

If you are interested in understanding these motivations a bit better, you can check out
[this article.](https://medium.com/@glaubercosta_11125/the-linux-development-process-is-it-worth-the-hassle-4f09d7ff09a2)

## Pull Request Process

As of today (September 2021), this is the set of rules that materialize the principles above:

1. **Test your changes.** Unless what you are doing is _absolutely_ trivial, add unit tests. Good unit tests come in
   bundles, and should test for both the expected and unexpected cases.

2. **Discuss your architecture first.** If you are making changes that add new components, new data structures, or
   reorganize an existing flow, it is helpful to plan how best to achieve that. A GitHub issue is the place to have this
   conversation.

3. **Invest in your git log.** If you fix a bug, tell us more about how you found it, in which circumstances it appears,
   etc. This is important for others as well as for _future you_. Split significant changes in smaller, self-contained
   commits, so it is easier to follow what you have done. `git add -p` is a powerful tool, and you should use it.

   At the end, if the result is too hard to follow and the change is simple and limited in complexity, **squashing your
   commits is okay**. Otherwise, if the diff is complex or has a large surface area, we will ask you to rewrite history
   to preserve the individual commits of your branch.

## Licensing

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as described in the [README](README.md#license), without any
additional terms or conditions.
