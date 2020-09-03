# Contributing

The best way to interact with this project is and will be a work in progress. However there are
some core tenets based on my previous experiences (two decades of Linux and Linux-like projects)
that should inform the spirit of the any particular rule we come up with:

1. Code review is important, but it is not important enough to block progress on anyone's end.
   What is more important is architectural review and unit tests.

2. Splitting your changes in easy to consume pieces that are self contained is important, but
   not important enough to impose a constant burden on the code author.

Writing system software is challenging and it is not a myth that it is more challenging than
conventional user-level applications. My view is not that "if you are good enough to do systems
programming you are good enough to learn process XYZ", but rather "if you are already undertaking
something complex like system programming, I shouldn't make your life harder". 

As of today (September 2020), this is the set of rules that materialize the principles above:

## Pull Request Process

1. Unless what you are doing is absolutely trivial, add unit tests. Good unit tests come in bundles,
   and usually test for both the expected and how one handles the unexpected case. If we agree
   on what your unit tests do and cover, I don't care as much about how you implement things.

2. If you are making changes that add new components, new data structures, or reorganize an existing
   flow, it is helpful to discuss your architecture first. That discussion is better had over an
   issue or a separate .md file. If we agree on your architecture, and we agree on what your unit
   tests do and cover, I don't care as much about how you implement things.
   
3. Invest in making your commit messages descriptive. If you fix a bug, tell us more about how you found
   it, in which circumnstances it appears, etc. That is important for others as well as for future
   you. Split big changes in smaller commits so it is easier for others and future you to follow what
   you are doing. `git add -p` is a really powerful tool and you should use it.
   However, once you added a pull request, and that PR started to see lively discussion,
   do not rewrite history: keep adding code on top.

   If the end result is too hard to follow then we may ask you to do a final pass at the end to
   rewrite history. In that case you should open a new pull request that refers to the original one,
   and keep the original branch around. The diff against the original branch should be empty.

4. Code formatting and organization is not something worth arguing about. It has value, but not
   nearly enough to justify the time spent. In that way I like the way rust is opinionated. Code
   that is submitted has to be formatted `rustfmt --edition=2018`. Automating that through github
   actions or any other mechanism would be a welcome change.
   
If you are interested in understanding my motivations a bit better, you can check out
[this
article](https://medium.com/@glaubercosta_11125/the-linux-development-process-is-it-worth-the-hassle-4f09d7ff09a2)

## Adding new crates or code

If you add a new crate or import code from an existing repository,
please update the file LICENSE-3rdparty.csv to reflect your changes.
