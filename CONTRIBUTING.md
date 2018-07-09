# Contributing to LocustDB

Thank you for your interest in contributing to LocustDB! Good places to start are [this blog post][blogpost], the [README.md][readme] and the [issue tracker][issues].

If you have any question about LocustDB, feel free to ask on [gitter][gitter].

## Working on issues
If you're looking for somewhere to start, check out the [good first issue tag][good-first-issue].

Feel free to ask for guidelines on how to tackle a problem on [gitter][gitter] or open a new [new issue][new-issue].
This is especially important if you want to add new features to LocustDB or make large changes to the already existing code base. LocustDB's core developers will do their best to provide help.

If you start working on an already-filed issue, post a comment on this issue to let people know that somebody is working it. Feel free to ask for comments if you are unsure about the solution you would like to submit.

We use the "fork and pull" model [described here][development-models], where contributors push changes to their personal fork and create pull requests to bring those changes into the source repository.

Your basic steps to get going:

* Fork LocustDB and create a branch from master for the issue you are working on.
* Please adhere to the code style that you see around the location you are working on.
* [Commit as you go][githelp].
* Include tests that cover all non-trivial code. Usually the easiest way of doing that is to add a new integration test to `tests/query_tests.rs`.
* If you are adding a performance optimisation, make sure there is a benchmark case in `benches/basic.rs` that covers the optimisation.
* Make sure `cargo test` passes and the bencharks can still be compiled (`cargo check --bench basic`). Running clippy and rustfmt is encouraged, but the existing code is not currently compliant.
* Push your commits to GitHub and create a pull request against LocustDB's `master` branch.

[githelp]: https://dont-be-afraid-to-commit.readthedocs.io/en/latest/git/commandlinegit.html
[development-models]: https://help.github.com/articles/about-collaborative-development-models/
[gitter]: https://gitter.im/LocustDB/Lobby
[issues]: https://github.com/cswinter/LocustDB/issues
[new-issue]: https://github.com/cswinter/LocustDB/issues/new
[good-first-issue]: https://github.com/cswinter/LocustDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22
[blogpost]: https://clemenswinter.com/2018/07/09/how-to-analyze-billions-of-records-per-second-on-a-single-desktop-pc/
[readme]: https://github.com/cswinter/LocustDB/blob/master/README.md
