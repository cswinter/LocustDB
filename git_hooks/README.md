Git hook for running tests and compiling benchmark before pushing.

Activate by linking to the `pre-push` script from `.git/hooks`:

```bash
ln -s git_hooks/pre-push .git/hooks/pre-push
```

If you are using git bash on Windows and can't use symbolic links, and you may want to just copy the file instead:

```bash
cp git_hooks/pre-push .git/hooks/pre-push
```
