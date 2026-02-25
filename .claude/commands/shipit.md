Ship the current commit as a tagged release.

## Steps

### 1. Merge PR branch (if applicable)

Check if you're on a branch other than `main`:

- If on a PR branch:
  1. Run `git branch --show-current` to confirm
  2. Run `gh pr list --head <branch> --json number,title,state` to find the associated PR
  3. If a PR exists and is open, merge it with `gh pr merge <number> --squash --delete-branch`
  4. Run `git checkout main && git pull` to switch to main with the merged changes
- If already on `main`, continue to the next step

### 2. Determine next version

1. Run `git tag --sort=-v:refname | head -20` to list recent tags
2. Parse the latest tag to determine the current version components (major, minor, patch, pre-release)
3. Present the user with version bump options using `AskUserQuestion`. Compute concrete next versions based on the latest tag. For example, if the latest tag is `v0.1.0-alpha.8`, the options should be:
   - **Next pre-release**: `v0.1.0-alpha.9` (increment the pre-release number)
   - **Next patch**: `v0.1.1` (drop pre-release, bump patch)
   - **Next minor**: `v0.2.0` (bump minor, reset patch)
   - **Next major**: `v1.0.0` (bump major, reset minor and patch)

   Adapt the options to what makes sense given the current version. Always show concrete version strings, not abstract labels.

### 3. Tag and push

1. After the user selects a version, create an annotated tag: `git tag -a <version> -m <version>`
2. Push the tag: `git push origin <version>`

### 4. Print release tracking command

After the tag is pushed, print the command to watch the release workflow:

```
gh run watch $(gh run list --workflow=release.yml --limit=1 --json databaseId --jq '.[0].databaseId')
```

Tell the user they can run this to follow the release in their terminal.
