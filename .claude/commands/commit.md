Inspect the currently staged changes and commit them.

## Steps

1. Run `git diff --cached` to see what is staged. If nothing is staged, tell the user and stop.
2. Run `git diff --cached --stat` to get a high-level summary of files changed.
3. Analyze the staged diff carefully and draft a commit message following the style rules below.
4. Show the user the proposed commit message and a brief rationale. Ask them to approve, edit, or reject it before committing.
5. Once approved, run `git commit -m "<message>"` (no Co-Authored-By trailer).

## Commit message style rules

These rules are derived from the project's existing commit history:

- **Lowercase start**: always begin with a lowercase letter.
- **Imperative/descriptive verb**: lead with a verb that describes the change — e.g. `simplify`, `fix`, `add`, `remove`, `replace`, `improve`, `restore`, `split`, `unify`, `move`, `harden`, `stabilize`, `streamline`, `clean up`, `modernize`, `route`, `tie`, `shift`.
- **Concise**: keep the title to a single line, ideally under 60 characters.
- **Detail when necessary**: if context isn't clear in the title alone, add one or more concise lines/paragraphs decribing key points below. Keep within standard commit message line-wrapping lengths.
- **No trailing period**.
- **No conventional-commit prefixes** (no `feat:`, `fix:`, `chore:`, etc.).
- **No ticket or issue references**.
- **Describe what changed**, not why — e.g. `simplify auth model and command surface`, `improve churn resilience for peers`, `restore positional serve naming`.
- **No Co-Authored-By trailer**.
- **Signed commits (`--s`)**.
