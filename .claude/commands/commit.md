Stage changes relevant to the current task and commit them.

## Steps

1. Review the conversation context to understand which files were changed as part of the current task.
2. Run `git status` to see all modified and untracked files. Stage only the files relevant to the current task using `git add <file>...`. Do not use `git add -A` or `git add .`.
3. Run `git diff --cached` and `git diff --cached --stat` to confirm what is staged.
4. Analyze the staged diff carefully and draft a commit message following the style rules below.
5. Run `git commit -s -m "<message>"` immediately — do not ask for confirmation.

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
