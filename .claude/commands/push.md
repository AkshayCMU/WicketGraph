# description: Automatically stage, commit, and safely push changes to GitHub

You are a Senior AI Engineer. Please execute the following Git workflow:
1. Run `git status` and `git diff` to analyze the exact changes made to the workspace.
2. Generate a highly professional, conventional commit message (e.g., `feat:`, `chore:`, `refactor:`) that accurately describes these specific changes.
3. Run `git add .`
4. Run `git commit -m "<your_generated_message>"`
5. Run `git push`

**SAFETY PROTOCOL IF PUSH FAILS:**
- If the push fails (e.g., due to merge conflicts, remote changes, or hook failures), **DO NOT execute any destructive commands.**
- **STRICT RULE:** You are strictly forbidden from deleting any files, dropping stashes, or running `git reset --hard`.
- You are authorized to read the error and *edit* files to fix conflicts, but you must not delete the files.
- If you cannot safely resolve the issue via standard edits, stop immediately and write a detailed summary of the error to a new file named `push_failure_report.txt` in the root directory, then exit.