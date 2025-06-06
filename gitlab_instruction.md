# GitLab Usage Instructions

## Understanding Git Branches

Branches in Git are like separate workspaces for your code. Here's why they're useful:

1. **Main Branch**
   - This is the primary, stable version of your code
   - Always contains working, tested code
   - Everyone on the team uses this as the base

2. **Feature Branches**
   - Like a separate workspace for your changes
   - You can experiment without affecting the main code
   - If something goes wrong, it's isolated to your branch
   - Perfect for:
     - Adding new features
     - Fixing bugs
     - Trying new ideas

3. **Why Use Branches?**
   - **Safety**: Your changes don't affect the main code until ready
   - **Organization**: Each feature/bug fix has its own space
   - **Collaboration**: Multiple people can work on different things
   - **Review**: Changes can be reviewed before going into main code

4. **Direct Push vs Feature Branch**
   ```bash
   # Direct Push to Main (Not Recommended)
   git checkout main
   git add .
   git commit -m "Your changes"
   git push origin main
   
   # Using Feature Branch (Recommended)
   git checkout -b feature/your-changes
   git add .
   git commit -m "Your changes"
   git push origin feature/your-changes
   ```
   
   **Why Feature Branches are Better:**
   - Changes are reviewed before merging
   - Prevents accidental breaking of main code
   - Better collaboration with team members
   - Easier to track and manage changes
   - Can work on multiple features simultaneously

5. **What Happens After Merging?**
   - When your branch is merged into main:
     - Your changes become part of the main code
     - The main branch is updated with your changes
     - Your feature branch is no longer needed
   - You can choose to:
     - Delete the branch (common practice)
     - Keep it for reference
   - The merge process is permanent - your changes are now part of the main code

5. **Practical Example**
   ```bash
   # You're working on adding a new feature
   
   # 1. Start from the main branch (always up to date)
   git checkout main
   git pull origin main
   
   # 2. Create a new branch for your feature
   git checkout -b feature/add-new-button
   
   # 3. Make your changes (they're isolated in this branch)
   # ... make changes to your code ...
   
   # 4. Test your changes
   # ... run tests ...
   
   # 5. If everything works, commit your changes
   git add .
   git commit -m "Added new button feature"
   
   # 6. Push your branch to GitLab
   git push origin feature/add-new-button
   
   # 7. Create a merge request for review
   # (This lets others check your changes before they go into main)
   
   # 8. After merge is approved and completed:
   git checkout main
   git pull origin main
   git branch -d feature/add-new-button  # Delete the branch (optional)
   ```

## Initial Setup

1. **Clone the Repository**
   ```bash
   git clone <your-gitlab-repo-url>
   cd <repo-name>
   ```

2. **Set Up Python Environment**
   ```bash
   # Activate the pyenv virtual environment
   pyenv activate tevi_venv
   
   # Install uv if not already installed
   pip install uv
   ```

3. **Managing Local Files**
   ```bash
   # Add files to .gitignore that should not be tracked
   # Example: echo "local_file.md" >> .gitignore
   
   # Common files to ignore:
   # - Local documentation (e.g., GITLAB_INSTRUCTIONS.md)
   # - Environment-specific files
   # - Personal notes
   # - Local configuration files
   ```

## Managing Dependencies

1. **Installing New Packages**
   ```bash
   # Always use --active flag to ensure packages are installed in the pyenv virtual environment
   uv add --active <package-name>
   ```

2. **Updating Dependencies**
   ```bash
   # Update all dependencies
   uv pip sync
   
   # Update specific package
   uv add --active <package-name> --upgrade
   ```

3. **Syncing with pyproject.toml**
   ```bash
   # Install all dependencies from pyproject.toml
   uv pip sync
   
   # Update pyproject.toml with current dependencies
   uv pip freeze > requirements.txt
   ```

## GitLab Authentication

1. **Setting Up Access Token**
   - Go to GitLab (https://gitlab.tevi.dev)
   - Click your profile picture → Preferences/Settings
   - In left sidebar, click "Access Tokens"
   - Create new token with permissions:
     - `read_repository`
     - `write_repository`
   - Copy the token immediately (format: `glpat-XXXXXXXXXXXXXXXXXXXX`)

2. **Configuring Git Remote**
   ```bash
   # Remove existing remote
   git remote remove origin
   
   # Add new remote with token
   git remote add origin https://<username>:<token>@gitlab.tevi.dev/tevi/data/pipelines.git
   
   # Verify remote
   git remote -v
   ```

## Git Workflow

1. **Checking Repository Status**
   ```bash
   # Check current status of your repository
   git status
   
   # Pull latest changes from remote
   git pull origin main
   ```

2. **Making Changes**
   ```bash
   # Create a new branch ONLY if you're planning to make changes
   git checkout -b feature/your-feature-name
   
   # Make your changes to the code
   # ...
   
   # Stage your changes
   git add .
   
   # Commit your changes
   git commit -m "Description of your changes"
   ```

3. **Pushing Changes**
   ```bash
   # Push your branch to GitLab
   git push origin feature/your-feature-name
   ```

4. **Creating Merge Request**
   - After pushing, GitLab will provide a merge request URL
   - Click the URL or go to GitLab → Your Project → Merge Requests → New
   - Fill in the merge request form:
     - Title: Clear description of changes
     - Description: Detailed explanation of changes
     - Select reviewers if needed
   - Click "Create merge request"

5. **After Merge Request is Approved**
   ```bash
   # Switch back to main branch
   git checkout main
   
   # Pull latest changes
   git pull origin main
   
   # Delete your feature branch (optional)
   git branch -d feature/your-feature-name
   ```

## Syncing with Teammate Updates

If your teammate has updated the repository, or if someone pushes code before you commit, follow these steps to keep your work in sync and avoid conflicts:

### 1. If You Have No Local Changes
```bash
git pull origin <branch-name>
```
- Replace `<branch-name>` with your working branch (e.g., `main` or `develop`).
- This will update your local branch with the latest changes from the remote repository.

### 2. If You Have Local Changes (Uncommitted)

#### Option A: Stash Your Changes (Recommended)
```bash
git status  # Check your changes
git stash   # Temporarily save your local changes
git pull origin <branch-name>  # Get the latest updates
git stash pop  # Re-apply your changes
```
- If there are conflicts after `git stash pop`, resolve them, then:
  ```bash
  git add <conflicted-file>
  git commit -m "Resolved merge conflicts"
  ```

#### Option B: Commit Your Changes First
```bash
git add .
git commit -m "Your commit message"
git pull origin <branch-name>
```
- If there are conflicts, Git will prompt you to resolve them. After resolving:
  ```bash
  git add <conflicted-file>
  git commit
  ```

### 3. After Syncing
- Continue working as usual.
- When ready, push your changes:
  ```bash
  git push origin <branch-name>
  ```

### Example Workflow (If Teammate Pushes Before You Commit)
```bash
git status
git stash
git pull origin main
git stash pop
# Resolve any conflicts if prompted
git add .
git commit -m "Describe your changes"
git push origin main
```

**Summary:**
- Always pull the latest changes before starting new work.
- If you have local changes, stash or commit them before pulling.
- Resolve any merge conflicts as needed.

## Common Issues and Solutions

1. **Authentication Issues**
   - If you get "Access denied" errors:
     - Verify your access token is correct
     - Check token permissions
     - Ensure token hasn't expired
   - To update credentials:
     - Remove existing remote
     - Add new remote with updated token

2. **Virtual Environment Issues**
   - Always use `--active` flag with `uv add` to ensure packages are installed in the correct environment
   - If you see a `.venv` directory being created, you're not using the `--active` flag

3. **Dependency Conflicts**
   - Use `uv pip sync` to ensure all dependencies match pyproject.toml
   - If conflicts persist, check the package versions in pyproject.toml

4. **Git Issues**
   - If you get conflicts, pull the latest changes first
   - Use `git status` to check your current state
   - Use `git log` to see commit history
   - If you see files that shouldn't be tracked, add them to `.gitignore`

## Best Practices

1. **Dependencies**
   - Always specify version numbers in pyproject.toml
   - Use `uv add --active` for new packages
   - Keep pyproject.toml and requirements.txt in sync

2. **Git**
   - Create new branches for each feature/fix
   - Write clear commit messages
   - Keep your local repository up to date
   - Use `.gitignore` for files that should remain local

3. **Code**
   - Follow the project's coding standards
   - Write tests for new features
   - Document your changes

## Working with Submodules or Pushing Subfolders to a Different Repo

Sometimes you may have a directory (like `personal_github`) inside your main project that is its own git repository (a submodule or standalone repo). You might want to push this code to a different GitHub repository (for example, your personal GitHub account).

### 1. Check if the folder is a git repository
```bash
cd /path/to/personal_github
# Check git status and current remote
git status
git remote -v
```

### 2. Change the remote to your personal GitHub repo
```bash
# Remove the current remote (if needed)
git remote remove origin
# Add your personal GitHub repo as the new remote
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
# Or use SSH if you prefer:
git remote add origin git@github.com:YOUR_USERNAME/YOUR_REPO.git
# Verify
git remote -v
```

### 3. Add, commit, and push your changes
```bash
git add .
git commit -m "Your commit message"
git push -u origin main  # or your branch name
```

### 4. (Optional) Restore the original remote
If you want to push back to the original remote later, you can add it again:
```bash
git remote add origin https://github.com/ORIGINAL_OWNER/ORIGINAL_REPO.git
```
Or use a different name (e.g., `personal` and `company`):
```bash
git remote add personal https://github.com/YOUR_USERNAME/YOUR_REPO.git
git remote add company https://gitlab.com/COMPANY/REPO.git
```
Then push to the desired remote:
```bash
git push personal main
git push company main
```

**Tip:**
- If you want to keep the subfolder in sync with both remotes, use different remote names and push to each as needed.
- If you only want to copy the code once, you can also just copy the folder, initialize a new git repo, and push as a new project.

## Keeping Company and Personal Repos Separate

When working with multiple repositories (e.g., `company_gitlab` and `personal_github`), it's important to keep their histories and remotes separate to avoid accidentally pushing code to the wrong place.

### 1. Check which repository you are in
```bash
git rev-parse --show-toplevel
```
- This will print the root directory of the current git repository. Make sure it matches the folder you expect.

### 2. Check the current remote(s)
```bash
git remote -v
```
- This shows the URLs for fetch and push. Make sure you are pushing to the correct remote (company or personal).

### 3. Change the remote if needed
```bash
# Remove the current remote (if you want to change it)
git remote remove origin
# Add the correct remote
git remote add origin <your-remote-url>
# Example for company:
git remote add origin https://gitlab.com/company/repo.git
# Example for personal:
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
```

### 4. Best Practices
- **Never copy the `.git` folder** from one repo to another.
- **Do not nest one git repo inside another** unless you are intentionally using submodules.
- **Only commit and push in the repo you intend** (check your directory and remote before running git commands).
- **If you want to share code**, copy only the code files (not the git history), or use a shared library repo.

### 5. Troubleshooting
- If you accidentally committed or pushed company code to your personal repo (or vice versa), you can use `git reset` or `git revert` to undo the commit (see the 'How to uncommit' section above).
- If you are unsure which repo you are in, always check with `git rev-parse --show-toplevel` and `git remote -v` before committing or pushing.

**Summary:**
- Keep each repo's `.git` folder and remote separate.
- Always check your current directory and remote before running git commands.
- Use clear naming and organization to avoid confusion between company and personal projects.

## Notes

- These instructions also apply to GitHub repositories
- Replace `main` with your default branch name if different
- Always check the project's specific guidelines in the repository
- Keep sensitive and local-only files in `.gitignore`

## Python Environment Setup

### For Python 3.11
1. Create and activate virtual environment:
   ```bash
   pyenv virtualenv 3.11.9 tevi_venv
   pyenv activate tevi_venv
   ```

2. Install packages:
   ```bash
   uv add --active numpy==1.26.0 pandas==2.2.0
   ```

### For Python 3.13
1. Install BLAS library (required for numpy):
   ```bash
   brew install openblas
   ```

2. Set environment variables for openblas:
   ```bash
   export LDFLAGS="-L/opt/homebrew/opt/openblas/lib"
   export CPPFLAGS="-I/opt/homebrew/opt/openblas/include"
   export PKG_CONFIG_PATH="/opt/homebrew/opt/openblas/lib/pkgconfig"
   ```

3. Create and activate virtual environment:
   ```bash
   pyenv virtualenv 3.13.3 tevi_venv_313
   pyenv activate tevi_venv_313
   ```

4. Install packages:
   ```bash
   uv add --active numpy==1.26.0 pandas==2.2.0
   ```

Note: BLAS is a system-level dependency that needs to be installed before creating the Python 3.13 virtual environment. The environment variables are required for the compiler to find the openblas library during numpy installation. 