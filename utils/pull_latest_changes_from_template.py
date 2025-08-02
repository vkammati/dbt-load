import os
from datetime import datetime

import git
from git import GitCommandError, Repo
from logger import get_logger, set_handler

# Create "root" logger and set handler
logger = get_logger("git_variables", "INFO")
set_handler(logger)


def git_checkout(repo, branch_name):
    """
    This function attempts to checkout the specified branch (`branch_name`).
    using the `git checkout` command provided by GitPython (`repo.git.checkout()`).
    If successful, it logs a confirmation message indicating the successful
    checkout of the branch. If any exception occurs during the checkout process,
    it catches the exception and logs an error message.
    Args:
    - repo: GitPython repository object representing the repository.
    - branch_name: Name of the branch to checkout.
    """
    try:
        repo.git.checkout(branch_name)
        logger.info(f"\nChecked out branch '{branch_name}' successfully.\n")
    except Exception as e:
        logger.info("Error:", e)


def check_and_add_remote(repo, remote_name, repo_url):
    """
    Checks if a remote exists in a Git repository using GitPython,
    and add it if it does not exist.
    Args:
    - repo: GitPython repository object.
    - remote_name: Name of the remote (template repo) to check/add.
    - repo_url: URL of the remote repository to add (template repo URL).
    Logs a message indicating whether the remote already exists or not.
    If the remote does not exist, attempts to create it using `repo.create_remote()`.
    Logs success message if the remote is added successfully,
    otherwise logs the encountered error message.
    """

    remotes = repo.remotes
    remote_names = [remote.name for remote in remotes]

    if remote_name in remote_names:
        logger.info(f"\nRemote '{remote_name}' already exists.\n")
    else:
        logger.info(f"\nRemote '{remote_name}' does not exist. Adding it now...\n")
        try:
            repo.create_remote(remote_name, repo_url)
            logger.info(f"\nRemote '{remote_name}' added successfully.\n")
        except Exception as e:
            logger.info("Error:", e)


def fetch_latest_changes_without_tags(repo, remote_name):
    """
    Fetch the latest changes from a remote Git repository without fetching tags.
    Args:
    - repo: GitPython repository object.
    - remote_name: Name of the remote repository to fetch changes from (template repo).
    Logs a message indicating that it's fetching the latest changes
    from the specified remote.
    Uses `repo.remote(remote_name)` to get the remote object and fetches changes
    using `remote.fetch(prune=True)`.
    Logs a success message if the fetch operation completes successfully.
    Logs an error message if any exception occurs during the fetch process.
    """

    try:
        logger.info(f"\nFetching latest changes from '{remote_name}'...\n")
        remote = repo.remote(remote_name)
        remote.fetch(prune=True)
        logger.info(f"\nLatest changes from '{remote_name}' fetched successfully.\n")
    except Exception as e:
        logger.info("Error:", e)


def create_and_checkout_branch_with_timestamp(repo, branch_prefix, remote_name):
    """
    Create a new branch in a Git repository with a timestamped name and switch to it.
    Args:
    - repo: GitPython repository object.
    - branch_prefix: Prefix to prepend to the timestamp to form the branch name.
    - remote_name: Name of the remote (template) repository to get changes from.
    Generates a branch name using the current timestamp formatted as '%Y%m%d%H%M%S'.
    Attempts to create the new branch using `repo.create_head(branch_name)`.
    Sets the repository's head reference to the newly created branch
    using `repo.head.reference = new_branch`.
    Logs success message if the branch creation and checkout are successful.
    Logs an error message if any exception occurs during the branch creation
    or checkout process.
    """
    # Generate branch name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    branch_name = f"{branch_prefix}-{timestamp}"

    # Create and checkout the new branch
    try:
        logger.info(
            "\nCreating a new branch to get the changes " f"from '{remote_name}'...\n"
        )
        new_branch = repo.create_head(branch_name)
        repo.head.reference = new_branch
        logger.info(f"\nCreated and switched to branch '{branch_name}' successfully.\n")
    except Exception as e:
        logger.info("Error:", e)


def merge_changes_from_template(repo, template_branch):
    """
    Merge changes from a template branch into the current branch in a Git repository,
    resolving conflicts by accepting changes from the template branch.
    Args:
    - repo: GitPython repository object.
    - template_branch: Name of the template branch to merge changes from.
    Performs a merge operation from `template_branch` into the current branch,
    using GitPython.
    Uses `repo.git.merge()` with options `--no-ff`, `--strategy-option=theirs`, and
    `allow_unrelated_histories=True` to merge changes from the template branch,
    while accepting changes from the template in case of conflicts.
    If a merge conflict occurs (`git.exc.GitCommandError`), resolves conflicts
    by keeping changes from the template branch for conflicting files.
    Deletes files that are deleted in both branches,
    or in the local branch. Adds untracked files to the index.
    """

    try:
        # Merge changes from the template branch, accepting theirs
        logger.info(
            "\nMerging changes from the template branch " f"'{template_branch}'...\n"
        )
        repo.git.merge(
            "--no-ff",
            "--strategy-option=theirs",
            template_branch,
            allow_unrelated_histories=True,
            m="Get changes from template",
        )
        logger.info("\nMerged changes from the template successfully.\n")

    except git.exc.GitCommandError as e:
        try:
            if "CONFLICT" in e.stdout:
                logger.info(
                    "Merge conflict occurred. Resolving conflicts "
                    "by keeping changes from the template branch."
                )
                for file in repo.index.diff(None):

                    # Check if the file is deleted in both branches
                    if file.a_blob is None and file.b_blob is None:
                        logger.info(
                            f"File '{file.b_path}' is deleted in both branches."
                            "Resolving as deleted."
                        )
                        repo.index.remove([file.b_path], working_tree=True)

                    # Check if the file is deleted in the local branch
                    elif file.a_blob is None:
                        logger.info(
                            f"File '{file.b_path}' is deleted in local branch."
                            "Resolving as deleted."
                        )
                        repo.index.remove([file.b_path], working_tree=True)

                    # Check if the file is deleted in the template branch
                    elif file.b_blob is None:
                        logger.info(
                            "File '{file.a_blob.path}' is deleted in" " template branch."
                        )
                        logger.info("Resolving as deleted.")
                        repo.index.remove([file.a_blob.path], working_tree=True)

                    # Check if the file is present in the local branch
                    elif file.a_blob.path not in repo.head.commit.tree:
                        logger.info(
                            f"File '{file.a_path}' is present in local branch."
                            " Resolving by keeping the file from local branch."
                        )
                        repo.git.checkout("--ours", file.a_blob.path)

                    # Keep the changes from the template branch for other conflicted files
                    else:
                        repo.git.checkout("--theirs", file.a_blob.path)
                        logger.info(
                            f"Conflicts resolved for {file.a_blob.path} "
                            "by keeping changes from the template branch."
                        )

                logger.info(
                    "Conflicts resolved by keeping changes " "from the template branch."
                )

        except Exception as e:
            # Handle other errors
            logger.info("AN error occurred:", e)

            # Add files not present in the template repository
            files_not_in_template = set(repo.untracked_files) - set(repo.untracked_files)
            repo.index.add(files_not_in_template)

            # repo.index.add(repo.untracked_files)

    # Add untracked files
    repo.index.add(repo.untracked_files)
    repo.git.add("--all")
    logger.info("\nAll changes added successfully to staging area.\n")


def soft_reset_and_restore_specific_files(repo):
    """
    Perform a soft reset to undo the last commit in a Git repository,
    and restore specific files from the staging area.
    Args:
    - repo: GitPython repository object.
    Performs a soft reset using `repo.git.reset('--soft', 'HEAD~1')`
    to undo the last commit while keeping changes staged.
    Restores specific files listed in `files_to_restore` from the staging area using
    `repo.git.restore('--staged', file)` for each file.
    Logs success messages for both the reset and file restoration operations.
    Logs an error message if any exception occurs during the file restoration process.
    """

    logger.info("\nPerforming a soft reset...\n")
    repo.git.reset("--soft", "HEAD~1")
    logger.info("\nSoft reset completed successfully.\n")

    # List of files to restore
    files_to_restore = [
        "config/",
        "dbt_transform/models/",
        "dbt_transform/analyses/",
        "dbt_transform/tests/",
        "dbt_transform/dbt_project.yml",
        "dbt_transform/seeds/",
        "README.md",
        "terraform/providers.tf",
        ".github/CODEOWNERS",
        ".github/workflows/cd_deployment_pipeline_aws.yml",
        ".github/release_template.md",
        "utils/benchmark",
        "utils/create_example_data",
    ]

    try:
        # Restore the specified files from the index (staging area)
        for file in files_to_restore:
            repo.git.restore("--staged", file)
        logger.info("\nSpecific files restored successfully.\n")

    except Exception as e:
        logger.info(f"An error occurred: {e}")


def commit_and_push_changes_to_remote(repo, template_branch):
    """
    Commit changes to the current branch in a Git repository,
    and push them to a remote repository.
    Args:
    - repo: GitPython repository object.
    - template_branch: Name of the template branch from which changes were merged.
    Commits changes to the current branch using `repo.index.commit()` ,
    with a message indicatingthat changes from `template_branch` were merged
    and conflicts were resolved.
    Logs a success message after committing changes.
    Attempts to push the committed changes to the remote repository specified as 'origin'.
    Sets up tracking for the local branch and logs a success message,
    if the push is successful.
    Logs an error message if any exception occurs during the push operation.
    """

    repo.index.commit(
        message=f"Latest change from {template_branch} merged " "(conflicts resolved)"
    )
    logger.info("\nChanges committed successfully.\n")

    logger.info("\nPushing the changes to the remote repository...\n")
    try:
        # Specify the name of the local branch
        local_branch_name = repo.active_branch.name

        # Specify the name of the remote (typically 'origin')
        remote_name = "origin"

        # Push the new local branch to the remote repository and set up tracking
        repo.git.push("-u", remote_name, local_branch_name)
        logger.info(
            f"Local branch '{local_branch_name}' set up to track remote branch '"
            f"{remote_name}/{local_branch_name}' and pushed successfully."
        )

    except GitCommandError as e:
        logger.info("An error occurred while pushing the branch:", e)


def get_git_root():
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    logger.info(f"git_root: {git_root}")


def check_for_uncommitted_changes(repo):
    """
    Check if there are any uncommitted changes in the repository.
    Args:
    - repo: GitPython repository object.
    Returns:
    - True if there are uncommitted changes, False otherwise.
    """

    # Get staged(uncommitted) files
    staged_files = [item.a_path for item in repo.index.diff("HEAD")]

    if staged_files:
        logger.info("\n\nThere are uncommitted (staged) changes in the repository:\n")
        for file_path in staged_files:
            logger.info(file_path)
        return True
    else:
        logger.info(
            "\nThere are no uncomitted (staged) changes in the repository."
            "Proceeding with the execution.\n"
        )
        return False


def display_changes_to_be_committed(repo, YELLOW, RESET):
    """
    Display the changes that are staged to be committed.
    Args:
    - repo: GitPython repository object.
    - YELLOW : ANSI color code for yellow color
    - RESET : ANSI color code to reset to default color
    Logs the changes that are staged to be committed using `repo.index.diff("HEAD")`.
    """

    # Get staged(uncommitted) files
    staged_files = [item.a_path for item in repo.index.diff("HEAD")]

    if staged_files:
        logger.info(
            f"\n\n{YELLOW}The following changes" f" are staged to be committed:{RESET}\n"
        )
        for file_path in staged_files:
            logger.info(file_path)


def commit_template_changes_user_input(YELLOW, RED, RESET):
    """
    Takes the user input to commit the changes or not.
    Args:
    - YELLOW : ANSI color code for yellow color
    - RED : ANSI color code for red color
    - RESET : ANSI color code to reset to default color
    Prompts the user to enter input on whether to commit changes or abort execution.
    If the user enters 'y', the changes are committed.
    If the user enters 'n' or any other input,
    the script is exited with a warning/error message.
    """

    # Set default response as 'y' to commit the changes
    commit_changes = "y"
    # Ask the user whether to commit the changes or abort execution
    commit_changes = input(
        f"\n\n{YELLOW}Do you want to commit the changes?" f"(y/n): {RESET}"
    )
    if commit_changes.lower() == "n":
        logger.warning(f"\n\n{YELLOW}WARNING : {RESET}You entered '{commit_changes}'.")
        logger.info(
            "\n\nPlease review the changes manually."
            "\nHere's the link to the DALPS documentation:\n"
            "https://dalps.shell.com/technologies/edpl/dbt-reference-pipeline/"
            "project/Update-your-project\n"
        )
        logger.info("\nExiting the script run.\n")
        return False

    elif commit_changes.lower() == "y":
        logger.info(
            f"\n\nYou entered '{commit_changes}'."
            f"\nContinuing with the commit process.\n"
        )
        return True

    else:
        logger.error(
            f"\n\n{RED}ERROR : {RESET}Invalid input '{commit_changes}'."
            "\nExiting the script run.\n"
        )
        return False


def display_uncommitted_changes_from_restored_files(repo, YELLOW, RESET):
    """
    Display the uncommitted changes from the restored files.
    Args:
    - repo: GitPython repository object.
    - YELLOW : ANSI color code for yellow color
    - RESET : ANSI color code to reset to default color
    Displays the uncommitted changes from the restored files
    """
    # Get diff between working tree and index (staging area)
    diff = repo.index.diff(None)

    # Extract paths of modified and untracked files
    modified_files = [item.a_path for item in diff if not item.deleted_file]
    untracked_files = repo.untracked_files

    if modified_files or untracked_files:
        logger.warning(
            f"\n\n{YELLOW}WARNING : {RESET}"
            "There are uncommitted changes from restored files.\n"
        )
        if modified_files:
            logger.info(f"\n{YELLOW}Modified Files : {RESET}")
            for file_path in modified_files:
                logger.info(file_path)
        if untracked_files:
            logger.info(f"\n{YELLOW}Untracked Files : {RESET}")
            for file_path in untracked_files:
                logger.info(file_path)


def delete_uncommitted_changes_user_input(YELLOW, RED, RESET):
    """
    Takes the user input to delete or retain the uncommitted changes.
    Args:
    - YELLOW : ANSI color code for yellow color
    - RED : ANSI color code for red color
    - RESET : ANSI color code to reset to default color
    Prompts the user to enter input on whether to delete or retain uncommitted changes.
    If the user enters 'd', the uncommitted changes are deleted.
    If the user enters 'r', the uncommitted changes are retained.
    If the user enters any other input, the script is exited with a warning/error message.
    """

    # Set default response as 'y' to delete the uncommitted changes
    delete_changes = "y"
    # Ask the user whether to delete or retain the uncommitted changes
    delete_changes = input(
        f"\n\n{YELLOW}Do you want to delete the uncommitted changes?" f"(y/n): {RESET}"
    )
    if delete_changes.lower() == "y":
        logger.warning(
            f"\n\n{YELLOW}WARNING : {RESET}You entered '{delete_changes}'."
            "\nDeleting the uncommitted changes.\n"
        )
        return True

    elif delete_changes.lower() == "n":
        logger.info(
            f"\n\nYou entered '{delete_changes}'."
            f"\nRetaining the uncommitted changes. Please review"
            " and commit as needed.\n"
        )
        return False
    else:
        logger.error(
            f"\n\n{RED}WARNING : {RESET}Invalid input '{delete_changes}'."
            "\nExiting the script run.\n"
        )
        return False


def main():
    # Manages Git operations for integrating changes from a template repository
    # into a local repository

    # Initialize cloned repository location, template repo URL,
    # remote name and template branch
    repo = Repo(get_git_root())
    repo_url = "https://github.com/sede-x/Template-EDP-DBT-Reference-Pipeline.git"
    remote_name = "template-dbt"
    template_branch = "template-dbt/main"

    # ANSI color codes to log warnings in YELLOW
    YELLOW = "\033[93m"  # Yellow color
    RED = "\033[91m"  # Red color
    RESET = "\033[0m"  # Reset to default color

    # Check for uncommitted changes in the repository
    if check_for_uncommitted_changes(repo):
        logger.error(
            f"\n\n{RED}ERROR : {RESET}Please commit or stash the changes before"
            " proceeding with the upgrade.\n"
        )
        logger.info("\n\nExiting the script run.\n")
        exit()

    # Checkout the main branch of the local repository to start the process
    branch_name = "main"
    git_checkout(repo, branch_name)

    # Add remote if not added, fetch latest changes
    check_and_add_remote(repo, remote_name, repo_url)
    fetch_latest_changes_without_tags(repo, remote_name)

    # Create and checkout a new branch with timestamp
    branch_prefix = "upgrade/get-changes-from-template"
    create_and_checkout_branch_with_timestamp(repo, branch_prefix, remote_name)

    # Merge changes from template branch
    merge_changes_from_template(repo, template_branch)

    # Perform soft reset and restore specific files
    soft_reset_and_restore_specific_files(repo)

    # Show the changes to be committed to the user
    display_changes_to_be_committed(repo, YELLOW, RESET)

    # Ask the user if they want to commit the changes, exit execution if not
    if commit_template_changes_user_input(YELLOW, RED, RESET) is False:
        exit()

    # Remaining code is only executed if user agrees to commit the changes

    # Commit and push changes to remote repository
    commit_and_push_changes_to_remote(repo, template_branch)

    # Show uncommitted changes from restored files to user
    display_uncommitted_changes_from_restored_files(repo, YELLOW, RESET)

    # Ask the user if they want to delete the uncommitted changes, delete if yes
    if delete_uncommitted_changes_user_input(YELLOW, RED, RESET) is True:
        # Delete uncommitted changes

        # untracked files
        repo.git.clean("-fd")

        # modified files
        repo.git.checkout("--", ".")

        logger.info("\n\nUncommitted changes have been deleted successfully.\n")


if __name__ == "__main__":
    main()
