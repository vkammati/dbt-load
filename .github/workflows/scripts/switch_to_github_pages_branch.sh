# Check if github_pages branch already exists
_branch_name="github_pages"
echo "Checking if branch ${_branch_name} exists."
_check_branch=$(git ls-remote --heads origin ${_branch_name})

if [[ -z ${_check_branch} ]]; then
    echo "Branch '${_branch_name}' does NOT exist. Creating it."
    git checkout -b ${_branch_name}
    echo "Removing all files and folders except 'docs' folder."
    find . -mindepth 1 -maxdepth 1 -not -name docs -not -name .git -print0 \
        | xargs -0 -I {} rm -r "{}"
else
    echo "Switching to branch '${_branch_name}'."
    git fetch --all -q
    git switch ${_branch_name}
fi
