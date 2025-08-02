import re


# Function to extract variable names from variables.tf
def extract_tf_variables(file_path):
    with open(file_path, "r") as file:
        content = file.read()
    return re.findall(r'variable\s+"(\w+)"', content)


# Function to extract declared variables from GitHub Actions workflow
def extract_github_actions_variables(file_path):
    with open(file_path, "r") as file:
        content = file.read()
    return re.findall(r"TF_VAR_(\w+)", content)


def main():

    # Path to your variables.tf file
    variables_tf_path = "terraform/variables.tf"

    # Path to your GitHub Actions workflow file (cd_deploy.yml and ci_terrafom_plan.yml)
    cd_deploy = ".github/workflows/cd_deploy.yml"
    ci_terraform_plan = ".github/workflows/ci_terrafom_plan.yml"

    # Extract variables
    tf_variables = extract_tf_variables(variables_tf_path)
    deploy_ga_variables = extract_github_actions_variables(cd_deploy)
    terraform_plan_ga_variables = extract_github_actions_variables(ci_terraform_plan)

    # Check for missing variables
    missing_tf_plan_variables = [
        var for var in tf_variables if var not in terraform_plan_ga_variables
    ]
    missing_deploy_variables = [
        var for var in tf_variables if var not in deploy_ga_variables
    ]

    errors: list[str] = []

    if missing_tf_plan_variables:
        for var in missing_tf_plan_variables:
            errors.append(f"Missing variable TF_VAR_{var} in ci_terraform_plan.yml")

    if missing_deploy_variables:
        for var in missing_deploy_variables:
            errors.append(f"Missing variable TF_VAR_{var} in cd_deploy.yml")

    if errors:
        raise Exception("\n".join(errors))
    else:
        print("All terraform variables are declared in the GitHub Actions workflows.")


if __name__ == "__main__":
    main()
