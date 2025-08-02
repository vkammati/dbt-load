import configparser
import os
import subprocess
import sys


def _get_requirements_path() -> str:
    if len(sys.argv) > 1:
        requirements_path = sys.argv[1]
    else:
        requirements_path = os.path.join(os.getcwd(), "requirements.txt")

    if not os.path.exists(requirements_path):
        raise ValueError(f"Error: {requirements_path} does not exist.")

    return requirements_path


def _get_requirement_packages(requirements_path) -> dict[str, str]:
    with open(requirements_path, "r") as f:
        packages = {
            line.split("==")[0].strip(): line.split("==")[1].strip()
            for line in f
            if "==" in line
        }
    return packages


def _get_installed_version(package_name):
    """Retrieve the installed version of a package."""
    result = subprocess.check_output(
        [
            "pip",
            "show",
            package_name,
        ],
        universal_newlines=True,
    )
    for line in result.split("\n"):
        if line.startswith("Version:"):
            return line.split(": ")[1]

    return None


def _get_package_updates(packages):
    result = subprocess.check_output(
        [
            "pip",
            "list",
            "--outdated",
        ],
        universal_newlines=True,
    )
    lines = result.split("\n")[2:-1]

    updates = {}
    for line in lines:
        parts = line.split()
        package_name = parts[0]
        if package_name in packages:
            current_version = parts[1]
            latest_version = parts[2]
            updates[package_name] = (current_version, latest_version)

    return updates


def _update_requirements_package_version(
    requirements_path,
    package,
    n_version,
) -> None:
    with open(requirements_path, "r") as f:
        lines = f.readlines()

    # Find the package and update its version
    for index, line in enumerate(lines):
        if line.startswith(package + "=="):
            lines[index] = f"{package}=={n_version}\n"
            break

    with open(requirements_path, "w") as f:
        f.writelines(lines)


def _pin_requirements(requirements_path) -> None:
    with open(requirements_path, "r") as f:
        requirements = f.readlines()

    # Check and pin version if missing
    for index, line in enumerate(requirements):
        if "==" in line:
            package_name, current_version = line.strip().split("==")
        else:
            package_name = line.strip()
            current_version = None

        installed_version = _get_installed_version(package_name)

        if installed_version != current_version:
            if installed_version:
                requirements[index] = f"{package_name}=={installed_version}\n"
                print(f"Pin version {package_name}=={installed_version}\n")

    with open(requirements_path, "w") as f:
        f.writelines(requirements)


def _update_package(requirements_path, updates) -> None:
    print("Installing updates...")
    for package, (o_version, n_version) in updates.items():
        subprocess.check_call(
            [
                "pip",
                "--quiet",
                "install",
                f"{package}=={n_version}",
            ]
        )
        print(f"{package} updated!")
        _update_requirements_package_version(
            requirements_path,
            package,
            n_version,
        )


def _upgrade_setup_cfg(requirement_packages):
    config = configparser.ConfigParser()
    config.read("setup.cfg")

    install_requires = config["options"]["install_requires"]
    install_requires_latest = ""

    # Loop through all "install requirements"
    for package_line in install_requires.strip().split("\n"):
        package_name = package_line.split("==")[0].strip()

        # if package is also in requirements.txt, use that (updated) version, else
        # leave the line unchanged.
        if package_name in requirement_packages.keys():
            install_requires_latest += (
                f"\n{package_name} == {requirement_packages[package_name]}"
            )
        else:
            install_requires_latest += f"\n{package_line}"

    config["options"]["install_requires"] = install_requires_latest

    with open("setup.cfg", "w") as f:
        config.write(f)


def main() -> None:

    requirements_path = _get_requirements_path()

    # Update the requirements.txt to pin versions if they're not pinned
    _pin_requirements(requirements_path)

    requirement_packages = _get_requirement_packages(requirements_path)

    updates = _get_package_updates(requirement_packages.keys())

    if not updates:
        print("All packages are up to date!")
        return

    print("Updates are available for the following packages:")
    for package, (o_version, n_version) in updates.items():
        print(f"{package}: \033[91m{o_version}\033[0m -> " f"\033[92m{n_version}\033[0m")

    _update_package(requirements_path, updates)
    print(f"{requirements_path} has been updated!")

    requirement_packages = _get_requirement_packages(requirements_path)
    _upgrade_setup_cfg(requirement_packages)


if __name__ == "__main__":
    main()
