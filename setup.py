from setuptools import setup


def load_requirements(use_case):
    """
    Loading range requirements.
    Packaging should be used for installing the package into existing stacks.
    We therefore read the .in file for the use case.
    .txt files include the exact pins, and are useful for deployments or dev
    environments with exactly comparable environments.
    """
    reqs = []
    with open("requirements/%s.in" % use_case, "r") as f:
        reqs = [
            req
            for req in f.read().splitlines()
            if not req.strip() == ""
            and not req.strip().startswith("#")
            and not req.strip().startswith("-c")
            and not req.strip().startswith("--find-links")
        ]
    return reqs


setup(install_requires=load_requirements("app"))

