from setuptools import setup, find_namespace_packages

setup(
    name="sb-notion",
    version="0.6.0",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "notion-client>=2.0.0",
        "python-dotenv>=0.19.0",
        "rich>=10.0.0"
    ],
    entry_points={
        "console_scripts": [
            "sb-notion-generate=sb_notion.generate.cli:main"
        ]
    },
    python_requires=">=3.7",
    author="snadboy",
    description="A Python library for interacting with Notion databases",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/snadboy/sb-notion",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
