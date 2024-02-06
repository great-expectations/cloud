# Contributing to `great_expectations_cloud`

## Managing Dependencies

Dependencies are the virtual environment is managed with [`poetry`](https://python-poetry.org/docs/)

### Synchronize Dependencies

To ensure you are using the latest version of the core and development dependencies run `poetry install --sync`.
Also available as an invoke task.
```console
invoke deps
```

### Updating `poetry.lock` dependencies

The dependencies installed in our CI and the docker build step are determined by the [poetry.lock file](https://python-poetry.org/docs/basic-usage/#installing-with-poetrylock).

[To update only a specific dependency](https://python-poetry.org/docs/cli/#update) (such as `great_expectations`) ...
```console
poetry update great_expectations
```

[To resolve and update all dependencies ...](https://python-poetry.org/docs/cli/#lock)
```console
poetry lock
```

In either case, the updated `poetry.lock` file must be committed and merged to main.


### Inspecting the dependency graph

https://python-poetry.org/docs/cli/#show

```console
poetry show --tree --no-dev
```

#### Why is a dependency included?

To determine why a particular dependency is part of the dependency tree we can use the `--why` flag to see which dependencies rely on it.

```console
poetry show --tree --why pydantic
```
```
great-expectations 0.18.8 Always know what to expect from your data.
└── pydantic >=1.9.2
    ├── annotated-types >=0.4.0
    │   └── typing-extensions >=4.0.0
    ├── pydantic-core 2.14.6
    │   └── typing-extensions >=4.6.0,<4.7.0 || >4.7.0 (circular dependency aborted here)
    └── typing-extensions >=4.6.1 (circular dependency aborted here)
```
