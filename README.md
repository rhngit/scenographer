# scenographer

[![PyPI pyversions](https://img.shields.io/pypi/pyversions/scenographer.svg?style=flat-square)](https://pypi.python.org/pypi/scenographer/)
[![GitHub license](https://img.shields.io/github/license/zyperco/scenographer.svg?style=flat-square)](https://github.com/zyperco/scenographer/blob/master/LICENSE)
[![PyPI version shields.io](https://img.shields.io/pypi/v/scenographer.svg?style=flat-square)](https://pypi.python.org/pypi/scenographer/)

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg?style=flat-square)](https://GitHub.com/zyperco/scenographer/graphs/commit-activity)
[![zyperco](https://circleci.com/gh/zyperco/scenographer.svg?style=svg)](https://app.circleci.com/pipelines/github/zyperco/scenographer)

**scenographer** is a Python script that can create a subset of a postgres database, without losing referential integrity.

The goal is to be able to spawn data-correct databases to easily create new environments that can be used for testing and / or demo'ing.

Relevant links:
  - [Documentation](https://zyperco.github.io/scenographer/)

## Installation

Use [pip](https://pip.pypa.io/en/stable/) to install `scenographer`.

```bash
pip install scenographer
```

## Usage

Scenographer requires a configuration file. An empty one, to serve as a starting point, is available by running `scenographer empty-config`.

After adjusting the configuration file, it's easy to start the sampling run:

```bash
scenographer bin/scenographer sample config.json
```

or if the schema doesn't need to be recreated in the target database:

```bash
scenographer bin/scenographer sample config.json --skip-schema
```

## Configuration

### SOURCE_DATABASE_URL

The connection string for the source database. Only Postgres is supported.

### TARGET_DATABASE_URL

The connection string for the target database. Only Postgres is supported.

### IGNORE_RELATIONS

Scenographer works by traversing a DAG graph created from the foreign key constraints of the database.
However, it's not always the case that the database forms a DAG. To handle those cases, some foreign keys can be ignored by adding exceptions in this form:

```python
IGNORE_RELATIONS = [
  {"pk": "product.id", "fk": "client.favorite_product_id"}
]
```

### EXTEND_RELATIONS

In other ocasions, the actual foreign key constraint is not present in the database, although it exists in the business-side of things (like Rails does it).
Additional relations can be added to handle those cases. The relations take the same format of `IGNORE_RELATIONS `.

### IGNORE_TABLES

Some tables are _extra_. They may not matter, they may require a special solution or they are part of different components. Either way, you can ignore them.

### QUERY_MODIFIERS

For some cases, it's useful to tap into the actual queries being made. For that, you can add an entry here. Here's an example:

```python
QUERY_MODIFIERS={
    "_default": {"conditions": [], "limit": 300},
    "users": {"conditions": ["email ilike '%@example.com'"]},
}
```

Each entry is a table, with the exception of `_default` which is applied to all queries. Its values can have a `conditions` and/or `limit` key. For conditions you can write plain `sql`.


### OUTPUT_DIRECTORY

At some point, the data is converted into CSV files to be imported into postgres. This is the directory for said CSV files. If you don't care about it, feel free to ignore. If it's not declared, it will create and use a temporary dictory instead.


## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
