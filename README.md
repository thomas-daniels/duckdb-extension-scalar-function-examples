# Scalar function examples in DuckDB extensions

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

Contains examples of implementing scalar functions in DuckDB extensions. [Take a look at my blog post!](https://thomasd.be/2025/03/16/duckdb-extension-scalar-functions.html)


## Building

### Build steps

To build the extension, run:
```sh
make
```

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB, for example:

```
D select quack('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Quack Jane 🐥 |
└───────────────┘
```