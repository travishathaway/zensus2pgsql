<style>
.md-content .md-typeset h1 { display: none; }
</style>

<p align="center">
  <img src="./assets/zensus2pgsql-logo.svg" width="50%">
</p>
<p align="center">
  <em>German census data, now in PostgreSQL!</em>
</p>

<p align="center">
  <a href="https://github.com/travishathaway/zensus2pgsql/actions?query=workflow%3ATest" target="_blank">
      <img src="https://github.com/travishathaway/zensus2pgsql/workflows/Test/badge.svg" alt="Test">
  </a>
  <a href="https://travishathaway.github.io/zensus2pgsql" target="_blank">
      <img src="https://img.shields.io/static/v1?label=Documentation&message=View&color=blue&logo=readme&logoColor=white" alt="Documentation">
  </a>
  <a href="https://pypi.org/project/zensus2pgsql" target="_blank">
    <img src="https://img.shields.io/pypi/v/zensus2pgsql?color=%2334D058&label=pypi%20package" alt="Package version">
  </a>
</p>

# zensus2pgsql

A CLI program that imports German census CSV data (spelled "zensus" in German) into PostgreSQL
with PostGIS geographic data types. All data is provided by the Statitisches Bundesamt of 
Germany. Currently, only data from the 2022 census is available.

If you want to download the raw data yourself or see the website it comes from, check out the
link below:

- [Zensus 2022 Publikationen](https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Zensus2022/_publikationen.html)

## Installing

To install this program, run:

```sh
pip install zensus2pgsql
```

## Using

To get started using this tool, you'll first want to see what datasets are availble:

```cli
zensus2pgsql list
```

For now, the names of the data sets are only available in German, so please use translation
software if you need help figuring out what they mean!

After you've fond a dataset to import, you can do so by running the following command:

```cli
zensus2pgsql create \
  --host localhost \
  --port 5432 \
  --user username \
  --schema public \
  heizungsart eigentuemerquote
```

The above command will import two dataset related to the type of heating a house uses and the
percentage of those who own their home in a particular area.

## Contributing

Contributions are welcome and can ake the following forms:

- Creating a bug report if you've found an issue
- Creating a feature request if you'd like to see new functionality added
- Creating a pull request to accomplish either of the two above
- Leaving a star on the repository 

Before creating a pull request, I kindly ask that you either create a bug report or
a feature request first, so I can sign off on having it added to the project.
