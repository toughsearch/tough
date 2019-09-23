# Tough Search

## Install

```bash
$ git clone https://github.com/toughsearch/tough.git
$ cd tough
$ pip3 install -r requirements.txt
```

## Configure

```bash
$ cp conf-example.yaml conf.yaml
```

And then adjust `conf.yaml` according to your file structure and date format.

## Create index

```bash
$ python3 -m tough reindex <index_name>
```

## Search

Let's say you want to find all requests to URL `/foobar` that came from March 5 to March 7, 2019 in your nginx logs:

```bash
$ python3 -m tough search -df 2019-03-05 -dt 2019-03-07 '/foobar' <index_name>
```

Or, maybe, find `/foobar` and `/foobaz` with regex `/fooba[rz]`:

```bash
$ python3 -m tough search -df 2019-03-05 -dt 2019-03-07 -e '/fooba[rz]' <index_name>
```
