# pg_beam

Stream postgres COPY over http.

The idea is to efficiently move data between postgres DBs without cli access.

Possible use cases:

* expose `pg_stat_statements` over http: view in browser and scrape to a local postgres on an interval.
* sync data between servers.

Why not:

* Huge security risk.  No restricting of tables / columns.  No auth.  Will add examples for those, but don't use this in prod atm haha.


## Query Parameters

The only required parameter is `table`

```
/tx?table=event
```

select columns:

```
/tx?table=event&select=id,sig,kind,pubkey
```

where:

```
/tx?table=event&select=id,sig,kind,pubkey&where.kind.eq=1&where.pubkey.eq=db8dbd03a057be695f095cf053af601700eb967f592893010afbe61371dc4cd9
```


## Beaming data from another pg_beam server

First you must create a target table:

```
create table host1_event (like event including all);
```

Then you can make a request same as above, but with `host` and `to` parameters:

```
/rx?table=event&host=host1.com/tx&to=host1_event&where.id.gt=10
```


## Gotcha: generated columns

If a table has generated columns and you wish to use `select` or `where` features, either:

* omit `including all` when creating target table to convert generated columns to a plain columns
* OR omit any generated columns from the `select` column list
* OR don't use `select` or `where` query parameters... default COPY behavior will omit generated columns


## Run It

```
export DATABASE_URL=postgres://localhost:5432/my_db
go run server.go
```


## TODO

* example with some basic auth
* ability to configure allowed tables, or specify a function to validate things
* example of using in an existing server

maybe:

* ability to truncate target table before rx new data?
* fancier example client that loads new data `where.id.gt=99` cursor style
* fancier example sync client that loads to temp table, de-duplicates, copys to real table.
* order by support?
