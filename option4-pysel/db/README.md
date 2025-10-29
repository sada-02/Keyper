# Database package

This package contains supported databases. Currently, it includes the following
databases:

* levelDB

## Custom database

To add a custom database, you need to implement the following interface for it:

```go
type DB interface {
 Get(key []byte) ([]byte, error)
 Set(key []byte, value []byte) error
 Delete(key []byte) error
 Has(key []byte) bool
 Close() error
}
```

in a new file in the `db` package. Then, you need to use this database in either the balancer or partition packages.

For reference, view the `leveldb` package.
