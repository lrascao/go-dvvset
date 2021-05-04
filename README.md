# go-dvvset

> Dotted Version Vectors Sets Go implementation

Go version of the [Dotted Version Vector Sets](https://github.com/ricardobcl/Dotted-Version-Vectors) Erlang reference implementation.

## Examples

### Concurrent writes

```Go
// Client 1 writes `v1` at node `n1`
c1 := dvvset.New("v1")
c1.Update("n1")

// Client2 writes `v2` at node `n2` with no causal context
c2 := dvvset.New("v2")
c2.Update("n2")

// when replication occurs both nodes will observe the concurrent write
s := Sync([]Clock{c1, c2})
fmt.Printf("s values: %v\n", s.Values())
```
```Shell
$ s values: [v1 v2]
```

### Read before write

```Go
// Client 1 writes `v1` at node `n1`
c1 := dvvset.New("v1")
c1.Update("n1")

// Client2 reads and then writes `v2` at node `n2`
c2 := dvvset.NewWithContext(c1.Join(), "v2")
c2.Update("n2")

// The `v2` write causally descends from `v1` write
s := Sync([]Clock{c1, c2})
fmt.Printf("s values: %v\n", s.Values())
```
```Shell
$ s values: [v2]
```

