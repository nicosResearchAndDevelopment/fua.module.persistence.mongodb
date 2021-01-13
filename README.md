# module.persistence.mongodb

- [Persistence](https://git02.int.nsc.ag/Research/fua/lib/module.persistence)

## Interface

```ts
interface MongoDBStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, db: MongoDBDatabase): MongoDBStore;
};
```