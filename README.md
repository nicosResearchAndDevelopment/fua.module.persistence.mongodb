# module.persistence.mongodb

- [Persistence](../module.persistence)

## Interface

```ts
interface MongoDBStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, db: MongoDBDatabase): MongoDBStore;
};
```