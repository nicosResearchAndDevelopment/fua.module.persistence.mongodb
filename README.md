# module.persistence.mongodb

## Interface

```ts
interface MongoDBStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, db: MongoDBDatabase): MongoDBStore;
};
```