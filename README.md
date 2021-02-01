# module.persistence.mongodb

- [Persistence](https://git02.int.nsc.ag/Research/fua/lib/module.persistence)

## Interface

### MongoDBStore

```ts
interface MongoDBStore extends DataStore {
    size(): Promise<number>;

    match(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Promise<Dataset>;

    add(quads: Quad | Iterable<Quad>): Promise<number>;
    addStream(stream: Readable<Quad>): Promise<number>;
    delete(quads: Quad | Iterable<Quad>): Promise<number>;
    deleteStream(stream: Readable<Quad>): Promise<number>;
    deleteMatches(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Promise<number>;

    has(quads: Quad | Iterable<Quad>): Promise<boolean>;

    on(event: "added", callback: (quad: Quad) => void): this;
    on(event: "deleted", callback: (quad: Quad) => void): this;
    on(event: "error", callback: (err: Error) => void): this;
};
```

### MongoDBStoreFactory

```ts
interface MongoDBStoreFactory extends DataStoreFactory {
    dataStore({ url: string, db: string, config: Object }): MongoDBStore;
    isDataStore(that: MongoDBStore | any): true | false;

    validSubject(that: Term | any): true | false;
    validPredicate(that: Term | any): true | false;
    validObject(that: Term | any): true | false;
    validGraph(that: Term | any): true | false;
    validQuad(that: Quad | any): true | false;
};
```