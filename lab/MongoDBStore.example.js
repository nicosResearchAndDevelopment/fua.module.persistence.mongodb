const
    persistence = require('../../module.persistence/src/module.persistence.js'),
    { MongoDBStore } = require('../src/MongoDBStore.js');

(async () => {

    const store = new MongoDBStore({
        url: 'mongodb://localhost:27017/',
        db: 'MongoDBStore',
        config: {
            useUnifiedTopology: true
        }
    });

    store.on('error', (err) => console.error(err));
    store.on('added', (quad) => console.log('event.added:', quad));
    store.on('deleted', (quad) => console.log('event.deleted:', quad));

    const quads = [
        persistence.fromQuad({
            subject: { termType: 'NamedNode', value: 'ex:hello' },
            predicate: { termType: 'NamedNode', value: 'rdfs:label' },
            object: { termType: 'Literal', value: 'Hello World!', language: 'en' }
        }),
        persistence.fromQuad({
            subject: { termType: 'NamedNode', value: 'ex:hello' },
            predicate: { termType: 'NamedNode', value: 'ex:lorem' },
            object: { termType: 'BlankNode', value: '1' }
        }),
        persistence.fromQuad({
            subject: { termType: 'BlankNode', value: '1' },
            predicate: { termType: 'NamedNode', value: 'rdfs:label' },
            object: { termType: 'Literal', value: 'Lorem Ipsum' }
        })
    ];

    console.log('store.add:', await store.add(quads));
    console.log('store.match:', Array.from(await store.match(persistence.namedNode('ex:hello'))));
    console.log('store.deleteMatches:', await store.deleteMatches(null, persistence.namedNode('rdfs:label')));
    console.log('store.delete:', await store.delete(quads));

    debugger;

})().catch(console.error);