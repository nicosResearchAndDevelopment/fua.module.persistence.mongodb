const
    { MongoClient } = require('mongodb'),
    {
        validSubject, validPredicate, validObject, validGraph, validQuad
    } = require('./MongoDBStore.js'),
    {
        dataStore, isDataStore
    } = require('./MongoDBStoreFactory.js');

async function buildIndex(options) {
    const
        client = await MongoClient.connect(options.url, options.config),
        db = client.db(options.db),
        coll = db.collection('quads');

    await Promise.all([
        coll.createIndex(
            { 'subject': 1 },
            { 'name': 'SubjectIndex' }
        ),
        coll.createIndex(
            { 'predicate': 1 },
            { 'name': 'PredicateIndex' }
        ),
        coll.createIndex(
            { 'object': 1 },
            { 'name': 'ObjectIndex' }
        ),
        coll.createIndex(
            { 'graph': 1 },
            { 'name': 'GraphIndex' }
        ),
        coll.createIndex(
            { 'subject': 1, 'predicate': 1, 'object': 1, 'graph': 1 },
            { 'name': 'QuadIndex', 'unique': true }
        )
    ]);
} // buildIndex

module.exports = {
    dataStore, isDataStore,
    validSubject, validPredicate, validObject, validGraph, validQuad,
    buildIndex
}; // exports