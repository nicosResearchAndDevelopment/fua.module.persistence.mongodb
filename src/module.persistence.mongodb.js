const
    // dataFactory = require('../../module.persistence/src/module.persistence.js'),
    // datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
    MongoDBStore = require('./MongoDBStore_beta.js');

/**
 * @param {NamedNode} graph
 * @para {MongoDBDatabase} db
 * @returns {MongoDBStore}
 */
exports.store = function(graph, db) {
    return new MongoDBStore(graph, db);
};