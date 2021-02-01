const
    { MongoDBStore } = require('./MongoDBStore.js'),
    { _assert } = require('./util.js');

function isDataStore(that) {
    return that instanceof MongoDBStore;
} // isDataStore

/**
 * @param {Object} options
 * @param {string} options.url
 * @param {string} options.db
 * @param {Object} [options.config]
 * @returns {MongoDBStore}
 */
function dataStore(options) {
    return new MongoDBStore(options);
} // dataStore

module.exports = {
    dataStore, isDataStore
}; // exports