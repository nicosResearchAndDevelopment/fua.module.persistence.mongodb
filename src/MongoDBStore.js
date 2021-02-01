const
    { EventEmitter } = require('events'),
    { MongoClient } = require('mongodb'),
    persistence = require('../../module.persistence/src/module.persistence.js'),
    { isNamedNode, isBlankNode, isLiteral, isDefaultGraph, isQuad } = persistence,
    { _assert } = require('./util.js');

// interface MongoDBStore extends DataStore {
// [x] size(): Promise<number>;
//
// [x] match(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Promise<Dataset>;
//
// [x] add(quads: Quad | Iterable<Quad>): Promise<number>;
// [ ] addStream(stream: Readable<Quad>): Promise<number>;
// [x] delete(quads: Quad | Iterable<Quad>): Promise<number>;
// [ ] deleteStream(stream: Readable<Quad>): Promise<number>;
// [x] deleteMatches(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Promise<number>;
//
// [ ] has(quads: Quad | Iterable<Quad>): Promise<boolean>;
//
// [ ] on(event: "added", callback: (quad: Quad) => void): this;
// [ ] on(event: "deleted", callback: (quad: Quad) => void): this;
// [ ] on(event: "error", callback: (err: Error) => void): this;
// };

class MongoDBStore extends EventEmitter {

    #db = null;

    constructor({ url, db, config }) {
        super();
        this.#db = new Promise((resolve, reject) => {
            MongoClient.connect(url, config, (err, client) => {
                if (err) reject(err);
                else {
                    this.#db = client.db(db);
                    resolve(this.#db);
                }
            });
        });
    } // MongoDBStore#constructor

    /**
     * @returns {Promise<number>}
     */
    async size() {
        const
            db = await this.#db,
            coll = db.collection('quads'),
            count = await coll.estimatedDocumentCount();
        return count;
    } // MongoDBStore#size

    /**
     * @param {Term} [subject]
     * @param {Term} [predicate]
     * @param {Term} [object]
     * @param {Term} [graph]
     * @returns {Promise<Dataset>}
     */
    async match(subject, predicate, object, graph) {
        const findQuery = {};
        if (subject) {
            _assert(validSubject(subject), 'MongoDBStore#match : invalid subject', TypeError);
            findQuery['subject'] = subject;
        }
        if (predicate) {
            _assert(validPredicate(predicate), 'MongoDBStore#match : invalid predicate', TypeError);
            findQuery['predicate'] = predicate;
        }
        if (object) {
            _assert(validObject(object), 'MongoDBStore#match : invalid object', TypeError);
            findQuery['object'] = object;
        }
        if (graph) {
            _assert(validGraph(graph), 'MongoDBStore#match : invalid graph', TypeError);
            findQuery['graph'] = graph;
        }

        const
            db = await this.#db,
            coll = db.collection('quads'),
            projection = { '_id': 0, 'subject': 1, 'predicate': 1, 'object': 1, 'graph': 1 },
            findCursor = await coll.find(findQuery, projection),
            dataset = persistence.dataset();

        await findCursor.forEach((quadDoc) => {
            dataset.add(persistence.fromQuad(quadDoc));
        });

        return dataset;
    } // MongoDBStore#match

    /**
     *
     * @param {Quad|Iterable<Quad>} quads
     * @returns {Promise<number>}
     */
    async add(quads) {
        /** @type {Array<Quad>} */
        const quadArr = isQuad(quads) ? [quads] : Array.isArray(quads) ? quads : Array.from(quads);
        _assert(quadArr.every(validQuad), 'MongoDBStore#add : invalid quads');

        const
            db = await this.#db,
            coll = db.collection('quads'),
            bulkQuery = quadArr.map((quad) => ({
                'updateOne': {
                    'filter': quad,
                    'update': quad,
                    'upsert': true
                }
            })),
            { upsertedCount } = await coll.bulkWrite(bulkQuery);

        return upsertedCount;
    } // MongoDBStore#add

    /**
     *
     * @param {Quad|Iterable<Quad>} quads
     * @returns {Promise<number>}
     */
    async delete(quads) {
        /** @type {Array<Quad>} */
        const quadArr = isQuad(quads) ? [quads] : Array.isArray(quads) ? quads : Array.from(quads);
        _assert(quadArr.every(validQuad), 'MongoDBStore#delete : invalid quads');

        const
            db = await this.#db,
            coll = db.collection('quads'),
            bulkQuery = quadArr.map((quad) => ({
                'deleteOne': {
                    'filter': quad
                }
            })),
            { deletedCount } = await coll.bulkWrite(bulkQuery);

        return deletedCount;
    } // MongoDBStore#delete

    /**
     * @param {Term} [subject]
     * @param {Term} [predicate]
     * @param {Term} [object]
     * @param {Term} [graph]
     * @returns {Promise<number>}
     */
    async deleteMatches(subject, predicate, object, graph) {
        const findQuery = {};
        if (subject) {
            _assert(validSubject(subject), 'MongoDBStore#deleteMatches : invalid subject', TypeError);
            findQuery['subject'] = subject;
        }
        if (predicate) {
            _assert(validPredicate(predicate), 'MongoDBStore#deleteMatches : invalid predicate', TypeError);
            findQuery['predicate'] = predicate;
        }
        if (object) {
            _assert(validObject(object), 'MongoDBStore#deleteMatches : invalid object', TypeError);
            findQuery['object'] = object;
        }
        if (graph) {
            _assert(validGraph(graph), 'MongoDBStore#deleteMatches : invalid graph', TypeError);
            findQuery['graph'] = graph;
        }

        const
            db = await this.#db,
            coll = db.collection('quads'),
            projection = { '_id': 1 },
            findCursor = await coll.find(findQuery, projection),
            bulkQuery = [];

        await findCursor.forEach((quadDoc) => {
            bulkQuery.push({
                'deleteOne': {
                    'filter': quadDoc
                }
            });
        });

        const { deletedCount } = await coll.bulkWrite(bulkQuery);
        return deletedCount;
    } // MongoDBStore#deleteMatches

} // MongoDBStore

function validSubject(term) {
    return isNamedNode(term) || isBlankNode(term);
} // validSubject

function validPredicate(term) {
    return isNamedNode(term);
} // validPredicate

function validObject(term) {
    return isNamedNode(term) || isLiteral(term) || isBlankNode(term);
} // validObject

function validGraph(term) {
    return isDefaultGraph(term) || isNamedNode(term);
} // validGraph

function validQuad(term) {
    return isQuad(term)
        && validSubject(term.subject)
        && validPredicate(term.predicate)
        && validObject(term.object)
        && validGraph(term.graph);
} // validQuad

module.exports = {
    MongoDBStore,
    validSubject, validPredicate, validObject, validGraph, validQuad
}; // exports