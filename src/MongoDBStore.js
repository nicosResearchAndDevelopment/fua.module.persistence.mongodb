const
    { EventEmitter } = require('events'),
    { MongoClient } = require('mongodb'),
    persistence = require('../../module.persistence/src/module.persistence.js'),
    { isNamedNode, isBlankNode, isLiteral, isDefaultGraph, isQuad } = persistence,
    { _assert } = require('./util.js'),
    _quadToDoc = (quad) => ({
        subject: quad.subject,
        predicate: quad.predicate,
        object: quad.object,
        graph: quad.graph
    }),
    _docToQuad = (quadDoc) => persistence.fromQuad(quadDoc);

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
        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                count = await coll.estimatedDocumentCount();
            return count;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
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
            findQuery.subject = subject;
        }
        if (predicate) {
            _assert(validPredicate(predicate), 'MongoDBStore#match : invalid predicate', TypeError);
            findQuery.predicate = predicate;
        }
        if (object) {
            _assert(validObject(object), 'MongoDBStore#match : invalid object', TypeError);
            findQuery.object = object;
        }
        if (graph) {
            _assert(validGraph(graph), 'MongoDBStore#match : invalid graph', TypeError);
            findQuery.graph = graph;
        }

        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                projection = { _id: false, subject: true, predicate: true, object: true, graph: true },
                findCursor = await coll.find(findQuery, projection),
                dataset = persistence.dataset();

            await findCursor.forEach((quadDoc) => {
                dataset.add(persistence.fromQuad(quadDoc));
            });

            return dataset;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#match

    /**
     * @param {Quad|Iterable<Quad>} quads
     * @returns {Promise<number>}
     */
    async add(quads) {
        /** @type {Array<Quad>} */
        const quadArr = isQuad(quads) ? [quads] : Array.isArray(quads) ? quads : Array.from(quads);
        _assert(quadArr.every(validQuad), 'MongoDBStore#add : invalid quads', TypeError);

        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                bulkQuery = quadArr.map((quad) => {
                    const quadDoc = _quadToDoc(quad);
                    return {
                        updateOne: {
                            filter: quadDoc,
                            update: { $setOnInsert: quadDoc },
                            upsert: true
                        }
                    };
                }),
                { result } = await coll.bulkWrite(bulkQuery);

            for (let { index } of result.upserted) {
                this.emit('added', quadArr[index]);
            }

            return result.nUpserted;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#add

    /**
     * @param {Readable<Quad>} stream
     * @returns {Promise<number>}
     */
    async addStream(stream) {
        const quads = [];
        await new Promise((resolve) => {
            stream.on('data', quad => quads.push(quad));
            stream.on('end', resolve);
        });
        return this.add(quads);
    } // MongoDBStore#addStream

    /**
     * @param {Quad|Iterable<Quad>} quads
     * @returns {Promise<number>}
     */
    async delete(quads) {
        /** @type {Array<Quad>} */
        const quadArr = isQuad(quads) ? [quads] : Array.isArray(quads) ? quads : Array.from(quads);
        _assert(quadArr.every(validQuad), 'MongoDBStore#delete : invalid quads', TypeError);

        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                // REM: bulkWrite would be more efficient, but you cannot get the deleted quads
                resultArr = await Promise.all(quadArr.map(
                    (quad) => coll.findOneAndDelete(_quadToDoc(quad))
                ));

            let deleted = 0;
            resultArr.forEach((result, index) => {
                if (result.value) {
                    deleted++;
                    this.emit('deleted', quadArr[index]);
                }
            });

            return deleted;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#delete

    /**
     * @param {Readable<Quad>} stream
     * @returns {Promise<number>}
     */
    async deleteStream(stream) {
        const quads = [];
        await new Promise((resolve) => {
            stream.on('data', quad => quads.push(quad));
            stream.on('end', resolve);
        });
        return this.delete(quads);
    } // MongoDBStore#deleteStream

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
            findQuery.subject = subject;
        }
        if (predicate) {
            _assert(validPredicate(predicate), 'MongoDBStore#deleteMatches : invalid predicate', TypeError);
            findQuery.predicate = predicate;
        }
        if (object) {
            _assert(validObject(object), 'MongoDBStore#deleteMatches : invalid object', TypeError);
            findQuery.object = object;
        }
        if (graph) {
            _assert(validGraph(graph), 'MongoDBStore#deleteMatches : invalid graph', TypeError);
            findQuery.graph = graph;
        }

        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                projection = { _id: true, subject: true, predicate: true, object: true, graph: true },
                findCursor = await coll.find(findQuery, projection),
                bulkQuery = [], quads = [];

            await findCursor.forEach((quadDoc) => {
                quads.push(_docToQuad(quadDoc));
                bulkQuery.push({
                    deleteOne: {
                        filter: { _id: quadDoc._id }
                    }
                });
            });

            const { result } = await coll.bulkWrite(bulkQuery);
            for (let quad of quads) {
                this.emit('deleted', quad);
            }

            return result.nRemoved;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#deleteMatches

    /**
     * @param {Quad|Iterable<Quad>} quads
     * @returns {Promise<boolean>}
     */
    async has(quads) {
        /** @type {Array<Quad>} */
        const quadArr = isQuad(quads) ? [quads] : Array.isArray(quads) ? quads : Array.from(quads);
        _assert(quadArr.every(validQuad), 'MongoDBStore#has : invalid quads', TypeError);

        try {
            const
                db = await this.#db,
                coll = db.collection('quads'),
                counts = await Promise.all(quadArr.map(
                    (quad) => coll.countDocuments(_quadToDoc(quad), { limit: 1 })
                ));

            return counts.every(val => val > 0);
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#has

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