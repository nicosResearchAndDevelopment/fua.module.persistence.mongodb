const
    util          = require('@nrd/fua.core.util'),
    assert        = new util.Assert('module.persistence.mongodb'),
    {MongoClient} = require('mongodb'),
    {DataStore}   = require('@nrd/fua.module.persistence');

class MongoDBStore extends DataStore {

    #db = null;

    constructor(options, factory) {
        super(options, factory);
        const {url, db, config} = options;
        assert(util.isString(url), 'MongoDBStore#constructor : invalid url');
        assert(util.isString(db), 'MongoDBStore#constructor : invalid db');
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

    async size() {
        try {
            const
                db    = await this.#db,
                coll  = db.collection('quads'),
                count = await coll.estimatedDocumentCount();
            return count;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#size

    async match(subject, predicate, object, graph) {
        const
            dataset   = await super.match(subject, predicate, object, graph),
            findQuery = {};

        if (subject) findQuery.subject = subject;
        if (predicate) findQuery.predicate = predicate;
        if (object) findQuery.object = object;
        if (graph) findQuery.graph = graph;

        try {
            const
                db         = await this.#db,
                coll       = db.collection('quads'),
                projection = {_id: false, subject: true, predicate: true, object: true, graph: true},
                findCursor = await coll.find(findQuery, projection);

            await findCursor.forEach((quadDoc) => {
                const quad = this.factory.fromQuad(quadDoc);
                dataset.add(quad);
            });

            return dataset;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#match

    async add(quads) {
        const
            quadArr = await super.add(quads);

        try {
            const
                db        = await this.#db,
                coll      = db.collection('quads'),
                bulkQuery = quadArr.map((quad) => {
                    const quadDoc = {
                        subject:   quad.subject,
                        predicate: quad.predicate,
                        object:    quad.object,
                        graph:     quad.graph
                    };
                    return {
                        updateOne: {
                            filter: quadDoc,
                            update: {$setOnInsert: quadDoc},
                            upsert: true
                        }
                    };
                }),
                {result}  = await coll.bulkWrite(bulkQuery);

            for (let {index} of result.upserted) {
                this.emit('added', quadArr[index]);
            }

            return result.nUpserted;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#add

    async addStream(stream) {
        const
            quadStream = await super.addStream(stream),
            quadArr    = [];

        await new Promise((resolve) => {
            quadStream.on('data', quad => quadArr.push(quad));
            quadStream.on('end', resolve);
        });

        return this.add(quadArr);
    } // MongoDBStore#addStream

    async delete(quads) {
        const quadArr = await super.delete(quads);

        try {
            const
                db        = await this.#db,
                coll      = db.collection('quads'),
                // REM: bulkWrite would be more efficient, but you cannot get the deleted quads
                resultArr = await Promise.all(quadArr.map((quad) => {
                    const quadDoc = {
                        subject:   quad.subject,
                        predicate: quad.predicate,
                        object:    quad.object,
                        graph:     quad.graph
                    };
                    return coll.findOneAndDelete(quadDoc)
                }));

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

    async deleteStream(stream) {
        const
            quadStream = await super.deleteStream(stream),
            quadArr    = [];

        await new Promise((resolve) => {
            quadStream.on('data', quad => quadArr.push(quad));
            quadStream.on('end', resolve);
        });

        return this.delete(quadArr);
    } // MongoDBStore#deleteStream

    async deleteMatches(subject, predicate, object, graph) {
        await super.deleteMatches(subject, predicate, object, graph);
        const
            findQuery = {};

        if (subject) findQuery.subject = subject;
        if (predicate) findQuery.predicate = predicate;
        if (object) findQuery.object = object;
        if (graph) findQuery.graph = graph;

        try {
            const
                db                    = await this.#db,
                coll                  = db.collection('quads'),
                projection            = {_id: true, subject: true, predicate: true, object: true, graph: true},
                findCursor            = await coll.find(findQuery, projection),
                bulkQuery = [], quads = [];

            await findCursor.forEach((quadDoc) => {
                quads.push(this.factory.fromQuad(quadDoc));
                bulkQuery.push({
                    deleteOne: {
                        filter: {_id: quadDoc._id}
                    }
                });
            });

            const {result} = await coll.bulkWrite(bulkQuery);
            for (let quad of quads) {
                this.emit('deleted', quad);
            }

            return result.nRemoved;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#deleteMatches

    async has(quads) {
        const
            quadArr = await super.has(quads);

        try {
            const
                db     = await this.#db,
                coll   = db.collection('quads'),
                counts = await Promise.all(quadArr.map((quad) => {
                    const quadDoc = {
                        subject:   quad.subject,
                        predicate: quad.predicate,
                        object:    quad.object,
                        graph:     quad.graph
                    };
                    return coll.countDocuments(quadDoc, {limit: 1})
                }));

            return counts.every(val => val > 0);
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // MongoDBStore#has

    async createIndex() {
        const
            db   = await this.#db,
            coll = db.collection('quads');

        await Promise.all([
            coll.createIndex(
                {'subject': 1},
                {'name': 'SubjectIndex'}
            ),
            coll.createIndex(
                {'predicate': 1},
                {'name': 'PredicateIndex'}
            ),
            coll.createIndex(
                {'object': 1},
                {'name': 'ObjectIndex'}
            ),
            coll.createIndex(
                {'graph': 1},
                {'name': 'GraphIndex'}
            ),
            coll.createIndex(
                {'subject': 1, 'predicate': 1, 'object': 1, 'graph': 1},
                {'name': 'QuadIndex', 'unique': true}
            )
        ]);
    } // MongoDBStore#createIndex

} // MongoDBStore

module.exports = MongoDBStore;
