const
	module_name = 'module.rdf.MongoDBStore',
	{ EventEmitter } = require('events'),
	// { MongoClient } = require('mongodb'),
	dataFactory = require('../../module.persistence/src/module.persistence.js'),
	datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
	{ isQuad, isNamedNode, isLiteral, isSubject, isPredicate, isObject } = dataFactory;

// TODO the amount of stored data can be reduced, if terms got stored separately

class MongoDBStore extends EventEmitter {

	#graph = null;
	#db = null;

	constructor(graph, db) {
		if (!isNamedNode(graph))
			throw new Error(`${module_name}#constructor : invalid graph`);
		if (!(db && typeof db.collection === 'function'))
			throw new Error(`${module_name}#constructor : invalid db`);

		super();
		this.#graph = graph;
		this.#db = db;
	}

	/**
	 * @returns {Promise<Number>} the number of quads
	 */
	async size() {
		const
			col = this.#db.collection('tripel'),
			count = await col.estimatedDocumentCount();

		return count;
	} // Neo4jStore#size

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>} true, if an actual quad got added
	 */
	async add(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#add : invalid quad`);

		const
			col = this.#db.collection('tripel'),
			query = { subject: quad.subject, predicate: quad.predicate, object: quad.object },
			{ upsertedCount } = await col.updateOne(query, { $setOnInsert: query }, { upsert: true });

		return upsertedCount > 0;
	} // Neo4jStore#add

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>} true, if an actual quad got deleted
	 */
	async delete(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#delete : invalid quad`);

		const
			col = this.#db.collection('tripel'),
			query = { subject: quad.subject, predicate: quad.predicate, object: quad.object },
			{ deletedCount } = await col.deleteOne(query);

		return deletedCount > 0;
	} // Neo4jStore#delete

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>} true, if the store contains the quad
	 */
	async has(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#has : invalid quad`);

		const
			col = this.#db.collection('tripel'),
			query = { subject: quad.subject, predicate: quad.predicate, object: quad.object },
			existedCount = await col.countDocuments(query, { limit: 1 });

		return existedCount > 0;
	} // Neo4jStore#has

	/**
	 * @param {Term} [subject]
	 * @param {Term} [predicate]
	 * @param {Term} [object]
	 * @returns {Promise<Dataset>} new dataset with matching quads
	 */
	async match(subject, predicate, object) {
		const query = {};

		if (subject) {
			if (!isSubject(subject))
				throw new Error(`${module_name}#match : invalid subject`);
			query.subject = subject;
		}
		if (predicate) {
			if (!isPredicate(predicate))
				throw new Error(`${module_name}#match : invalid predicate`);
			query.predicate = predicate;
		}
		if (object) {
			if (object && !isObject(object))
				throw new Error(`${module_name}#match : invalid object`);
			query.object = object;
		}

		const
			col = this.#db.collection('tripel'),
			findCursor = await col.find(query, { projection: { subject: 1, predicate: 1, object: 1 } }),
			dataset = datasetFactory.dataset();

		await findCursor.forEach((quadDoc) => {
			quadDoc.graph = this.#graph;
			dataset.add(dataFactory.fromQuad(quadDoc));
		});

		return dataset;
	} // Neo4jStore#match

} // MongoDBStore

module.exports = MongoDBStore;