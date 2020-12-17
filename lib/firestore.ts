import { Connector } from 'loopback-connector';
import {
	QuerySnapshot,
	Firestore as Admin,
	Query,
	DocumentSnapshot,
	QueryDocumentSnapshot,
	DocumentReference
} from '@google-cloud/firestore';
import { IFilter, IDataSource, ICallback } from './interfaces';
import { operators } from './config';

const initialize = function initializeDataSource(
	dataSource: IDataSource,
	callback: any
) {
	dataSource.connector = new Firestore(dataSource.settings!);
	process.nextTick(() => {
		callback();
	});
};

class Firestore extends Connector {
	public _models: any;
	public db: any;

	constructor(dataSourceProps: IDataSource) {
		super();
		this._models = {};

		const { projectId, clientEmail, privateKey } = dataSourceProps;

		const firestore = new Admin({
			credentials: {
				private_key: privateKey!.replace(/\\n/g, '\n'), // eslint-disable-line camelcase
				client_email: clientEmail // eslint-disable-line camelcase
			},
			projectId
		});

		this.db = firestore;
	}

	/**
	 * Find matching model instances by the filter
	 *
	 * @param {String} model The model name
	 * @param {Object} filter The filter
	 * @param {Function} [callback] The callback function
	 */
	public all = async (
		model: string,
		filter: IFilter,
		_options: any,
		callback: ICallback
	) => {
		const { where } = filter;

		try {
			let result: any[];
			if (where && where.id) {
				const { id } = where;
				if (id.inq) {
					const results = await Promise.all(
						id.inq.map(async (id: any) => {
							const result = await this.findById(model, id);
							return result[0];
						})
					);
					return callback(
						null,
						results.filter(
							(row: any) => typeof row.deleted === 'undefined' || !row.deleted
						)
					);
				} else {
					result = await this.findById(model, id);
				}
			} else if (this.hasFilter(filter)) {
				result = await this.findFilteredDocuments(model, filter);
			} else {
				result = await this.findAllOfModel(model);
			}

			callback(null, result);
		} catch (error) {
			callback(error);
		}
	};

	/**
	 * Check if a model instance exists by id
	 * @param {String} model The model name
	 * @param {Number | String} id The id value
	 * @param {Function} [callback] The callback function
	 *
	 */
	public exists = (
		model: string,
		id: number | string,
		_options: any,
		callback: ICallback
	) => {
		this._exists(model, id, _options, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			callback(null, res.exists);
		});
	};

	/**
	 * Count the number of instances for the given model
	 *
	 * @param {String} model The model name
	 * @param {Object} where The id Object
	 * @param {Function} [callback] The callback function
	 *
	 */
	public count = (
		model: string,
		where: any,
		_options: any,
		callback: ICallback
	) => {
		if (Object.keys(where).length > 0) {
			const query = this.buildNewQuery(model, { where });
			query
				.get()
				.then((doc: QuerySnapshot) => {
					callback(null, doc.docs.length);
				})
				.catch((err: Error) => callback(err));
		} else {
			this.db
				.collection(model)
				.where('deleted', '==', false)
				.get()
				.then((doc: QuerySnapshot) => {
					callback(null, doc.docs.length);
				})
				.catch((err: Error) => callback(err));
		}
	};

	public ping = (callback: ICallback) => {
		if (this.db.projectId) {
			callback(null);
		} else {
			callback(new Error('Ping Error'));
		}
	};

	/**
	 * Update all matching instances
	 * @param {String} model The model name
	 * @param {Object} where The search criteria
	 * @param {Object} data The property/value pairs to be updated
	 * @callback {Function} callback Callback function
	 */
	public update = (
		model: string,
		where: any,
		data: any,
		_options: any,
		callback: ICallback
	) => {
		const self = this;
		this._exists(model, where.id, null, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			if (res && res.exists) {
				// save in History first
				this.saveHistory(model, res, 'update');

				self.db
					.collection(model)
					.doc(where.id)
					.update(data)
					.then(() => {
						// Document updated successfully.
						callback(null, []);
					});
			} else {
				callback(new Error('Document not found'));
			}
		});
	};

	/**
	 * Replace properties for the model instance data
	 * @param {String} model The name of the model
	 * @param {String | Number} id The instance id
	 * @param {Object} data The model data
	 * @param {Object} options The options object
	 * @param {Function} [callback] The callback function
	 */
	public replaceById = (
		model: string,
		id: string | number,
		data: any,
		_options: any,
		callback: ICallback
	) => {
		const self = this;
		this._exists(model, id, null, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			if (res && res.exists) {
				// save in History first
				this.saveHistory(model, res, 'update-replace');

				self.db
					.collection(model)
					.doc(id)
					.update(data)
					.then(() => {
						// Document updated successfully.
						callback(null, []);
					});
			} else {
				callback(new Error('Document not found'));
			}
		});
	};

	/**
	 * Update properties for the model instance data
	 * @param {String} model The model name
	 * @param {String | Number} id The instance id
	 * @param {Object} data The model data
	 * @param {Function} [callback] The callback function
	 */
	public updateAttributes = (
		model: string,
		id: string | number,
		data: any,
		_options: any,
		callback: ICallback
	) => {
		const self = this;
		this._exists(model, id, null, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			if (res && res.exists) {
				// save in History first
				this.saveHistory(model, res, 'update-attributes');

				self.db
					.collection(model)
					.doc(id)
					.set(data)
					.then(() => {
						// Document updated successfully.
						callback(null, []);
					});
			} else {
				callback(new Error('Document not found'));
			}
		});
	};

	/**
	 * Delete a model instance by id
	 * @param {String} model The model name
	 * @param {String | Number} id The instance id
	 * @param [callback] The callback function
	 */
	public destroyById = (
		model: string,
		id: string | number,
		callback: ICallback
	) => {
		const self = this;
		this._exists(model, id, null, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			if (res && res.exists) {
				// save in History first
				this.saveHistory(model, res, 'delete');

				self.db
					.collection(model)
					.doc(id)
					// .delete()
					.set({ deleted: true }, { merge: true })
					.then(() => {
						// Document deleted successfully.
						callback(null, []);
					});
			} else {
				callback(new Error('Document not found'));
			}
		});
	};

	/**
	 * Delete a model instance
	 * @param {String} model The model name
	 * @param {Object} where The id Object
	 * @param [callback] The callback function
	 */
	public destroyAll = (model: string, where: any, callback: ICallback) => {
		const self = this;

		if (where.id) {
			this.destroyById(model, where.id, callback);
		} else {
			this.deleteCollection(this.db, model, 10)
				.then(() => {
					callback(null, '');
				})
				.catch(err => callback(err));
		}
	};

	public deleteQueryBatch = (
		db: Admin,
		query: Query,
		batchSize: number,
		resolve: any,
		reject: any
	) => {
		query
			.get()
			.then(snapshot => {
				// When there are no documents left, we are done
				if (snapshot.size == 0) {
					return 0;
				}

				// Delete documents in a batch
				const batch = db.batch();
				snapshot.docs.forEach(doc => batch.delete(doc.ref));

				return batch.commit().then(() => snapshot.size);
			})
			.then(numDeleted => {
				if (numDeleted === 0) {
					resolve();
					return;
				}

				// Recurse on the next process tick, to avoid
				// exploding the stack.
				process.nextTick(() => {
					this.deleteQueryBatch(db, query, batchSize, resolve, reject);
				});
			})
			.catch(reject);
	};

	public create = (model: string, data: any, callback: ICallback) => {
		this.db
			.collection(model)
			.add({
				...data,
				deleted: false
			})
			.then((ref: DocumentReference) => {
				callback(null, ref.id);
			})
			.catch((err: Error) => {
				callback(err);
			});
	};

	public updateAll = (
		model: string,
		where: any,
		data: any,
		_options: any,
		callback: ICallback
	) => {
		const self = this;
		this._exists(model, where.id, null, (err, res: DocumentSnapshot) => {
			if (err) callback(err);
			if (res && res.exists) {
				// save in History first
				this.saveHistory(model, res, 'update');

				self.db
					.collection(model)
					.doc(where.id)
					.update(data)
					.then(() => {
						// Document updated successfully.
						callback(null, []);
					});
			} else {
				callback(new Error('Document not found'));
			}
		});
	};

	/**
	 * Complete the Document objects with their ids
	 * @param {DocumentSnapshot[] | QueryDocumentSnapshot[]} snapshots The array of snapshots
	 */
	private completeDocumentResults = (
		snapshots: DocumentSnapshot[] | QueryDocumentSnapshot[]
	) => {
		const results: any[] = [];

		snapshots.forEach(item => {
			const data: any = item.data();
			delete data.deleted;

			results.push({
				id: item.id,
				...data
			});
		});

		return results;
	};

	private saveHistory = async (
		model: string,
		documentSnapshot: DocumentSnapshot,
		operationType: string
	) => {
		const ref = this.db.collection(model);
		return await ref
			.doc(documentSnapshot.id)
			.collection(`${model}History`)
			.doc()
			.set({
				...documentSnapshot.data(),
				operation_type: operationType, // eslint-disable-line camelcase
				operation_date: new Date().getTime() / 1000 // eslint-disable-line camelcase
			});
	};

	/**
	 * Internal method to get documents by history
	 * retrieve the last snapshot of the document based on the operation_date
	 *
	 * @param string model
	 * @param {IFilter} filter The filter
	 */
	private getFromHistoryQuery = async (model: string, filter: IFilter) => {
		const query = this.buildNewQuery(`${model}History`, filter);
		const result = await query.get();
		const groups: { [key: string]: any } = {};
		result.forEach((doc: any) => {
			const data = doc.data();
			const id = doc.id;
			delete data.deleted;
			if (typeof groups[id] === 'undefined') {
				groups[id] = data;
			} else if (
				typeof groups[id].operation_date !== 'undefined' &&
				typeof data.operation_date !== 'undefined' &&
				data.operation_date > groups[id].operation_date
			) {
				groups[id] = data;
			}
		});

		return Object.keys(groups).map(id => {
			return { id, ...groups[id] };
		});
	};

	/**
	 * Internal method - Check if filter object has at least one valid property
	 * @param {IFilter} filter The filter
	 */
	private hasFilter = ({ where, order, limit, fields, skip }: IFilter) => {
		if (where || limit || fields || order || skip) return true;
		return false;
	};

	/**
	 * Find matching Collection Document by the id
	 * @param {String} model The model name
	 * @param {String} id The Entity id
	 */
	private findById = async (model: string, id: string) => {
		try {
			const documentSnapshot = await this.db
				.collection(model)
				.doc(id)
				.get();
			if (!documentSnapshot.exists) return Promise.resolve([]);

			const data = documentSnapshot.data();
			delete data.deleted;
			const result = { id: documentSnapshot.id, ...data };

			return Promise.resolve([result]);
		} catch (error) {
			throw error;
		}
	};

	/**
	 * Find all Documents of a Collection
	 * @param {String} model The model name
	 */
	private findAllOfModel = async (model: string) => {
		try {
			const collectionRef = this.db
				.collection(model)
				.where('deleted', '==', false);
			const snapshots = await collectionRef.get();

			if (snapshots.empty || snapshots.size === 0) return Promise.resolve([]);

			const result = this.completeDocumentResults(snapshots.docs);

			return Promise.resolve(result);
		} catch (error) {
			throw error;
		}
	};

	/**
	 * Internal method - Get Documents with query execution
	 * @param {String} model The model name
	 * @param {IFilter} filter The filter
	 */
	private findFilteredDocuments = async (
		model: string,
		filter: IFilter
	): Promise<any> => {
		if (
			filter.where &&
			filter.where.and &&
			Array.isArray(filter.where.and) &&
			filter.where.and.length > 0
		) {
			let newWhereCondition: any = {};
			filter.where.and.forEach((val: any) => {
				newWhereCondition = Object.assign(newWhereCondition, val);
			});
			const newFilter = Object.assign(filter, { where: newWhereCondition });
			return await this.findFilteredDocuments(model, newFilter);
		}

		if (
			filter.where &&
			filter.where.or &&
			Array.isArray(filter.where.or) &&
			filter.where.or.length > 0
		) {
			let results: any[] = [];
			await Promise.all(
				filter.where.or.map(async (val: any) => {
					const newFilter = Object.assign(filter, { where: val });
					const filterResult = await this.findFilteredDocuments(
						model,
						newFilter
					);
					results = [...results, ...filterResult];
				})
			);
			if (results.length > 0) {
				const uniqueArray = Array.from(new Set(results.map(val => val.id))).map(
					id => {
						return results.find(val => val.id === id);
					}
				);
				return uniqueArray;
			}
			return results;
		}
		if (filter.where && Object.keys(filter.where).length) {
			for (const key in filter.where) {
				if (Array.isArray(filter.where[key].inq || filter.where[key].in)) {
					const array = filter.where[key].inq || filter.where[key].in;
					if (array.length > 10) {
						const promises = [];
						for (let i = 0; i <= array.length / 10; i++) {
							const ids = array.slice(i * 10, (i + 1) * 10);

							let query = null;
							if (model.indexOf('History') > 0) {
								query = this.db.collectionGroup(model);
							} else {
								query = this.db.collection(model);
							}
							query = query.where('deleted', '==', false).where(key, 'in', ids);
							promises.push(query.get());
						}

						return Promise.all(promises).then(batches => {
							let res: any[] = [];
							batches.forEach(batch => {
								res = res.concat(this.completeDocumentResults(batch));
							});

							return res;
						});
					}
				}
			}
		}
		const query = this.buildNewQuery(model, filter);
		const snapshots = await query.get();

		return this.completeDocumentResults(snapshots);
	};

	/**
	 * Internal method for building query
	 * @param {String} model The model name
	 * @param {IFilter} filter The filter
	 */
	private buildNewQuery = (model: string, filter: IFilter) => {
		const { where, limit, fields, skip } = filter;

		let query = null;
		if (model.indexOf('History') > 0) {
			query = this.db.collectionGroup(model);
		} else {
			query = this.db.collection(model);
		}

		if (where) {
			for (const key in where) {
				if (where.hasOwnProperty(key)) {
					const value = { [key]: where[key] };
					query = this.addFiltersToQuery(query, value);
				}
			}
			if (typeof where.deleted === 'undefined') {
				query = query.where('deleted', '==', false);
			}
		} else {
			query = query.where('deleted', '==', false);
		}

		let { order } = filter;
		if (order) {
			if (!Array.isArray(order)) {
				order = [order];
			}

			for (const option of order) {
				const [property, orderOption] = option.split(' ');
				query = query.orderBy(property, orderOption);
			}
		}

		if (limit) {
			query = query.limit(limit);
		}

		if (skip) {
			query = query.offset(skip);
		}

		if (fields) {
			for (const key in fields) {
				if (fields.hasOwnProperty(key)) {
					const field = fields[key];
					if (field) query = query.select(key);
				}
			}
		}

		return query;
	};

	/**
	 * Add new filter to a Query
	 * @param {Query} query Firestore Query
	 * @param {Object} filter The filter object
	 */
	private addFiltersToQuery = (query: Query, filter: IFilter) => {
		const key = Object.keys(filter)[0];
		const value = Object.values(filter)[0];

		const isObject = typeof value === 'object';

		if (isObject) {
			return this.addInnerFiltersToQuery(query, key, value);
		}

		return query.where(key, '==', value);
	};

	/**
	 * Add inner filters to a Query
	 * @param {Query} query Firestore Query
	 * @param {String} key Property name being filtered
	 * @param {Object} value Object with operator and comparison value
	 */
	private addInnerFiltersToQuery = (query: Query, key: string, value: any) => {
		let resultQuery = query;

		for (const operation in value) {
			if (!value.hasOwnProperty(operation)) {
				continue;
			}
			const comparison = value[operation];
			const operator = operators[operation];
			if (operation === 'between') {
				resultQuery = resultQuery
					.where(key, '>=', comparison[0])
					.where(key, '<=', comparison[1]);
			} else {
				resultQuery = resultQuery.where(key, operator, comparison);
			}
		}

		return resultQuery;
	};

	private deleteCollection = (
		db: Admin,
		collectionPath: string,
		batchSize: number
	) => {
		const collectionRef = db.collection(collectionPath);
		const query = collectionRef.orderBy('__name__').limit(batchSize);

		return new Promise((resolve, reject) => {
			this.deleteQueryBatch(db, query, batchSize, resolve, reject);
		});
	};

	private _exists = (
		model: string,
		id: number | string,
		_options: any,
		callback: ICallback
	) => {
		this.db
			.collection(model)
			.doc(id)
			.get()
			.then((doc: DocumentSnapshot) => {
				callback(null, doc);
			})
			.catch((err: Error) => callback(err));
	};
}

export { Firestore, initialize };
