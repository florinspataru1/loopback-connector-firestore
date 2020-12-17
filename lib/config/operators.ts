import { WhereFilterOp } from '@google-cloud/firestore';

const operators: { [key: string]: WhereFilterOp } = {
	lt: '<',
	lte: '<=',
	gt: '>',
	gte: '>=',
	in: 'in',
	eq: '==',
	inq: 'in',
	nin: 'array-contains-any'
};

export default Object.freeze(operators);
