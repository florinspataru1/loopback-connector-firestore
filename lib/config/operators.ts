import { WhereFilterOp } from '@google-cloud/firestore';

const operators: { [key: string]: WhereFilterOp } = {
	lt: '<',
	lte: '<=',
	gt: '>',
	gte: '>=',
	in: 'in',
	eq: '==',
	inq: 'in'
};

export default Object.freeze(operators);
