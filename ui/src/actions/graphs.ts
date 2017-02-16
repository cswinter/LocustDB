import { createAction } from 'redux-actions';
import * as Actions from '../constants/actions';

export const lineGraph = createAction<GraphItemData>(Actions.LINE_GRAPH);
export const pieGraph = createAction<GraphItemData>(Actions.PIE_GRAPH);
