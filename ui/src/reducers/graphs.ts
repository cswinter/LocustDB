import { handleActions } from 'redux-actions';
import * as Actions from '../constants/actions';

const initialState: GraphStoreState = [{
  id: 0,
  graph_type: 1,
  /** add graph source data **/
}];

export default handleActions<GraphStoreState, GraphItemData>({
  [Actions.LINE_GRAPH]: (state, action) => {
    return initialState;
  },

  [Actions.PIE_GRAPH]: (state, action) => {
    return initialState;
  },
}, initialState);
