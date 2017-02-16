import { routerReducer as routing, RouteActions } from 'react-router-redux';
import { combineReducers, Reducer } from 'redux';
import handleGraphActions from './graphs';

export interface RootState {
  routing: RouteActions;
  graphs: GraphStoreState;
}

export default combineReducers<RootState>({
  routing,
  handleGraphActions
});
