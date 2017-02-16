import * as React from 'react';
import { bindActionCreators } from 'redux';
import { connect } from 'react-redux';
import { RootState } from '../../reducers';
import * as GraphActions from '../../actions/graphs';
import Header from '../../components/Header';
import ChartSection from '../../components/Charts';
import * as style from './style.css';

interface AppProps {
  graphs: GraphItemData[];
  actions: typeof GraphActions;
};

interface AppState {
  /* empty */
}

class App extends React.Component<AppProps, AppState>{
  render() {
    const { graphs, actions, children } = this.props;
    return (
      <div className={style.normal}>
        <Header />
        <ChartSection actions={actions} />
        {children}
      </div>
    );
  }
}

function mapStateToProps(state: RootState) {
  return {
    /* empty */
  };
}

function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(GraphActions as any, dispatch)
  };
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(App);
