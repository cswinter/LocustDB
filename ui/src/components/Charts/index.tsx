import * as React from 'react';
import * as GraphActions from '../../actions/graphs';
import { connect } from 'react-redux';
import * as ReactHighcharts from 'react-highcharts';
import * as style from './style.css';

interface ChartSectionProps {
  actions: typeof GraphActions;
};

interface ChartSectionState {
  value: number;
};

class ChartSection extends React.Component<ChartSectionProps, ChartSectionState> {

  constructor(props?: ChartSectionProps, context?: any) {
    super(props, context);
    this.state = { value: 1 };
  }

  render() {
    const { actions } = this.props;
    const { value } = this.state;

    const config = {
       title: {
        text: 'Dropbox new team over last month'
    },

    subtitle: {
        text: 'compare month over month'
    },

    xAxis: {
        title: {
            text: 'Day of the month'
        }
    },

    yAxis: {
        title: {
            text: 'New Teams'
        }
    },

    legend: {
        layout: 'vertical',
        align: 'right',
        verticalAlign: 'middle'
    },

    plotOptions: {
        series: {
            pointStart: 1
        }
    },

    series: [{
        name: 'Feb 2017',
        data: [400, 456, 478, 510, 499, 476, 489, 495, 501, 510, 508, 516, 536, 540, 548, 557, 578, 610, 599, 576, 689, 695, 701, 710, 708, 716, 736, 740],
    }, {
        name: 'Jan 2017',
        data: [450, 476, 488, 500, 510, 466, 469, 485, 500, 500, 508, 526, 546, 560, 578, 587, 598, 600, 610, 636, 679, 685, 701, 700, 704, 726, 746, 750],
    }]

    }

    return (
      <section>
        <ReactHighcharts config = {config} /> 
      </section>
    );
  }
}

export default ChartSection;
