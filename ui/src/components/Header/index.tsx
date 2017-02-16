import * as React from 'react';

interface HeaderProps {
  /* empty */
}

interface HeaderState {
  /* empty */
}

class Header extends React.Component<HeaderProps, HeaderState> {

  constructor(props?: HeaderProps, context?: any) {
    super(props, context);
  }

  render() {
    return (
      <header>
        <h1>Ruba</h1>
      </header>
    );
  }
}

export default Header;
