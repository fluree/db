import { render, screen } from '@testing-library/react';
import App from './App';

test('lists flureedb fns', () => {
  const {asFragment} = render(<App />);
  expect(asFragment()).toMatchSnapshot();
});
