import './App.css';
import flureedb from '@fluree/fluree-browser-sdk';

function App() {
  return (
    <ul className="App">
      {Object.keys(flureedb).sort().map(fn => <li key={fn}>{fn}</li>)}
    </ul>
  );
}

export default App;
