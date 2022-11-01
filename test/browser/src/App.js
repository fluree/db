import './App.css';
import flureedb from '@fluree/flureedb';

function App() {
  return (
    <ul className="App">
      {Object.keys(flureedb).sort().map(fn => <li key={fn}>{fn}</li>)}
    </ul>
  );
}

export default App;
