import React from 'react';
import MatrixBackground from './components/MatrixBackground';
import Dashboard from './components/Dashboard';

function App() {
  return (
    <div className="min-h-screen bg-matrix-black text-matrix-green">
      <MatrixBackground />
      <Dashboard />
    </div>
  );
}

export default App;
