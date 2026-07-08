import React, { useEffect, useState } from 'react';

const MATRIX_CHARS = 'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

interface MatrixChar {
  id: number;
  x: number;
  char: string;
  speed: number;
  opacity: number;
}

const MatrixBackground: React.FC = () => {
  const [chars, setChars] = useState<MatrixChar[]>([]);

  useEffect(() => {
    const numColumns = Math.floor(window.innerWidth / 20);
    const initialChars: MatrixChar[] = [];

    for (let i = 0; i < numColumns; i++) {
      for (let j = 0; j < 3; j++) {
        initialChars.push({
          id: i * 3 + j,
          x: i * 20,
          char: MATRIX_CHARS[Math.floor(Math.random() * MATRIX_CHARS.length)],
          speed: 1 + Math.random() * 3,
          opacity: Math.random() * 0.5 + 0.1,
        });
      }
    }

    setChars(initialChars);

    const interval = setInterval(() => {
      setChars(prevChars =>
        prevChars.map(char => ({
          ...char,
          char: Math.random() > 0.95 ? MATRIX_CHARS[Math.floor(Math.random() * MATRIX_CHARS.length)] : char.char,
        }))
      );
    }, 100);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="fixed top-0 left-0 w-full h-full pointer-events-none z-[-10] bg-gradient-to-b from-transparent to-green-500/10">
      {chars.map(char => (
        <div
          key={char.id}
          className="absolute text-xs select-none pointer-events-none text-green-500/50 animate-matrix-rain"
          style={{
            left: char.x,
            top: '-100px',
            animationDuration: `${10 / char.speed}s`,
            animationDelay: `${Math.random() * 10}s`,
            opacity: char.opacity,
          }}
        >
          {char.char}
        </div>
      ))}
    </div>
  );
};

export default MatrixBackground;
