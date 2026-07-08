/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        matrix: {
          green: '#00ff00',
          darkgreen: '#008f00',
          black: '#000000',
          darkgray: '#1a1a1a',
          red: '#ff0040',
          yellow: '#ffff00',
        }
      },
      fontFamily: {
        mono: ['Fira Code', 'monospace'],
      },
      animation: {
        'matrix-rain': 'matrix-rain 10s linear infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
        'blink': 'blink 1s step-start infinite',
      },
      keyframes: {
        'matrix-rain': {
          '0%': { transform: 'translateY(-100vh)' },
          '100%': { transform: 'translateY(100vh)' }
        },
        'glow': {
          '0%': { textShadow: '0 0 5px #00ff00' },
          '100%': { textShadow: '0 0 20px #00ff00, 0 0 30px #00ff00' }
        },
        'blink': {
          '0%, 50%': { opacity: '1' },
          '51%, 100%': { opacity: '0' }
        }
      }
    },
  },
  plugins: [],
}
