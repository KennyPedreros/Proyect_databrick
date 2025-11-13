/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        espe: {
          green: "#1B5E20",
          "green-light": "#4CAF50",
          "green-lighter": "#81C784",
          white: "#FFFFFF",
          gray: "#F5F5F5",
          dark: "#212121",
        },
      },
    },
  },
  plugins: [],
};
