@echo off
setlocal
echo Starting Taskloom on http://127.0.0.1:8080
echo Press Ctrl+C to stop.
node bin\taskloom.js serve --host 127.0.0.1 --port 8080 --data .\data
