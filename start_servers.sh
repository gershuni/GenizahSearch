#!/bin/bash
# Startup script for Genizah Search servers (Unix/Linux/Mac)
# Launches both the backend API and web interface

echo "============================================================"
echo "   Starting Genizah Search Servers"
echo "============================================================"
echo ""

# Set environment variables
export GENIZAH_BACKEND_PORT=8000
export GENIZAH_WEB_PORT=8081

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down servers..."
    kill $BACKEND_PID $WEB_PID 2>/dev/null
    wait $BACKEND_PID $WEB_PID 2>/dev/null
    echo "✅ All servers stopped."
    exit 0
}

# Trap Ctrl+C and other termination signals
trap cleanup SIGINT SIGTERM

# Start Backend API
echo "📡 Starting Backend API on http://localhost:$GENIZAH_BACKEND_PORT"
GENIZAH_PORT=$GENIZAH_BACKEND_PORT python -m backend.main > backend.log 2>&1 &
BACKEND_PID=$!
echo "   ✓ Backend API starting (PID: $BACKEND_PID)..."

# Wait for backend to start
sleep 2

# Start Web Interface
echo "🌐 Starting Web Interface on http://localhost:$GENIZAH_WEB_PORT"
GENIZAH_PORT=$GENIZAH_WEB_PORT python web/main.py > web.log 2>&1 &
WEB_PID=$!
echo "   ✓ Web Interface starting (PID: $WEB_PID)..."

echo ""
echo "============================================================"
echo "✅ Servers are running!"
echo "============================================================"
echo ""
echo "📡 Backend API:      http://localhost:$GENIZAH_BACKEND_PORT"
echo "   API Docs:         http://localhost:$GENIZAH_BACKEND_PORT/api/docs"
echo ""
echo "🌐 Web Interface:    http://localhost:$GENIZAH_WEB_PORT"
echo ""
echo "Logs are being written to:"
echo "   backend.log"
echo "   web.log"
echo ""
echo "Press Ctrl+C to stop both servers"
echo "============================================================"
echo ""

# Wait for processes
wait $BACKEND_PID $WEB_PID
