PYTHON = python3
PIP = pip3
APP_FILE = web_app.py
PORT = 8000
PID_FILE = server.pid
LOG_FILE = server.log

.PHONY: help install run start stop clean

help:
	@echo "Spark Analyzer Makefile Commands:"
	@echo "  make install  - Install dependencies from requirements.txt"
	@echo "  make run      - Run the server in foreground (Ctrl+C to stop)"
	@echo "  make start    - Run the server in background"
	@echo "  make stop     - Stop the background server"
	@echo "  make clean    - Remove logs, PID file, and temporary files"

install:
	$(PIP) install -r requirements.txt

run:
	@echo "Starting server in foreground on port $(PORT)..."
	$(PYTHON) $(APP_FILE)

start:
	@if [ -f $(PID_FILE) ]; then \
		echo "Server is already running with PID $$(cat $(PID_FILE))"; \
	else \
		nohup $(PYTHON) $(APP_FILE) > $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE); \
		echo "Server started with PID $$(cat $(PID_FILE))"; \
		echo "Logs: $(LOG_FILE)"; \
	fi

stop:
	@if [ -f $(PID_FILE) ]; then \
		kill $$(cat $(PID_FILE)) 2>/dev/null || true; \
		rm $(PID_FILE); \
		echo "Server stopped"; \
	else \
		echo "No PID file found. Checking port $(PORT)..."; \
		PID=$$(lsof -ti:$(PORT)); \
		if [ -n "$$PID" ]; then \
			kill $$PID; \
			echo "Server process $$PID on port $(PORT) killed"; \
		else \
			echo "No server running on port $(PORT)"; \
		fi \
	fi

clean:
	rm -f $(PID_FILE) $(LOG_FILE)
	rm -f latest_analysis_result.csv
	rm -rf __pycache__
	rm -rf */__pycache__
