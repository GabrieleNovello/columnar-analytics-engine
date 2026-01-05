# Windows-only Makefile (PowerShell + MinGW)

CXX      := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Werror -I.

INGEST_SRCS := ingestion/*.cpp metadata/meta_writer.cpp
EXEC_SRCS   := execution/*.cpp metadata/meta_reader.cpp

INGEST_BIN := ingest.exe
EXEC_BIN   := exec.exe

.PHONY: all ingest exec clean

all: ingest exec

ingest:
	$(CXX) $(CXXFLAGS) $(INGEST_SRCS) -o $(INGEST_BIN)

exec:
	$(CXX) $(CXXFLAGS) $(EXEC_SRCS) -o $(EXEC_BIN)

clean:
	del /Q $(INGEST_BIN) $(EXEC_BIN)

