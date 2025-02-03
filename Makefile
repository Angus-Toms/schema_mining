CXX = g++
CXXFLAGS = -std=c++17 -I./include
LDFLAGS = -L./include -lduckdb -Wl,-rpath,./include
BUILD_DIR = build
SRC_DIR = src

all: $(BUILD_DIR)/main

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/main: $(BUILD_DIR)/main.o | $(BUILD_DIR)
	$(CXX) $(BUILD_DIR)/main.o -o $(BUILD_DIR)/main $(LDFLAGS)

$(BUILD_DIR)/main.o: $(SRC_DIR)/main.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $(SRC_DIR)/main.cpp -o $(BUILD_DIR)/main.o

clean:
	rm -rf $(BUILD_DIR)
