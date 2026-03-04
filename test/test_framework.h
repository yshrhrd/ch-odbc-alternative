#pragma once

// Lightweight test framework (header-only)
// Enables unit testing without third-party dependencies

#include <string>
#include <vector>
#include <functional>
#include <iostream>
#include <sstream>
#include <cmath>

namespace test_framework {

struct TestCase {
    std::string name;
    std::string group;
    std::function<void()> func;
};

struct TestResult {
    std::string name;
    bool passed;
    std::string message;
};

class TestRunner {
public:
    static TestRunner &Instance() {
        static TestRunner runner;
        return runner;
    }

    void Register(const std::string &group, const std::string &name, std::function<void()> func) {
        tests_.push_back({name, group, func});
    }

    int Run() {
        int passed = 0;
        int failed = 0;
        std::string current_group;

        for (auto &test : tests_) {
            if (test.group != current_group) {
                current_group = test.group;
                std::cout << "\n=== " << current_group << " ===" << std::endl;
            }

            try {
                test.func();
                std::cout << "  [PASS] " << test.name << std::endl;
                passed++;
            } catch (const std::exception &e) {
                std::cout << "  [FAIL] " << test.name << ": " << e.what() << std::endl;
                failed++;
            }
        }

        std::cout << "\n========================================" << std::endl;
        std::cout << "Results: " << passed << " passed, " << failed << " failed, "
                  << (passed + failed) << " total" << std::endl;

        return failed > 0 ? 1 : 0;
    }

private:
    std::vector<TestCase> tests_;
};

class AssertionError : public std::runtime_error {
public:
    AssertionError(const std::string &msg) : std::runtime_error(msg) {}
};

inline void AssertTrue(bool condition, const std::string &msg = "Expected true") {
    if (!condition) throw AssertionError(msg);
}

inline void AssertFalse(bool condition, const std::string &msg = "Expected false") {
    if (condition) throw AssertionError(msg);
}

inline void AssertEqual(const std::string &expected, const std::string &actual, const std::string &msg = "") {
    if (expected != actual) {
        std::ostringstream oss;
        oss << (msg.empty() ? "Strings not equal" : msg) << ": expected=\"" << expected << "\" actual=\"" << actual << "\"";
        throw AssertionError(oss.str());
    }
}

inline void AssertEqual(int expected, int actual, const std::string &msg = "") {
    if (expected != actual) {
        std::ostringstream oss;
        oss << (msg.empty() ? "Integers not equal" : msg) << ": expected=" << expected << " actual=" << actual;
        throw AssertionError(oss.str());
    }
}

inline void AssertEqual(long long expected, long long actual, const std::string &msg = "") {
    if (expected != actual) {
        std::ostringstream oss;
        oss << (msg.empty() ? "Values not equal" : msg) << ": expected=" << expected << " actual=" << actual;
        throw AssertionError(oss.str());
    }
}

inline void AssertNotNull(const void *ptr, const std::string &msg = "Expected non-null pointer") {
    if (!ptr) throw AssertionError(msg);
}

inline void AssertNull(const void *ptr, const std::string &msg = "Expected null pointer") {
    if (ptr) throw AssertionError(msg);
}

struct TestRegistrar {
    TestRegistrar(const std::string &group, const std::string &name, std::function<void()> func) {
        TestRunner::Instance().Register(group, name, func);
    }
};

} // namespace test_framework

#define TEST(group, name) \
    static void test_##group##_##name(); \
    static test_framework::TestRegistrar reg_##group##_##name(#group, #name, test_##group##_##name); \
    static void test_##group##_##name()
