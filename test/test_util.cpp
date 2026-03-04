#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/util.h"

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// WideToUtf8 / Utf8ToWide
// ============================================================================

TEST(Util, WideToUtf8_BasicAscii) {
    std::wstring input = L"Hello";
    std::string result = WideToUtf8(input);
    AssertEqual("Hello", result);
}

TEST(Util, WideToUtf8_Empty) {
    std::wstring input = L"";
    std::string result = WideToUtf8(input);
    AssertEqual("", result);
}

TEST(Util, WideToUtf8_Japanese) {
    std::wstring input = L"\x3053\x3093\x306B\x3061\x306F"; // こんにちは
    std::string result = WideToUtf8(input);
    AssertFalse(result.empty(), "Japanese UTF-8 should not be empty");
    // UTF-8 encoded "こんにちは" is 15 bytes (3 bytes per character)
    AssertEqual(15, (int)result.size(), "Japanese UTF-8 byte length");
}

TEST(Util, Utf8ToWide_BasicAscii) {
    std::string input = "Hello";
    std::wstring result = Utf8ToWide(input);
    AssertTrue(result == L"Hello", "ASCII roundtrip");
}

TEST(Util, Utf8ToWide_Empty) {
    std::string input = "";
    std::wstring result = Utf8ToWide(input);
    AssertTrue(result.empty(), "Empty string roundtrip");
}

TEST(Util, WideUtf8_Roundtrip) {
    std::wstring original = L"Test \x30C6\x30B9\x30C8 123";
    std::string utf8 = WideToUtf8(original);
    std::wstring roundtrip = Utf8ToWide(utf8);
    AssertTrue(original == roundtrip, "Roundtrip conversion should preserve data");
}

TEST(Util, WideToUtf8_NullPtr) {
    std::string result = WideToUtf8(nullptr, 0);
    AssertEqual("", result);
}

TEST(Util, WideToUtf8_WithLength) {
    const wchar_t text[] = L"Hello World";
    std::string result = WideToUtf8((const SQLWCHAR *)text, 5);
    AssertEqual("Hello", result);
}

// ============================================================================
// CopyStringToBuffer
// ============================================================================

TEST(Util, CopyStringToBuffer_Normal) {
    std::string src = "Hello";
    SQLCHAR buffer[32];
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBuffer(src, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(5, (int)len);
    AssertEqual("Hello", std::string((char *)buffer));
}

TEST(Util, CopyStringToBuffer_Truncation) {
    std::string src = "Hello World";
    SQLCHAR buffer[6]; // Only room for 5 chars + null
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBuffer(src, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual(11, (int)len); // Full length reported
    AssertEqual("Hello", std::string((char *)buffer)); // Truncated
}

TEST(Util, CopyStringToBuffer_EmptyString) {
    std::string src = "";
    SQLCHAR buffer[16];
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBuffer(src, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)len);
}

TEST(Util, CopyStringToBuffer_NullTarget) {
    std::string src = "Hello";
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBuffer(src, nullptr, 0, &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(5, (int)len); // Length still reported
}

TEST(Util, CopyStringToBufferW_Normal) {
    std::string src = "Hello";
    SQLWCHAR buffer[32];
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBufferW(src, buffer, 32, &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(5, (int)len);
}

TEST(Util, CopyStringToBufferW_Truncation) {
    std::string src = "Hello World";
    SQLWCHAR buffer[6];
    SQLSMALLINT len = 0;

    SQLRETURN ret = CopyStringToBufferW(src, buffer, 6, &len);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual(11, (int)len);
}

// ============================================================================
// ParseConnectionString
// ============================================================================

TEST(Util, ParseConnectionString_Basic) {
    auto params = ParseConnectionString("Host=localhost;Port=8123;Database=default");
    AssertEqual("localhost", params["Host"]);
    AssertEqual("8123", params["Port"]);
    AssertEqual("default", params["Database"]);
}

TEST(Util, ParseConnectionString_WithDriver) {
    auto params = ParseConnectionString("Driver={ClickHouse ODBC Driver};Host=myhost;Port=8123;Database=mydb;UID=testuser;PWD=testpass");
    AssertEqual("{ClickHouse ODBC Driver}", params["Driver"]);
    AssertEqual("myhost", params["Host"]);
    AssertEqual("testuser", params["UID"]);
    AssertEqual("testpass", params["PWD"]);
}

TEST(Util, ParseConnectionString_EmptyValue) {
    auto params = ParseConnectionString("Host=localhost;PWD=;Database=default");
    AssertEqual("localhost", params["Host"]);
    AssertEqual("", params["PWD"]);
    AssertEqual("default", params["Database"]);
}

TEST(Util, ParseConnectionString_Whitespace) {
    auto params = ParseConnectionString("  Host = localhost ; Port = 8123 ");
    AssertEqual("localhost", params["Host"]);
    AssertEqual("8123", params["Port"]);
}

TEST(Util, ParseConnectionString_Empty) {
    auto params = ParseConnectionString("");
    AssertTrue(params.empty(), "Empty string should produce empty map");
}

TEST(Util, ParseConnectionString_TrailingSemicolon) {
    auto params = ParseConnectionString("Host=localhost;Port=8123;");
    AssertEqual("localhost", params["Host"]);
    AssertEqual("8123", params["Port"]);
}

// ============================================================================
// ToUpper / Trim
// ============================================================================

TEST(Util, ToUpper_Basic) {
    AssertEqual("HELLO WORLD", ToUpper("Hello World"));
}

TEST(Util, ToUpper_AlreadyUpper) {
    AssertEqual("ABC", ToUpper("ABC"));
}

TEST(Util, ToUpper_Empty) {
    AssertEqual("", ToUpper(""));
}

TEST(Util, Trim_Leading) {
    AssertEqual("hello", Trim("  hello"));
}

TEST(Util, Trim_Trailing) {
    AssertEqual("hello", Trim("hello  "));
}

TEST(Util, Trim_Both) {
    AssertEqual("hello", Trim("  hello  "));
}

TEST(Util, Trim_Empty) {
    AssertEqual("", Trim(""));
}

TEST(Util, Trim_OnlySpaces) {
    AssertEqual("", Trim("   "));
}
