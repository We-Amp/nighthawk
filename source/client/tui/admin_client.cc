// FORK-ONLY (We-Amp). See admin_client.h for the design contract.

#include "source/client/tui/admin_client.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>

#include "absl/status/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "absl/strings/ascii.h"
#include "absl/time/time.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// A dead or wedged admin endpoint must not wedge a worker-pool job forever.
constexpr int kSocketTimeoutSeconds = 10;

absl::StatusOr<std::pair<std::string, int>> ParseBase(absl::string_view base) {
  absl::string_view rest = base;
  if (!absl::ConsumePrefix(&rest, "http://")) {
    absl::ConsumePrefix(&rest, "//");
  }
  std::string host_port(rest);
  size_t colon = host_port.rfind(':');
  if (colon == std::string::npos) {
    return absl::InvalidArgumentError(
        absl::StrCat("admin_base must be host:port, got \"", base, "\""));
  }
  std::string host = host_port.substr(0, colon);
  int port = 0;
  if (!absl::SimpleAtoi(host_port.substr(colon + 1), &port) || port <= 0 || port > 65535) {
    return absl::InvalidArgumentError(
        absl::StrCat("admin_base has an invalid port in \"", base, "\""));
  }
  return std::make_pair(host, port);
}

absl::StatusOr<std::string> ResolveHost(const std::string& host) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  int rc = ::getaddrinfo(host.c_str(), nullptr, &hints, &result);
  if (rc != 0 || result == nullptr) {
    return absl::UnavailableError(
        absl::StrCat("failed to resolve admin host \"", host, "\": ", ::gai_strerror(rc)));
  }
  std::string resolved;
  for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    char buf[INET6_ADDRSTRLEN];
    if (ai->ai_family == AF_INET &&
        ::inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr, buf,
                    sizeof(buf)) != nullptr) {
      resolved = buf;
      break;
    }
    if (ai->ai_family == AF_INET6 && resolved.empty() &&
        ::inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, buf,
                    sizeof(buf)) != nullptr) {
      resolved = buf;
    }
  }
  ::freeaddrinfo(result);
  if (resolved.empty()) {
    return absl::UnavailableError(
        absl::StrCat("admin host \"", host, "\" resolved to no usable address"));
  }
  return resolved;
}

std::string ToLower(absl::string_view s) {
  std::string out(s);
  absl::AsciiStrToLower(&out);
  return out;
}

// Splits a header block into lowercase-name -> value pairs.
std::vector<std::pair<std::string, std::string>> ParseHeaders(const std::string& block) {
  std::vector<std::pair<std::string, std::string>> headers;
  size_t pos = 0;
  while (pos < block.size()) {
    size_t eol = block.find('\n', pos);
    if (eol == std::string::npos) {
      eol = block.size();
    }
    absl::string_view line(block.data() + pos, eol - pos);
    line = absl::StripTrailingAsciiWhitespace(line);
    pos = eol + 1;
    size_t colon = line.find(':');
    if (colon == absl::string_view::npos) {
      continue;
    }
    headers.emplace_back(ToLower(absl::StripAsciiWhitespace(line.substr(0, colon))),
                         std::string(absl::StripAsciiWhitespace(line.substr(colon + 1))));
  }
  return headers;
}

const std::string* FindHeader(const std::vector<std::pair<std::string, std::string>>& headers,
                              absl::string_view lower_name) {
  for (const auto& h : headers) {
    if (h.first == lower_name) {
      return &h.second;
    }
  }
  return nullptr;
}

// Dechunks a chunked body (fused-TE-only; no chunk extensions in admin
// responses, but tolerate them).
absl::StatusOr<std::string> Dechunk(const std::string& raw) {
  std::string out;
  out.reserve(raw.size());
  size_t pos = 0;
  while (true) {
    size_t line_end = raw.find("\r\n", pos);
    if (line_end == std::string::npos) {
      return absl::InternalError("chunked body truncated in chunk-size line");
    }
    std::string size_line = raw.substr(pos, line_end - pos);
    size_t semi = size_line.find(';');
    if (semi != std::string::npos) {
      size_line = size_line.substr(0, semi);
    }
    size_t chunk_size = 0;
    if (!absl::SimpleHexAtoi(size_line, &chunk_size)) {
      return absl::InternalError(absl::StrCat("bad chunk size \"", size_line, "\""));
    }
    pos = line_end + 2;
    if (chunk_size == 0) {
      return out; // Trailing CRLF / trailers after the last chunk are ignored.
    }
    if (pos + chunk_size > raw.size()) {
      return absl::InternalError("chunked body truncated in chunk data");
    }
    out.append(raw, pos, chunk_size);
    pos += chunk_size;
    if (raw.compare(pos, 2, "\r\n") == 0) {
      pos += 2;
    }
  }
}

} // namespace

AdminClient::AdminClient(AdminEndpoints endpoints) : endpoints_(std::move(endpoints)) {}

absl::StatusOr<AdminClient::Response> AdminClient::Request(absl::string_view method,
                                                           absl::string_view path_and_query) {
  auto base = ParseBase(endpoints_.admin_base);
  if (!base.ok()) {
    return base.status();
  }
  auto ip = ResolveHost(base->first);
  if (!ip.ok()) {
    return ip.status();
  }

  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return absl::InternalError(absl::StrCat("socket() failed: errno ", errno));
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(base->second));
  if (::inet_pton(AF_INET, ip->c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return absl::InternalError(absl::StrCat("bad admin address \"", *ip, "\""));
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return absl::UnavailableError(absl::StrCat("admin connect failed: errno ", errno));
  }
  timeval tv{};
  tv.tv_sec = kSocketTimeoutSeconds;
  tv.tv_usec = 0;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  std::string request = absl::StrCat(method, " ", path_and_query, " HTTP/1.1\r\n",
                                     "host: ", base->first, ":", base->second, "\r\n",
                                     "connection: close\r\n\r\n");
  absl::Status write_status = absl::OkStatus();
  size_t off = 0;
  while (off < request.size()) {
    ssize_t n = ::write(fd, request.data() + off, request.size() - off);
    if (n <= 0) {
      write_status =
          absl::InternalError(absl::StrCat("admin request write failed: errno ", errno));
      break;
    }
    off += static_cast<size_t>(n);
  }
  if (!write_status.ok()) {
    ::close(fd);
    return write_status;
  }

  std::string raw;
  char buf[8192];
  while (true) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n <= 0) {
      break; // Connection: close — EOF is the normal end of a response.
    }
    raw.append(buf, static_cast<size_t>(n));
  }
  ::close(fd);

  size_t header_end = raw.find("\r\n\r\n");
  if (header_end == std::string::npos) {
    return absl::InternalError("admin response has no header terminator");
  }
  std::string head = raw.substr(0, header_end);
  std::string raw_body = raw.substr(header_end + 4);
  size_t first_eol = head.find('\n');
  if (first_eol == std::string::npos) {
    return absl::InternalError(absl::StrCat("malformed admin response status line: ", head));
  }
  std::string status_line = head.substr(0, first_eol);
  std::vector<std::pair<std::string, std::string>> headers = ParseHeaders(head.substr(first_eol + 1));

  Response response;
  size_t sp1 = status_line.find(' ');
  size_t sp2 = (sp1 == std::string::npos) ? std::string::npos : status_line.find(' ', sp1 + 1);
  if (sp1 == std::string::npos || sp2 == std::string::npos ||
      !absl::SimpleAtoi(status_line.substr(sp1 + 1, sp2 - sp1 - 1), &response.status)) {
    return absl::InternalError(absl::StrCat("malformed admin response status line: ", status_line));
  }
  if (const std::string* date = FindHeader(headers, "date")) {
    response.date = *date;
  }
  if (const std::string* te = FindHeader(headers, "transfer-encoding")) {
    if (ToLower(*te).find("chunked") != std::string::npos) {
      absl::StatusOr<std::string> body = Dechunk(raw_body);
      if (!body.ok()) {
        return body.status();
      }
      response.body = *std::move(body);
      return response;
    }
  }
  if (const std::string* cl = FindHeader(headers, "content-length")) {
    size_t length = 0;
    if (!absl::SimpleAtoi(*cl, &length)) {
      return absl::InternalError(absl::StrCat("bad content-length \"", *cl, "\""));
    }
    response.body = raw_body.substr(0, std::min(length, raw_body.size()));
    return response;
  }
  response.body = raw_body;
  return response;
}

absl::Status AdminClient::EnableCpuProfiler(bool enable) {
  auto response = Request("POST", enable ? "/cpuprofiler?enable=y" : "/cpuprofiler?enable=n");
  if (!response.ok()) {
    return response.status();
  }
  if (response->status != 200) {
    return absl::FailedPreconditionError(
        absl::StrCat("SUT admin POST /cpuprofiler?enable=", enable ? "y" : "n",
                     " failed with status ", response->status, ": ", response->body));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> AdminClient::AllocProfilerBytes() {
  auto response = Request("POST", "/allocprofiler");
  if (!response.ok()) {
    return response.status();
  }
  if (response->status != 200) {
    return absl::FailedPreconditionError(absl::StrCat(
        "SUT admin POST /allocprofiler failed with status ", response->status, ": ",
        response->body));
  }
  return std::move(response->body);
}

absl::StatusOr<std::string> AdminClient::FetchPrometheusStats() {
  auto response = Request("GET", "/stats?format=prometheus");
  if (!response.ok()) {
    return response.status();
  }
  if (response->status != 200) {
    return absl::FailedPreconditionError(absl::StrCat(
        "SUT admin GET /stats?format=prometheus failed with status ", response->status,
        ": ", response->body));
  }
  return std::move(response->body);
}

absl::StatusOr<int64_t> AdminClient::FetchSutUnixTimeNs() {
  auto response = Request("GET", "/server_info");
  if (!response.ok()) {
    return response.status();
  }
  if (response->status != 200) {
    return absl::FailedPreconditionError(absl::StrCat(
        "SUT admin GET /server_info failed with status ", response->status, ": ",
        response->body));
  }
  if (response->date.empty()) {
    return absl::UnimplementedError(
        "/server_info carries no current-time field: the clock-source field path "
        "`date` (RFC 7231 Date response header) is absent from the response — "
        "record this against the SUT image.");
  }
  absl::Time parsed;
  std::string error;
  if (!absl::ParseTime("%a, %d %b %Y %H:%M:%S GMT", response->date, absl::UTCTimeZone(),
                       &parsed, &error)) {
    return absl::InternalError(
        absl::StrCat("unparseable /server_info date header \"", response->date, "\": ", error));
  }
  return absl::ToUnixNanos(parsed);
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
