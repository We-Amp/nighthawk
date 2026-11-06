#include "source/client/headless/fold_tracker.h"

#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

namespace {

absl::Status WindowNotFound(int64_t id) {
  return absl::NotFoundError(absl::StrCat("no window with id ", id));
}

} // namespace

absl::StatusOr<int64_t> FoldTracker::Stage() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Window& window : windows_) {
    if (window.state == State::kStaged || window.state == State::kFolding) {
      return absl::FailedPreconditionError(
          "a profile window is already staged or folding: single-flight discipline");
    }
  }
  Window window;
  window.id = next_id_++;
  windows_.push_back(std::move(window));
  return windows_.back().id;
}

absl::Status FoldTracker::MarkFolding(int64_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (Window& window : windows_) {
    if (window.id == id) {
      if (window.state != State::kStaged) {
        return absl::FailedPreconditionError(
            "only a staged window can start folding");
      }
      window.state = State::kFolding;
      return absl::OkStatus();
    }
  }
  return WindowNotFound(id);
}

absl::Status FoldTracker::MarkReady(int64_t id, std::string note) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (Window& window : windows_) {
    if (window.id == id) {
      if (window.state != State::kFolding) {
        return absl::FailedPreconditionError(
            "only a folding window can land as ready");
      }
      window.state = State::kReady;
      window.note = std::move(note);
      return absl::OkStatus();
    }
  }
  return WindowNotFound(id);
}

absl::Status FoldTracker::MarkFailed(int64_t id, std::string reason) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (Window& window : windows_) {
    if (window.id == id) {
      if (window.state != State::kStaged && window.state != State::kFolding) {
        return absl::FailedPreconditionError(
            "a terminal window cannot fail again");
      }
      window.state = State::kFailed;
      window.failure_reason = std::move(reason);
      return absl::OkStatus();
    }
  }
  return WindowNotFound(id);
}

absl::StatusOr<FoldTracker::WindowStatus> FoldTracker::Status(int64_t id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Window& window : windows_) {
    if (window.id == id) {
      WindowStatus status;
      status.id = window.id;
      status.state = window.state;
      status.failure_reason = window.failure_reason;
      status.note = window.note;
      return status;
    }
  }
  return WindowNotFound(id);
}

bool FoldTracker::busy() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Window& window : windows_) {
    if (window.state == State::kStaged || window.state == State::kFolding) {
      return true;
    }
  }
  return false;
}

int64_t FoldTracker::open_id() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Window& window : windows_) {
    if (window.state == State::kStaged) {
      return window.id;
    }
  }
  return 0;
}

int64_t FoldTracker::folding_id() const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Window& window : windows_) {
    if (window.state == State::kFolding) {
      return window.id;
    }
  }
  return 0;
}

std::vector<FoldTracker::WindowStatus> FoldTracker::All() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<WindowStatus> all;
  all.reserve(windows_.size());
  for (const Window& window : windows_) {
    WindowStatus status;
    status.id = window.id;
    status.state = window.state;
    status.failure_reason = window.failure_reason;
    status.note = window.note;
    all.push_back(std::move(status));
  }
  return all;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
