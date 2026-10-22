// Compile the real NIF in an isolated test VM, without production fault hooks.
#include <erl_nif.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static int test_thread_create(char *, ErlNifTid *, void *(*)(void *), void *, ErlNifThreadOpts *);
static pid_t test_waitpid(pid_t, int *, int);
static int test_select(int, fd_set *, fd_set *, fd_set *, timeval *);
static ssize_t test_read(int, void *, size_t);
static int test_kill(pid_t, int);
static int test_thread_join(ErlNifTid, void **);
static int test_waitid(idtype_t, id_t, siginfo_t *, int);

#define enif_thread_create test_thread_create
#define waitpid test_waitpid
#define select test_select
#define read test_read
#define kill test_kill
#define enif_thread_join test_thread_join
#define waitid test_waitid
#include "../../c_src/unix/pty.cpp"
#undef enif_thread_create
#undef waitpid
#undef select
#undef read
#undef kill
#undef enif_thread_join
#undef waitid

static std::atomic<bool> child_reaped{false};
static std::atomic<bool> joining_reader{false};
static std::atomic<bool> read_barrier_entered{false};

static std::string fault_path(const char *suffix) {
  return std::string(getenv("EXPTY_FAULT_PID")) + suffix;
}

static bool reject_rewait(const char *operation) {
  if (!child_reaped) return false;
  FILE *record = fopen(fault_path(".rewaits").c_str(), "a");
  if (record == nullptr) abort();
  fprintf(record, "post-reap %s\n", operation);
  fclose(record);
  errno = ECHILD;
  return true;
}

static bool fault_is(const char *mode) {
  const char *fault = getenv("EXPTY_NATIVE_FAULT");
  return fault != nullptr && strcmp(fault, mode) == 0;
}

static int test_thread_create(char *name, ErlNifTid *tid, void *(*fn)(void *),
                              void *arg, ErlNifThreadOpts *opts) {
  if ((fault_is("reader") && strcmp(name, "ExPTY.PipeSocketReader") == 0) ||
      (fault_is("waiter") && strcmp(name, "ExPTY.WaitPid") == 0)) {
    pid_t pid = strcmp(name, "ExPTY.WaitPid") == 0
      ? static_cast<pty_baton *>(arg)->pid : static_cast<pty_pipesocket *>(arg)->pid;
    FILE *record = fopen(getenv("EXPTY_FAULT_PID"), "w");
    if (record == nullptr) abort();
    fprintf(record, "%d", pid);
    fclose(record);
    return EAGAIN;
  }
  return enif_thread_create(name, tid, fn, arg, opts);
}

static pid_t test_waitpid(pid_t pid, int *status, int options) {
  if (reject_rewait("waitpid")) return -1;
  pid_t result = waitpid(pid, status, options);
  if (result == pid) {
    child_reaped = true;
    if (fault_is("custody") || fault_is("inflight")) {
      // Invoke the real teardown against the real registry at final reap.
      if (fault_is("custody")) cleanup();
      FILE *ready = fopen(fault_path(".reaped").c_str(), "w");
      if (ready == nullptr) abort();
      fprintf(ready, "%d", pid);
      fclose(ready);
      auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (fault_is("custody") && access(fault_path(".continue").c_str(), F_OK) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) abort();
        usleep(1000);
      }
    }
  }
  // Reap the actual child first; inject only the uncertain status report.
  if (result == pid && fault_is("waitpid")) {
    errno = ECHILD;
    return -1;
  }
  return result;
}

static int test_kill(pid_t pid, int signal) {
  if (fault_is("inflight") && signal == 10 && !child_reaped) {
    FILE *ready = fopen(fault_path(".signaling").c_str(), "w");
    if (ready == nullptr) abort();
    fclose(ready);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (access(fault_path(".continue").c_str(), F_OK) != 0) {
      if (std::chrono::steady_clock::now() >= deadline) abort();
      usleep(1000);
    }
    if (!child_reaped) return 0;
  }
  if (child_reaped) {
    FILE *record = fopen(fault_path(".signals").c_str(), "a");
    if (record == nullptr) abort();
    fprintf(record, "post-reap target=%d signal=%d\n", pid, signal);
    fclose(record);
    // Force missing-group fallback without signaling any live foreign PID.
    if (pid < 0) { errno = ESRCH; return -1; }
    return 0;
  }
  return kill(pid, signal);
}

static int test_waitid(idtype_t type, id_t id, siginfo_t *info, int options) {
  if (reject_rewait("waitid")) return -1;
  int result = waitid(type, id, info, options);
  if (result == 0 && info->si_pid == static_cast<pid_t>(id) && fault_is("waitid")) {
    // Model observed identity loss under the actual registry's observation lock.
    int status;
    if (waitpid(info->si_pid, &status, 0) != info->si_pid) abort();
    child_reaped = true;
    errno = ECHILD;
    return -1;
  }
  return result;
}

static int test_thread_join(ErlNifTid tid, void **result) {
  joining_reader = true;
  return enif_thread_join(tid, result);
}

static int test_select(int nfds, fd_set *readfds, fd_set *writefds,
                       fd_set *exceptfds, timeval *timeout) {
  if (fault_is("waiter")) {
    // The failed waiter must start joining before it consumes child identity.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!joining_reader && !child_reaped) {
      if (std::chrono::steady_clock::now() >= deadline) abort();
      usleep(1000);
    }
  }
  if (fault_is("select")) {
    errno = EBADF;
    return -1;
  }
  return select(nfds, readfds, writefds, exceptfds, timeout);
}

static ssize_t test_read(int fd, void *buffer, size_t count) {
  if (fault_is("straddle") && isatty(fd) && !read_barrier_entered.exchange(true)) {
    // Stop after the real pre-read exit observation, before consuming data.
    FILE *ready = fopen(fault_path(".reading").c_str(), "w");
    if (ready == nullptr) abort();
    fclose(ready);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (access(fault_path(".continue").c_str(), F_OK) != 0) {
      if (std::chrono::steady_clock::now() >= deadline) abort();
      usleep(1000);
    }
  }
  if (fault_is("read") && isatty(fd)) {
    errno = EBADF;
    return -1;
  }
  if ((fault_is("short-read") || fault_is("straddle")) && isatty(fd) && count > 997) count = 997;
  return read(fd, buffer, count);
}
