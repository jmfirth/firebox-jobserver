use crate::FromEnvErrorInner;
use std::io;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    count: Mutex<usize>,
    cvar: Condvar,
}

#[derive(Debug)]
pub struct Acquired(());

impl Client {
    pub fn new(limit: usize) -> io::Result<Client> {
        Ok(Client {
            inner: Arc::new(Inner {
                count: Mutex::new(limit),
                cvar: Condvar::new(),
            }),
        })
    }

    pub(crate) unsafe fn open(_s: &str, _check_pipe: bool) -> Result<Client, FromEnvErrorInner> {
        Err(FromEnvErrorInner::Unsupported)
    }

    pub fn acquire(&self) -> io::Result<Acquired> {
        let mut lock = self.inner.count.lock().unwrap_or_else(|e| e.into_inner());
        while *lock == 0 {
            lock = self
                .inner
                .cvar
                .wait(lock)
                .unwrap_or_else(|e| e.into_inner());
        }
        *lock -= 1;
        Ok(Acquired(()))
    }

    /// Like `acquire`, but waits at most `dur` for a token before returning
    /// `None`.
    ///
    /// The helper thread MUST use this instead of the blocking `acquire`:
    /// on unix the helper parked in a blocking `read(2)` is interruptible
    /// via `pthread_kill(SIGUSR1)` at teardown, but on wasm targets there is
    /// no signal that can interrupt a thread parked in a condvar
    /// (`memory.atomic.wait` parks never reach a syscall boundary where a
    /// signal could be delivered). A helper blocked in plain `acquire()`
    /// with no token forthcoming is therefore unjoinable, and
    /// `HelperThread::drop`'s unconditional join deadlocks the process at
    /// exit (cargo's jobserver helper under `-j1` hit exactly this:
    /// firebox#851 / the #913/#179 teardown wedge). The timed wait is the
    /// wasm-idiomatic interrupt substitute: the helper self-wakes and
    /// re-checks `producer_done`.
    pub fn acquire_timeout(&self, dur: Duration) -> Option<Acquired> {
        let mut lock = self.inner.count.lock().unwrap_or_else(|e| e.into_inner());
        if *lock == 0 {
            let (l, _timeout) = self
                .inner
                .cvar
                .wait_timeout(lock, dur)
                .unwrap_or_else(|e| e.into_inner());
            lock = l;
        }
        if *lock > 0 {
            *lock -= 1;
            Some(Acquired(()))
        } else {
            None
        }
    }

    pub fn try_acquire(&self) -> io::Result<Option<Acquired>> {
        let mut lock = self.inner.count.lock().unwrap_or_else(|e| e.into_inner());
        if *lock == 0 {
            Ok(None)
        } else {
            *lock -= 1;
            Ok(Some(Acquired(())))
        }
    }

    pub fn release(&self, _data: Option<&Acquired>) -> io::Result<()> {
        let mut lock = self.inner.count.lock().unwrap_or_else(|e| e.into_inner());
        *lock += 1;
        drop(lock);
        self.inner.cvar.notify_one();
        Ok(())
    }

    pub fn string_arg(&self) -> String {
        String::new()
    }

    pub fn available(&self) -> io::Result<usize> {
        let lock = self.inner.count.lock().unwrap_or_else(|e| e.into_inner());
        Ok(*lock)
    }

    pub fn configure(&self, _cmd: &mut Command) {
        // no-op: WASI has no cross-process jobserver pipes
    }
}

#[derive(Debug)]
pub struct Helper {
    thread: JoinHandle<()>,
    state: Arc<super::HelperState>,
}

pub(crate) fn spawn_helper(
    client: crate::Client,
    state: Arc<super::HelperState>,
    mut f: Box<dyn FnMut(io::Result<crate::Acquired>) + Send>,
) -> io::Result<Helper> {
    // Mirror the unix helper's interruptible-acquire loop. The token wait is
    // TIMED (see `acquire_timeout`): a wasm thread parked in a plain condvar
    // wait cannot be interrupted, so the helper must self-wake to observe
    // `producer_done` once the producer half is dropped. Without this, a
    // helper waiting on a token that never comes (cargo under `-j1` never
    // releases one) outlives the build and wedges teardown.
    let state2 = state.clone();
    let thread = Builder::new().spawn(move || {
        state2.for_each_request(|helper| loop {
            match client.inner.acquire_timeout(Duration::from_millis(50)) {
                Some(data) => {
                    break f(Ok(crate::Acquired {
                        client: client.inner.clone(),
                        data,
                        disabled: false,
                    }));
                }
                None if helper.lock().producer_done => break,
                None => {}
            }
        });
    })?;

    Ok(Helper { thread, state })
}

impl Helper {
    pub fn join(self) {
        // Same bounded, conditional join as the unix implementation (minus
        // the SIGUSR1 prod, which the timed acquire wait replaces): wait for
        // the helper to flag `consumer_done`, then join it. If the helper
        // somehow never gets there, leak it rather than deadlock the
        // process at exit — upstream's documented "off in the ether" policy.
        let dur = Duration::from_millis(10);
        let mut state = self.state.lock();
        debug_assert!(state.producer_done);
        for _ in 0..200 {
            if state.consumer_done {
                break;
            }
            state = self
                .state
                .cvar
                .wait_timeout(state, dur)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            thread::yield_now();
        }
        if state.consumer_done {
            drop(self.thread.join());
        }
    }
}
