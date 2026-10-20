pub mod container;
pub mod controls;

use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;

#[atomic_enum::atomic_enum]
pub enum PlayState {
    Paused = 0,
    Playing,
}

pub struct PlayerState {
    pub play: AtomicPlayState,
    pub time_ns: std::sync::atomic::AtomicU64,
    pub play_speed: atomic_float::AtomicF64,
}

pub struct Player {
    shutdown: tokio_util::sync::CancellationToken,
    pub metadata: container::Metadata,
    state: Arc<PlayerState>,
}

impl Player {
    pub async fn new(
        path: &std::path::Path,
        player_controls: UnboundedReceiver<controls::PlayerRequest>,
    ) -> Self {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (bounded_controls_tx, bounded_controls_rx) = tokio::sync::mpsc::channel(32);
        // TODO: move state handling to the container task
        // - right now none of it is updated
        // - then can display it in the ui

        let state = Arc::new(PlayerState {
            play: AtomicPlayState::new(PlayState::Paused),
            time_ns: std::sync::atomic::AtomicU64::from(0),
            play_speed: atomic_float::AtomicF64::new(1.0),
        });

        let metadata = container::container_task(
            container::Container::Mov,
            path,
            state.clone(),
            bounded_controls_rx,
            shutdown.clone(),
        )
        .await;
        tokio::task::spawn(controls::controls_task(
            metadata.time_granularity,
            player_controls,
            bounded_controls_tx,
            shutdown.clone(),
        ));
        Self {
            shutdown,
            metadata,
            state,
        }
    }

    pub fn play_state(&self) -> PlayState {
        self.state.play.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn play_speed(&self) -> f64 {
        self.state
            .play_speed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn time(&self) -> core::time::Duration {
        let ns = self
            .state
            .time_ns
            .load(std::sync::atomic::Ordering::Relaxed);
        core::time::Duration::from_nanos(ns)
    }

    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        self.shutdown.cancel()
    }
}

pin_project_lite::pin_project! {
    pub struct OneshotTimer<T = ()> {
        #[pin]
        pub timeout: tokio::time::Sleep,
        pub data: T,
    }
}
