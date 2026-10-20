pub mod mov;

use crate::player::PlayerState;
use crate::player::controls::PlayerAction;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::mpsc::Receiver;

pub enum Container {
    Mov,
}

pub struct TrackChannels {
    tx: tokio::sync::mpsc::Sender<TrackAction>,
    rx: tokio::sync::mpsc::Receiver<TrackResponse>,
}

pub struct ClosedTrack;

pub enum TrackRequest {
    Listen(TrackKind),
    Close(u32),
}

impl TrackChannels {
    pub async fn create_track<T: track::DisplayTrack>(
        &mut self,
        track: &Track<T>,
    ) -> Option<T::Display> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        // TODO: this should be gotten from the container task,
        // which then cancells it when the track is dropped
        let track_cancelled = tokio_util::sync::CancellationToken::new();

        let disp_track = T::create_display(track.id, rx, track_cancelled);
        self.tx.send(TrackAction::Open(track.id, tx)).await.ok()?;
        match self.rx.recv().await? {
            TrackResponse::Ok(id) => Some(disp_track),
            _ => None,
        }
    }
    pub async fn remove_track(&mut self, id: u32) {
        self.tx.send(TrackAction::Close(id)).await;
        match self.rx.recv().await.unwrap() {
            TrackResponse::Ok(id) => (),
            _ => panic!(),
        }
    }

    pub fn spawn_as_sync_task(
        self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<TrackRequest>,
        std::sync::mpsc::Receiver<(Track<track::Video>, Option<VideoTrack>)>,
        std::sync::mpsc::Receiver<(Track<track::Audio>, Option<AudioTrack>)>,
        std::sync::mpsc::Receiver<(u32, ClosedTrack)>,
    ) {
        use std::sync::mpsc as std_mpsc;
        use tokio::sync::mpsc as tokio_mpsc;
        let (vid_tx, vid_rx) = std_mpsc::channel();
        let (aud_tx, aud_rx) = std_mpsc::channel();
        let (closed_tx, closed_rx) = std_mpsc::channel();
        let (req_tx, req_rx) = tokio_mpsc::unbounded_channel();

        tokio::task::spawn(async move {
            let video = vid_tx;
            let audio = aud_tx;
            let closed = closed_tx;
            let mut reqs = req_rx;
            let mut this = self;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    req = reqs.recv() => {
                        let Some(req) = req else {
                            break;
                        };
                        match req {
                            TrackRequest::Listen(kind) => {
                                match kind {
                                    TrackKind::Audio(aud) => {
                                        let res = this.create_track(&aud).await;
                                        audio.send((aud, res)).unwrap();
                                    }
                                    TrackKind::Video(vid) => {
                                        let res = this.create_track(&vid).await;
                                        video.send((vid, res)).unwrap();
                                    }
                                }
                            }
                            TrackRequest::Close(id) => {
                                let res = this.remove_track(id).await;
                                closed.send((id, ClosedTrack)).unwrap();
                            }
                        }
                    }
                }
            }
        });

        (req_tx, vid_rx, aud_rx, closed_rx)
    }
    //pub async fn spawn_as_sync_task(mut self, tx: tokio::sync::mpsc::UnboundedSender<TrackAction>, video_tracks_tx: )
}

pub struct Metadata {
    /// minimum accepted time between frames
    pub time_granularity: core::time::Duration,
    pub duration: core::time::Duration,
    pub tracks: Vec<TrackKind>,
    pub channels: Option<TrackChannels>,
}

#[derive(Clone)]
pub struct Track<T> {
    pub(crate) id: u32,
    pub(crate) selected: bool,
    pub kind: T,
}
pub mod track {
    // FIXME: change the name of this
    // - its the trait for associating the struct to listen for audio/video
    // output
    pub trait DisplayTrack {
        type Display;
        fn create_display(
            id: u32,
            recv: tokio::sync::mpsc::Receiver<super::DisplayBuffer>,
            track_cancelled: tokio_util::sync::CancellationToken,
        ) -> Self::Display;
    }

    #[derive(Clone)]
    pub struct Audio;

    impl DisplayTrack for Audio {
        type Display = super::AudioTrack;
        fn create_display(
            id: u32,
            recv: tokio::sync::mpsc::Receiver<super::DisplayBuffer>,
            track_cancelled: tokio_util::sync::CancellationToken,
        ) -> Self::Display {
            super::AudioTrack {
                id,
                recv,
                track_cancelled,
            }
        }
    }

    #[derive(Clone)]
    pub struct Video {
        pub width: u32,
        pub height: u32,
    }

    impl DisplayTrack for Video {
        type Display = super::VideoTrack;
        fn create_display(
            id: u32,
            recv: tokio::sync::mpsc::Receiver<super::DisplayBuffer>,
            track_cancelled: tokio_util::sync::CancellationToken,
        ) -> Self::Display {
            super::VideoTrack {
                id,
                recv,
                track_cancelled,
            }
        }
    }
}

// FIXME: maybe have a trait with associated type for the data thats sent for display
// e.g for ancillary / for other tracks it could be data other than just a buffer
//  - or at least interpreted differently
pub struct AudioTrack {
    pub id: u32,
    pub recv: tokio::sync::mpsc::Receiver<DisplayBuffer>,
    pub track_cancelled: tokio_util::sync::CancellationToken,
}

pub struct VideoTrack {
    pub id: u32,
    pub recv: tokio::sync::mpsc::Receiver<DisplayBuffer>,
    pub track_cancelled: tokio_util::sync::CancellationToken,
}

#[derive(Clone)]
pub enum TrackKind {
    Audio(Track<track::Audio>),
    Video(Track<track::Video>),
}

pub struct DisplayBuffer<A: std::alloc::Allocator = std::alloc::Global> {
    pub bytes: Box<[u8], A>,
}

pub enum TrackAction {
    Open(u32, tokio::sync::mpsc::Sender<DisplayBuffer>),
    Close(u32),
}

pub enum UnsupportedTrackError {
    UnsupportedCodec,
    UnsupportedTrackKind,
}

enum TrackResponse {
    Ok(u32),
    TrackDoesNotExist(u32),
    TrackUnsupported(u32, UnsupportedTrackError),
    AlreadyOpen(u32),
    AlreadyClosed(u32),
}

// TODO: variant for when the file type is unknown
// - hardcoding it for now
pub async fn container_task(
    container: Container,
    path: &std::path::Path,
    state: Arc<PlayerState>,
    bounded_controls: Receiver<PlayerAction>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Metadata {
    match container {
        Container::Mov => mov::container_task(path, state, bounded_controls, shutdown).await,
    }
}
