use super::OneshotTimer;
use core::num::NonZeroIsize;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

pub enum PlayerRequest {
    Position(core::time::Duration),
    NextFrame,
    PreviousFrame,
    Play(f64),
    Pause,
}

// TODO: maybe add a JogDuration() ?
// - so that can jog +/-10s without the controls needing to know the current time of the video
//      - this would have to be in terms of NonZeroI64 where I64 is the number of ns
//      - this hits less io than doing random access
//          - e.g if the current position at the given time is known,
//          just diff the time to sample atom then go on the normal diff path to get the offset
//          - rather than having to linearly search for the position in the time to sample atom

pub enum PlayerAction {
    Position(core::time::Duration),
    JogFrame(NonZeroIsize),
    Play(f64),
    Pause,
}

// TODO: need a separate thing for audio
pub async fn controls_task(
    time_granularity: core::time::Duration,
    mut controls: UnboundedReceiver<PlayerRequest>,
    bounded_controls: Sender<PlayerAction>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    // also need a way to get responses back from the player
    // e.g, update the current time
    //  - this could just be atomicu64 for nanos

    // the only purpose of this task is
    // - delegate controls sent from the ui and turn them into something that
    //      - can be used by the decoder
    //      - wont overwhelm it
    //      - the ui needs to take whatever interface it has and turn it into these commands to send
    //          - e.g, replaying could be request position to 0 then play
    // - manage the state of the player
    //      - take responses back from the decoder and show it in the ui
    //          - decode error/dropped frame
    //          - set the play state to paused when the video ends

    // for the actual decoding
    // - one task for demuxing the container
    // - one task for decoding the frames per track

    // preferrably, this module would be its own lib
    // - such that any ui can use this in the future
    // - then the output should be a sender to a

    // NOTE: communicating directly with the container, which then distributes that to all tracks
    // is prob better than trying to talk to the tracks directly
    //  - this also reduces the amount of things to the codec

    /*
    let mut seek_task = None;
    */

    let jog_state: Option<OneshotTimer<isize>> = None;
    let mut jog_state = std::pin::pin!(jog_state);

    let seek_state: Option<OneshotTimer<core::time::Duration>> = None;
    let mut seek_state = std::pin::pin!(seek_state);

    const MIN_TIMESTEP: core::time::Duration = core::time::Duration::from_millis(20);

    let create_jog_state = || {
        let timeout = tokio::time::sleep(core::cmp::min(time_granularity, MIN_TIMESTEP));
        OneshotTimer {
            timeout,
            data: 0isize,
        }
    };

    let create_seek_state = || {
        let timeout = tokio::time::sleep(core::cmp::min(time_granularity, MIN_TIMESTEP));
        OneshotTimer {
            timeout,
            data: core::time::Duration::ZERO,
        }
    };

    loop {
        let mut jog_task = std::pin::pin!(async {
            if let Some(timer) = jog_state.as_mut().as_pin_mut() {
                let this = timer.project();
                this.timeout.await;
                *this.data
            } else {
                std::future::pending::<()>().await;
                0
            }
        });

        let mut seek_task = std::pin::pin!(async {
            if let Some(timer) = seek_state.as_mut().as_pin_mut() {
                let this = timer.project();
                this.timeout.await;
                *this.data
            } else {
                std::future::pending::<()>().await;
                core::time::Duration::ZERO
            }
        });

        tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            count = &mut jog_task => {
                jog_state.as_mut().set(None);
                println!("jogging {}", count);
                match NonZeroIsize::new(count) {
                    None => (),
                    Some(count) => {
                        let Ok(_) = bounded_controls.send(PlayerAction::JogFrame(count)).await else {
                            break;
                        };
                    }
                }
            }
            dur = &mut seek_task => {
                seek_state.as_mut().set(None);
                let Ok(_) = bounded_controls.send(PlayerAction::Position(dur)).await else {
                    break;
                };
            }
            ctrl = controls.recv() => {
                match ctrl {
                    None => break,
                    Some(req) => {
                        if matches!(req, PlayerRequest::PreviousFrame | PlayerRequest::NextFrame) {
                            if jog_state.is_none() {
                                jog_state.as_mut().set(Some(create_jog_state()));
                            }
                        } else {
                            jog_state.as_mut().set(None);
                        }

                        if matches!(req, PlayerRequest::Position(..)) {
                            seek_state.as_mut().set(Some(create_seek_state()));
                        } else {
                            seek_state.as_mut().set(None);
                        }

                        match req {
                            PlayerRequest::Play(speed) => {
                                // TODO: prevent multiple play requests at the same speed to occour
                                // same with pausing, most of the state should be managed here if
                                // possible
                                let Ok(_) = bounded_controls.send(PlayerAction::Play(speed)).await else {
                                    break;
                                };
                            }
                            PlayerRequest::Pause => {
                                let Ok(_) = bounded_controls.send(PlayerAction::Pause).await else {
                                    break;
                                };
                            }
                            PlayerRequest::PreviousFrame => {
                                let timer = jog_state.as_mut().as_pin_mut().unwrap().project();
                                *timer.data -= 1;
                            }
                            PlayerRequest::NextFrame => {
                                let timer = jog_state.as_mut().as_pin_mut().unwrap().project();
                                *timer.data += 1;
                            }
                            PlayerRequest::Position(pos) => {
                                let timer = seek_state.as_mut().as_pin_mut().unwrap().project();
                                *timer.data = pos;
                            }
                        }
                    }
                }
            }
        }
    }
}
