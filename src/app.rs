use std::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

use crate::player::controls::PlayerRequest;
use crate::player::container::{VideoTrack, AudioTrack, ClosedTrack, TrackRequest, Track, track};
struct Player {
    player: crate::player::Player,
    ctrl: UnboundedSender<PlayerRequest>,
    track_reqs: tokio::sync::mpsc::UnboundedSender<TrackRequest>, 
    video_tracks: std::sync::mpsc::Receiver<(Track<track::Video>, Option<VideoTrack>)>, 
    audio_tracks: std::sync::mpsc::Receiver<(Track<track::Audio>, Option<AudioTrack>)>, 
    closed_tracks: std::sync::mpsc::Receiver<(u32, ClosedTrack)>,
    opened_video_tracks: Vec<(Track<track::Video>, DisplayedVideoTrack)>,

    was_playing_before_drag: Option<bool>,
}

struct DisplayedVideoTrack {
    texture: egui_wgpu::wgpu::Texture,
    id: egui::TextureId,
}

use egui_wgpu::wgpu;
impl DisplayedVideoTrack {
    fn from_egui(
        width: u32,
        height: u32,
        display_out: tokio::sync::mpsc::Receiver<crate::player::container::DisplayBuffer>,
        shutdown: tokio_util::sync::CancellationToken,
        track_close: tokio_util::sync::CancellationToken,
        device: &wgpu::Device,
        queue: std::sync::Arc<wgpu::Queue>,
        proxy: &egui_winit::winit::event_loop::EventLoopProxy<crate::window::UserEvent>,
        renderer: &mut egui_wgpu::Renderer,
        rt: &tokio::runtime::Runtime,
    ) -> Self {
        let size = wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        };

        let texture = device.create_texture(&wgpu::TextureDescriptor {
            size,
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8Unorm,
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
            label: None,
            view_formats: &[wgpu::TextureFormat::Rgba8Unorm],
        });

        let texture2 = texture.clone();
        let proxy = proxy.clone();
        rt.spawn(async move {
            let texture = texture2;
            let mut display_out = display_out;
            let proxy = proxy;
            // TODO
            // - is this even working
            // - no
            //      - where are the channels going
            //      - theres like ~64 requests before its hanging
            loop {
                tokio::select! {
                    frame = display_out.recv() => {
                        let Some(frame) = frame else {
                            break;
                        };


                        let size = wgpu::Extent3d {
                            width: width as _,
                            height: height as _,
                            depth_or_array_layers: 1,
                        };

                        let origin = wgpu::Origin3d::ZERO;

                        queue.write_texture(
                            wgpu::TexelCopyTextureInfo {
                                texture: &texture,
                                mip_level: 0,
                                origin,
                                aspect: wgpu::TextureAspect::All,
                            },
                            &frame.bytes,
                            wgpu::TexelCopyBufferLayout {
                                offset: 0,
                                bytes_per_row: Some(4 * (width as u32)),
                                rows_per_image: Some(height as _),
                            },
                            size,
                        );
                        
                        let repaint_info = egui::RequestRepaintInfo {
                            viewport_id: egui::ViewportId::ROOT,
                            delay: std::time::Duration::ZERO,
                            current_cumulative_pass_nr: 0,
                        };
                        let _ = proxy.send_event(crate::window::UserEvent::RepaintRequest(repaint_info, None));
                    }
                    _ = shutdown.cancelled() => break,
                    _ = track_close.cancelled() => break,
                }
            }
        });

        let view = texture.create_view(&Default::default());
        let id = renderer.register_native_texture_with_sampler_options(
            device,
            &view,
            wgpu::SamplerDescriptor {
                min_filter: wgpu::FilterMode::Linear,
                mag_filter: wgpu::FilterMode::Linear,
                //mipmap_filter: wgpu::MipmapFilterMode::Linear,
                ..Default::default()
            },
        );

        Self {
            texture,
            id,
        }
    }
}

use std::collections::{BTreeMap, BTreeSet};

pub struct GlobalState<'a> {
    pub rt: &'a tokio::runtime::Runtime,
    
    pub player: Option<Player>,
    pub player_tx: mpsc::Sender<Player>,
    pub player_rx: mpsc::Receiver<Player>,

    pub inflight_tracks: BTreeSet<u32>,
    pub display_tracks: BTreeMap<u32, DisplayedVideoTrack>,

    pub create_video_tracks_tx: mpsc::Sender<(u32, Option<crate::player::container::VideoTrack>)>,
    pub create_video_tracks_rx: mpsc::Receiver<(u32, Option<crate::player::container::VideoTrack>)>,
    pub create_audio_tracks_tx: mpsc::Sender<(u32, Option<crate::player::container::AudioTrack>)>,
    pub create_audio_tracks_rx: mpsc::Receiver<(u32, Option<crate::player::container::AudioTrack>)>,
    pub remove_tracks_tx: mpsc::Sender<u32>,
    pub remove_tracks_rx: mpsc::Receiver<u32>,
}

impl <'a> GlobalState<'a> {
    pub fn new(rt: &'a tokio::runtime::Runtime) -> Self {
        let (player_tx, player_rx) = mpsc::channel();
        let (create_video_tracks_tx, create_video_tracks_rx) = mpsc::channel();
        let (create_audio_tracks_tx, create_audio_tracks_rx) = mpsc::channel();
        let (remove_tracks_tx, remove_tracks_rx) = mpsc::channel();
        Self {
            rt,
            player: None,
            player_tx,
            player_rx,
            inflight_tracks: Default::default(),
            display_tracks: Default::default(),
            create_video_tracks_tx, 
            create_video_tracks_rx,
            create_audio_tracks_tx, 
            create_audio_tracks_rx,
            remove_tracks_tx,
            remove_tracks_rx,
        }
    }
}

pub enum WindowKind {
    Main,
}

pub struct WindowState {
    
}

impl WindowState {
    pub fn from_window_kind(window_kind: WindowKind, ctx: &egui::Context) -> Self {
        Self {

        }
    }

    pub fn update(
        &mut self,
        ctx: &egui::Context,
        global_state: &mut GlobalState<'_>,
        wgpu: &crate::window::WgpuState,
        renderer: &mut egui_wgpu::Renderer,
        proxy: &egui_winit::winit::event_loop::EventLoopProxy<crate::window::UserEvent>,
    ) -> Option<UiRequest> {
        egui::CentralPanel::default()
            .show(ctx, |ui| {
                if ui.button("select video").clicked() {
                    let player_tx = global_state.player_tx.clone();
                    global_state.rt.spawn(async move {
                        let file = rfd::AsyncFileDialog::new()
                            .add_filter("video", &["mov", "mp4"])
                            .pick_file()
                            .await;

                        if let Some(file) = file {
                            let path = file.path();
                            let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel();
                            let mut player = crate::player::Player::new(path, ctrl_rx).await;

                            let track_task = core::mem::take(&mut player.metadata.channels).unwrap();
                            let (track_reqs, video_tracks, audio_tracks, closed_tracks) = track_task.spawn_as_sync_task(player.shutdown_token());

                            player_tx.send(Player {
                                player,
                                ctrl: ctrl_tx,
                                track_reqs,
                                video_tracks,
                                audio_tracks,
                                closed_tracks,
                                opened_video_tracks: vec![],
                                was_playing_before_drag: None,
                            }).unwrap();
                        }
                    });
                }

                if let Ok(player) = global_state.player_rx.try_recv() {
                    global_state.player = Some(player);
                }

                if let Some(player) = global_state.player.as_mut() {
                    if ui.button("next frame").is_pointer_button_down_on() {
                        player.ctrl.send(PlayerRequest::NextFrame).unwrap();
                    }

                    ui.add_space(20.);

                    if ui.button("prev frame").is_pointer_button_down_on() {
                        player.ctrl.send(PlayerRequest::PreviousFrame).unwrap();
                    }
                    egui::ComboBox::from_label("tracks")
                        .show_ui(ui, |ui| {
                            for track in &mut player.player.metadata.tracks {
                                use crate::player::container::{TrackKind, Track, track::{Audio, Video}};
                                let text = match track {
                                    TrackKind::Audio(_) => format!("Audio"),
                                    TrackKind::Video(Track { kind: Video { width, height}, .. }) => format!("{width}x{height} Video"),
                                };

                                let (selected, id) = match track {
                                    TrackKind::Audio(Track { selected, id, ..}) => (selected, *id),
                                    TrackKind::Video(Track { selected, id, ..}) => (selected, *id),
                                };
                                if ui.checkbox(selected, text).clicked() {
                                    if !global_state.inflight_tracks.contains(&id) {
                                        global_state.inflight_tracks.insert(id);

                                        if *selected {
                                            player.track_reqs.send(TrackRequest::Listen(track.clone()));
                                        } else {
                                            player.track_reqs.send(TrackRequest::Close(id));
                                        }
                                    }
                                }
                            }
                        });

                    while let Ok((video, video_track)) = player.video_tracks.try_recv() {
                        global_state.inflight_tracks.remove(&video.id);
                        if let Some(video_track) = video_track {
                            let display = DisplayedVideoTrack::from_egui(
                                video.kind.width, 
                                video.kind.height, 
                                video_track.recv, 
                                player.player.shutdown_token(),
                                video_track.track_cancelled,
                                &wgpu.device,
                                wgpu.queue.clone(),
                                proxy,
                                renderer,
                                &global_state.rt,
                            );
                            player.opened_video_tracks.push((video, display));
                        }
                    }
                    while let Ok((audio, _)) = player.audio_tracks.try_recv() {
                        global_state.inflight_tracks.remove(&audio.id);
                        // TODO: audio
                    }

                    while let Ok((id, _)) = player.closed_tracks.try_recv() {
                        global_state.inflight_tracks.remove(&id);
                    }


                    let playing = player.player.play_state();
                    use crate::player::PlayState;
                    let text = match playing {
                        PlayState::Playing => "pause",
                        PlayState::Paused => "play",
                    };
                    let playing = matches!(playing, crate::player::PlayState::Playing);

                    let playback_speed = 1.0;
                    
                    if ui.button(text).clicked() {
                        if playing {
                            player.ctrl.send(PlayerRequest::Pause).unwrap();
                        } else {
                            player.ctrl.send(PlayerRequest::Play(playback_speed)).unwrap();
                        }
                    }

                    for (video, display) in &player.opened_video_tracks {
                        ui.image((display.id, egui::Vec2::new(video.kind.width as f32, video.kind.height as f32)));
                    }

                    let mut current_time = player.player.time().as_secs_f64();
                    let total = player.player.metadata.duration.as_secs_f64();

                    let mut seek_time = 0.;
                    let timeline = egui_timeline_widget::Timeline::new(current_time, total, &mut seek_time);
                    let res = ui.add(timeline);

                    if res.is_pointer_button_down_on() || res.clicked() || res.drag_stopped() {
                        let seek_state = PlayerRequest::Position(core::time::Duration::from_secs_f64(seek_time));
                        // when dragged, want to seek to stop playing then seek to position,
                        // when clicked/stop dragging, want to seek to the position then start
                        // playing

                        if res.is_pointer_button_down_on() {
                            if player.was_playing_before_drag.is_none() {
                               player.was_playing_before_drag = Some(playing);
                            }
                            let play_state = PlayerRequest::Pause;
                            player.ctrl.send(play_state).unwrap();
                            player.ctrl.send(seek_state).unwrap();
                        } else {
                            player.ctrl.send(seek_state).unwrap();

                            let play_state = if player.was_playing_before_drag.unwrap_or(false) {
                                PlayerRequest::Play(playback_speed)
                            } else {
                                PlayerRequest::Pause
                            };
                            player.was_playing_before_drag = None;
                            player.ctrl.send(play_state).unwrap();
                        }
                    }
                }
            });

        None
    }
}

impl WindowState {
    pub fn is_root(&self) -> bool {
        true
    }
}

pub enum UiRequest {

}
