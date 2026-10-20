use tokio::sync::Notify;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use crate::player::controls::PlayerAction;
use super::{Metadata, TrackChannels};
use super::{TrackAction, TrackResponse, UnsupportedTrackError};
use super::super::{OneshotTimer};
pub use crate::player::{PlayerState, PlayState};

pub async fn container_task(path: &std::path::Path, state: Arc<PlayerState>, controls: Receiver<PlayerAction>, shutdown: tokio_util::sync::CancellationToken) -> Metadata {
    // the suppl task should parse the immediate metadata and store relavant info,
    // then spawn a looping task to handle decoding frames
    //  - this is so that a player for a given video has to get the metadata before being fully
    //  created,

    // this should prob also return some metadata associated with it
    // - e.g width, height, duration
    //      - thats abt it for now i think

    let (reader, mov, metadata, (track_cmd_rx, track_res_tx)) = parse_metadata(path).await;
    let time_granularity = mov.header.duration_from_step(1);

    tokio::task::spawn(async move {
        let mut controls = controls;
        let shutdown = shutdown;
        let mut track_cmd_rx = track_cmd_rx;
        let track_res_tx = track_res_tx;
        let mut reader = reader;
        let MovVideo {
            header,
            data,
            tracks,
        } = mov;

        use std::collections::BTreeMap;
        let mut track_map = tracks.into_iter().map(|track| (track.header.track_id, ActiveTrack { track, next: None })).collect::<BTreeMap<_, _>>();

        let mut open_track_map = std::collections::BTreeMap::<u32, CodecTaskHandle>::new();

        let mut current_time = core::time::Duration::ZERO;
        let mut is_playing = false;
        let opts = atom_parser::ParseOptions::default();

        let frame_display: Option<OneshotTimer<core::time::Duration>> = None;
        let mut frame_display = std::pin::pin!(frame_display);
        let mut play_speed = 1.0;

        loop {
            let next_frame = if !is_playing {
                None
            } else {
                let mut requested_time = None;

                for (track_id, track) in &mut track_map {
                    match (&mut track.next, open_track_map.get_mut(track_id)) {
                        (Some(TrackDisplayInfo { id, time } ), Some(handle)) => {
                            if time.saturating_sub(current_time) <= time_granularity {
                                if let Some(buffer) = handle.frame_buffer.remove(id) {
                                    let Ok(_) = handle.display_out.send(crate::player::container::DisplayBuffer { bytes: buffer }).await else {
                                        break;
                                    };
                                }
                                track.next = track.track.get_next_frame_info_from_time(&current_time, &mut reader, &opts, &header).await.unwrap();
                                if let Some(next) = track.next.as_ref() {
                                    if let Some(time) = &requested_time {
                                        requested_time = Some(core::cmp::min(*time, next.time));
                                    } else {
                                        requested_time = Some(next.time);
                                    }

                                    if handle.frame_buffer.contains_key(&next.id) {
                                        continue;
                                    }
                                    let offset = track.track.sample_to_chunk.get_frame_offset_async(next.id, &track.track.chunk_offset, &track.track.sample_size, &mut reader, &opts).await.unwrap();
                                    let extent = data.from_offset_32(offset).unwrap();
                                    handle.raw_frame_in.send((extent, next.id)).await.unwrap();
                                }
                            } else {
                                if let Some(current_dur) = &requested_time {
                                    requested_time = Some(core::cmp::min(*current_dur, *time));
                                } else {
                                    requested_time = Some(*time);
                                }
                            }
                        }
                        _ => (),
                    }
                }

                // if no tracks are requested, 
                // or if they are all finished playing then idle
                // otherwise wait for the minimum amount of time until
                // the next frame
                requested_time.map(|req_time| {
                    let timeout = tokio::time::sleep(req_time.abs_diff(current_time));
                    OneshotTimer {
                        timeout,
                        data: req_time,
                    }
                })
            };
            frame_display.as_mut().set(next_frame);

            loop {
                let mut display_task = std::pin::pin!(async {
                    if let Some(timer) = frame_display.as_mut().as_mut().as_pin_mut() {
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
                        return;
                    }
                    new_time = &mut display_task => {
                        // wake up the thread
                        // frames will be displayed on the next iter
                        state.time_ns.store(new_time.as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
                        current_time = new_time;
                        break;
                    }
                    cmd = track_cmd_rx.recv() => {
                        match cmd {
                            None => return,
                            Some(cmd) => match cmd {
                                TrackAction::Open(id, tx) => {
                                    let response = if open_track_map.contains_key(&id) {
                                        TrackResponse::AlreadyOpen(id)
                                    } else {
                                        if let Some(track) = track_map.get_mut(&id) {
                                            let r = reader.clone();
                                            match create_codec_task_from_track(&track.track, r, tx, shutdown.clone()) {
                                                Ok(handle) => {
                                                    track.next = track.track.get_current_frame_info_from_time(&current_time, &mut reader, &opts, &header).await.unwrap();

                                                    if let Some(current) = track.next.as_ref() {
                                                        let offset = track.track.sample_to_chunk.get_frame_offset_async(current.id, &track.track.chunk_offset, &track.track.sample_size, &mut reader, &opts).await.unwrap();
                                                        let extent = data.from_offset_32(offset).unwrap();
                                                        handle.raw_frame_in.send((extent, current.id)).await.unwrap();
                                                    }
                                                    //track.next = next_frame;
                                                    open_track_map.insert(id, handle);
                                                    TrackResponse::Ok(id)
                                                }
                                                Err(e) => {
                                                    TrackResponse::TrackUnsupported(id, e)
                                                }
                                            }
                                        } else {
                                            TrackResponse::TrackDoesNotExist(id)
                                        }
                                    };
                                    if let Err(_) = track_res_tx.send(response).await {
                                        return;
                                    }
                                }
                                TrackAction::Close(id) => {
                                    let response = if let Some(_) = open_track_map.remove(&id) {
                                        TrackResponse::Ok(id)
                                    } else if !track_map.contains_key(&id) {
                                        TrackResponse::TrackDoesNotExist(id)
                                    } else {
                                        TrackResponse::AlreadyClosed(id)
                                    };

                                    if let Err(_) = track_res_tx.send(response).await {
                                        return;
                                    }
                                } 
                            }
                        }
                    }
                    ctrl = controls.recv() => {
                        match ctrl {
                            None => return,
                            Some(cmd) => {
                                // any change in play state should reset the frame display
                                // - e.g changing playback speed, changing state, or pausing
                                // - the controls task should prevent sending multiple requests for the
                                // same playback speed sequentially
                                frame_display.as_mut().set(None);
                                match cmd {
                                    // TODO
                                    PlayerAction::Play(speed) => {
                                        state.play_speed.store(speed, std::sync::atomic::Ordering::Relaxed);
                                        // good enough for now

                                        let was_playing = is_playing;

                                        is_playing = speed != 0.;

                                        let play_state = if is_playing {
                                            PlayState::Playing
                                        } else {
                                            PlayState::Paused
                                        };
                                        state.play.store(play_state, std::sync::atomic::Ordering::Relaxed);


                                        if speed != play_speed || was_playing != is_playing {
                                            play_speed = speed;
                                            break;
                                        }
                                    }
                                    PlayerAction::Pause => {
                                        if is_playing != false {
                                            state.play.store(PlayState::Paused, std::sync::atomic::Ordering::Relaxed);
                                            is_playing = false;
                                            break;
                                        }
                                    }
                                    PlayerAction::Position(time) => {
                                        // TODO: this check can be better
                                        // e.g check individual tracks to see if their time is
                                        //  - so the same frame isnt requested multiple within the time ~16ms its valid
                                        if time != current_time {
                                            current_time = time;
                                            state.time_ns.store(time.as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
                                            for (track_id, track) in &mut track_map {
                                                if let Some(handle) = open_track_map.get_mut(track_id) {
                                                    track.next = track.track.get_current_frame_info_from_time(&current_time, &mut reader, &opts, &header).await.unwrap();

                                                    if let Some(current) = track.next.as_ref() {
                                                        let offset = track.track.sample_to_chunk.get_frame_offset_async(current.id, &track.track.chunk_offset, &track.track.sample_size, &mut reader, &opts).await.unwrap();
                                                        let extent = data.from_offset_32(offset).unwrap();
                                                        handle.raw_frame_in.send((extent, current.id)).await.unwrap();
                                                    }
                                                }
                                            }
                                            break;
                                        }
                                    }
                                    // TODO: jog frames
                                    _ => ()
                                }
                            }
                        }
                    }
                    _ = OpenTracks::new(&mut open_track_map) => {
                        for (track_id, track) in &mut track_map {
                            match (&mut track.next, open_track_map.get_mut(track_id)) {
                                (Some(TrackDisplayInfo { id, time } ), Some(handle)) => {
                                    if time.saturating_sub(current_time) <= time_granularity {
                                        if let Some(buffer) = handle.frame_buffer.remove(id) {
                                            let Ok(_) = handle.display_out.send(crate::player::container::DisplayBuffer { bytes: buffer }).await else {
                                                return;
                                            };
                                        }
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                }
            }
        }
    });

    metadata
}

struct CodecTaskHandle {
    display_out: tokio::sync::mpsc::Sender<super::DisplayBuffer>,
    raw_frame_in: tokio::sync::mpsc::Sender<(atom_parser::Extent<u64, u64>, u32)>,
    decoded_frame_out: tokio::sync::mpsc::Receiver<(Box<[u8]>, u32)>,
    codec_cancel_token: tokio_util::sync::CancellationToken,
    frame_buffer: std::collections::BTreeMap<u32, Box<[u8]>>,
}

impl Drop for CodecTaskHandle {
    fn drop(&mut self) {
        self.codec_cancel_token.cancel()
    }
}

// TODO: 
// - its buffered by the container
// - use two separate channels for send / completed frames
//
// TODO:
//  - this should be moved to separate modules
//  like
//      - codec
//          - video
//              - raw
//              - h264
//          - audio
//              - raw
//              - opus
fn create_codec_task_from_track<O>(track: &Track<O>, mut reader: atom_parser::AsyncFileReader<Arc<std::fs::File>, BUFFER_SIZE>, display_out: tokio::sync::mpsc::Sender<super::DisplayBuffer>, shutdown: tokio_util::sync::CancellationToken) -> Result<CodecTaskHandle, UnsupportedTrackError> {
    use quicktime_2001::{TrackKind, AudioTrack, VideoTrack};
    Ok(match &track.kind {
        TrackKind::Audio(AudioTrack { entries }) => {
            let entry = entries.first().unwrap();
            use quicktime_2001::AudioCodec;
            match &entry.codec {
                _ => return Err(UnsupportedTrackError::UnsupportedCodec)
            }
        }
        TrackKind::Video(VideoTrack { entries }) => {
            let entry = entries.first().unwrap();
            use quicktime_2001::VideoCodec;
            match &entry.codec {
                VideoCodec::Rgb => {
                    let (raw_frame_tx, raw_frame_rx) = tokio::sync::mpsc::channel(32);
                    let (finished_frame_tx, finished_frame_rx) = tokio::sync::mpsc::channel(32);
                    let codec_cancel_token = tokio_util::sync::CancellationToken::new();
                    let cancel = codec_cancel_token.clone();
                    tokio::task::spawn(async move {
                        let mut requests = raw_frame_rx;
                        let finished = finished_frame_tx;
                        let cancel = cancel;
                        loop {
                            tokio::select! {
                                frame = requests.recv() => {
                                    let Some((frame, id)) = frame else {
                                        break;
                                    };

                                    let frame_extent: atom_parser::Extent<u64, u64> = frame;
                                    use atom_parser::AsyncReader;
                                    let frame = reader.async_read_bytes(frame_extent).await.unwrap();
                                    let (pixels, _) = frame.as_chunks::<3>();
                                    let mut premul_output = Vec::with_capacity(frame.len() * 4 / 3);
                                    for [r, g, b] in pixels {
                                        premul_output.extend_from_slice(&[*r, *g, *b, 255]);
                                    }
                                    let frame = premul_output.into_boxed_slice();

                                    if finished.send((frame, id)).await.is_err() {
                                        break;
                                    };
                                }
                                _ = cancel.cancelled() => {
                                    break;
                                }
                            }
                        }
                    });
                    CodecTaskHandle {
                        display_out,
                        raw_frame_in: raw_frame_tx,
                        decoded_frame_out: finished_frame_rx,
                        frame_buffer: Default::default(),
                        codec_cancel_token,
                    }
                }
                _ => return Err(UnsupportedTrackError::UnsupportedCodec),
            }
        }
    })
}


#[derive(Debug)]
struct Track<O> {
    pub kind: quicktime_2001::TrackKind<O>,
    pub time_to_sample: quicktime_2001::stts::v0::TimeToSampleV0<O>,
    pub sample_size: quicktime_2001::stsz::v0::SampleSizeV0<O>,
    pub sample_to_chunk: quicktime_2001::stsc::v0::SampleToChunkV0<O>,
    pub chunk_offset: quicktime_2001::stco::v0::ChunkOffsetV0<O>,
    pub header: quicktime_2001::tkhd::v0::TrackHeaderV0<O>,
}

impl <O: atom_parser::Offset + Unpin> Track<O> {
    // TODO: something simiilar for going backwards
    async fn get_next_frame_info_from_id<R: atom_parser::AsyncReader<O>>(&self, id: u32, mut reader: R, opts: &atom_parser::ParseOptions, movie_header: &quicktime_2001::mvhd::v0::MovieHeaderV0<O>) -> Result<Option<TrackDisplayInfo>, atom_parser::ParseError> {
        let mut time = 0;
        let mut sample_count = 0;
        let mut time_to_sample_iter = self.time_to_sample.time_to_sample_table_async(&mut reader, opts);

        use tokio_stream::StreamExt;
        while let Some(entry) = time_to_sample_iter.next().await {
            let entry = entry?;
            for _ in 0..entry.sample_count {
                time += entry.sample_duration;
                sample_count += 1;
                if sample_count > id {
                    return Ok(Some(TrackDisplayInfo {
                        id: sample_count,
                        time: movie_header.duration_from_step(time),
                    }))
                }
            }
        }
        Ok(None)
    }

    async fn get_next_frame_info_from_time<R: atom_parser::AsyncReader<O>>(&self, current_time: &core::time::Duration, mut reader: R, opts: &atom_parser::ParseOptions, movie_header: &quicktime_2001::mvhd::v0::MovieHeaderV0<O>) -> Result<Option<TrackDisplayInfo>, atom_parser::ParseError> {
        let mut time = 0;
        let mut sample_count = 0;
        let mut time_to_sample_iter = self.time_to_sample.time_to_sample_table_async(&mut reader, opts);

        use tokio_stream::StreamExt;
        while let Some(entry) = time_to_sample_iter.next().await {
            let entry = entry?;
            for _ in 0..entry.sample_count {
                time += entry.sample_duration;
                sample_count += 1;
                // TODO something similar for when playing backwards
                // - just a different check
                if movie_header.duration_from_step(time) > *current_time {
                    return Ok(Some(TrackDisplayInfo {
                        id: sample_count,
                        time: movie_header.duration_from_step(time),
                    }))
                }
            }
        }
        Ok(None)
    }

    async fn get_current_frame_info_from_time<R: atom_parser::AsyncReader<O>>(&self, current_time: &core::time::Duration, mut reader: R, opts: &atom_parser::ParseOptions, movie_header: &quicktime_2001::mvhd::v0::MovieHeaderV0<O>) -> Result<Option<TrackDisplayInfo>, atom_parser::ParseError> {
        let mut time = 0;
        let mut sample_count = 0;
        let mut time_to_sample_iter = self.time_to_sample.time_to_sample_table_async(&mut reader, opts);

        let mut current = None;

        use tokio_stream::StreamExt;
        while let Some(entry) = time_to_sample_iter.next().await {
            let entry = entry?;
            for _ in 0..entry.sample_count {
                current = Some(TrackDisplayInfo {
                    id: sample_count,
                    time: movie_header.duration_from_step(time),
                });

                time += entry.sample_duration;
                sample_count += 1;
                // TODO something similar for when playing backwards
                // - just a different check
                if movie_header.duration_from_step(time) > *current_time {
                    return Ok(current);
                }
            }
        }
        Ok(current)
    }
}

struct MovVideo<O> {
    header: quicktime_2001::mvhd::v0::MovieHeaderV0<O>,
    data: quicktime_2001::MovieData<O>,
    tracks: Vec<Track<O>>,
}

// this is the frame to be displayed next  
// e.g this would be 0 & 0s on initial load,
// then 1 & 16ms with 60fps
#[derive(Debug)]
struct TrackDisplayInfo {
    id: u32,
    time: core::time::Duration,
}

struct ActiveTrack<O> {
    track: Track<O>,
    // if None, the track is assumed to be finished
    next: Option<TrackDisplayInfo>,
}

// each track could have a 'next display frame'

const BUFFER_SIZE: usize = 1024;

// TODO: this should also return the tracks stuff as well
async fn parse_metadata(path: &std::path::Path) -> (atom_parser::AsyncFileReader<std::sync::Arc<std::fs::File>, BUFFER_SIZE>, MovVideo<u64>, Metadata, (tokio::sync::mpsc::Receiver<TrackAction>, tokio::sync::mpsc::Sender<TrackResponse>)) {
    let mut r = {
        let file = tokio::fs::File::open(path).await.unwrap();
        let size = file.metadata().await.unwrap().len();
        let file = std::sync::Arc::new(file.into_std().await);
        atom_parser::reader::AsyncFileReader::<_, BUFFER_SIZE>::new(file, size)
    };
    let opts = Default::default();

    use atom_parser::AsyncParse;
    use quicktime_2001::*;
    let root = Root::parse_async(&mut r, &opts).await.unwrap();
    let mut root_iter = root.children_async(&mut r, &opts);

    let mut tracks = vec![];
    let mut movie_data = None;
    let mut movie_header = None;

    use tokio_stream::StreamExt;
    while let Some(child) = root_iter.next().await {
        let r = root_iter.reader();
        match child.unwrap() {
            root::Child::FileType(ftyp) => (),
            root::Child::Movie(movie) => {
                let mut child_iter = movie.children_async(r, &opts);

                while let Some(child) = child_iter.next().await {
                    let r = child_iter.reader();
                    match child.unwrap() {
                        moov::Child::MovieHeader(hd) => {
                            movie_header = Some(hd);
                        }
                        moov::Child::Clipping(clip) => {
                        }
                        moov::Child::Track(track) => {

                            let mut sample_description = None;
                            let mut time_to_sample = None;
                            let mut sample_sizes = None;
                            let mut sample_to_chunk = None;
                            let mut chunk_offset = None;
                            let mut track_header = None;

                            let mut child_iter = track.children_async(r, &opts);
                            while let Some(child) = child_iter.next().await {
                                let r = child_iter.reader();

                                match child.unwrap() {
                                    trak::Child::TrackHeader(hdr) => {
                                        track_header = Some(hdr)
                                    }
                                    trak::Child::Clipping(c) => (),
                                    trak::Child::TrackMatte(tm) => (),
                                    trak::Child::Edit(e) => {
                                        let mut child_iter = e.children_async(r, &opts);
                                        while let Some(child) = child_iter.next().await {
                                            let r = child_iter.reader();
                                            match child.unwrap() {
                                                edts::Child::EditList(el) => match el.version {
                                                    elst::EditListVersions::V0(el) => {
                                                        let list_entires = el
                                                            .table_entries_async(r, &opts)
                                                            .collect::<Result<Vec<_>, _>>()
                                                            .await
                                                            .unwrap();
                                                    }
                                                    elst::EditListVersions::Unknown(v) => {
                                                    }
                                                },
                                                edts::Child::Unsupported(..) => (),
                                            }
                                        }
                                    }
                                    trak::Child::TrackReference(tref) => {
                                    }
                                    trak::Child::TrackLoadingSettings(tls) => {
                                    }
                                    trak::Child::TrackInputMap(tim) => {
                                    }
                                    trak::Child::Media(m) => {
                                        let mut media_info = None;
                                        let mut handler_ref = None;
                                        let mut sample_desc_table = None;

                                        let mut child_iter = m.children_async(r, &opts);
                                        while let Some(child) = child_iter.next().await {
                                            let r = child_iter.reader();
                                            match child.unwrap() {
                                                mdia::Child::MediaHeader(h) => {
                                                }
                                                mdia::Child::HandlerReference(r) => {
                                                    if handler_ref.as_ref().is_some() {
                                                        panic!("duplicate handler refs");
                                                    }
                                                    match r.version {
                                                        hdlr::HandlerReferenceVersions::V0(v0) => {
                                                            match v0.component_type {
                                                                hdlr::v0::ComponentType::Media(
                                                                    m,
                                                                ) => handler_ref = Some(m.subtype),
                                                                o => panic!(
                                                                    "unknown media type: {o:?}"
                                                                ),
                                                            }
                                                        }
                                                        v => panic!("unknown hdlr version: {v:?}"),
                                                    }
                                                }
                                                mdia::Child::MediaInformation(info) => {

                                                    let DiscoveredMediaInformation {
                                                        kind,
                                                        data_information,
                                                        sample_table
                                                    } = DiscoveredMediaInformation::from_children_async(&info, &mut *r, &opts).await.unwrap();

                                                    if media_info.is_some() {
                                                        panic!("duplicate media infos");
                                                    }
                                                    media_info = Some(kind);

                                                    if let Some(dinfo) = data_information {
                                                        let mut child_iter =
                                                            dinfo.children_async(r, &opts);
                                                        while let Some(child) =
                                                            child_iter.next().await
                                                        {
                                                            let r = child_iter.reader();
                                                            match child.unwrap() {
                                                                dinf::Child::DataReference(dref) => {
                                                                    match dref.version {
                                                                        dref::DataReferenceVersions::V0(dref) => {
                                                                            let mut child_iter = dref.children_async(r, &opts);
                                                                            while let Some(child) = child_iter.next().await {
                                                                                match child.unwrap() {
                                                                                    dref::v0::Child::MacAlias(alis) => (),
                                                                                    dref::v0::Child::MacResource(rsrc) => (),
                                                                                    dref::v0::Child::Url(url) => (),
                                                                                    dref::v0::Child::Unsupported(..) => (),
                                                                                }
                                                                            }
                                                                        }
                                                                        dref::DataReferenceVersions::Unknown(v) => (),
                                                                    }
                                                                }
                                                                dinf::Child::Unsupported(..) => (),
                                                            }
                                                        }
                                                    }

                                                    if let Some(stable) = sample_table {

                                                        let mut child_iter =
                                                            stable.children_async(r, &opts);
                                                        while let Some(child) =
                                                            child_iter.next().await
                                                        {
                                                            let r = child_iter.reader();
                                                            match child.unwrap() {
                                                                stbl::Child::SampleDescription(desc) => {
                                                                    if sample_desc_table.is_some() {
                                                                        panic!("more than 1 stsd atom found in the same minf atom");
                                                                    }
                                                                    sample_desc_table = Some(desc);
                                                                }
                                                                stbl::Child::TimeToSample(tts) => {
                                                                    match &tts.version {
                                                                        stts::TimeToSampleVersions::V0(tts) => {
                                                                            let time_to_sample = tts.time_to_sample_table_async(r, &opts).collect::<Result<Vec<_>, _>>().await.unwrap();
                                                                        }
                                                                        stts::TimeToSampleVersions::Unknown(v) => (),
                                                                    }
                                                                    time_to_sample = Some(tts);
                                                                }
                                                                stbl::Child::SyncSample(sync) => {
                                                                    match sync.version {
                                                                        stss::SyncSampleVersions::V0(sync) => {
                                                                            let sync_samples = sync.sync_sample_table_async(r, &opts).collect::<Result<Vec<_>, _>>().await.unwrap();
                                                                        }
                                                                        stss::SyncSampleVersions::Unknown(v) => (),
                                                                    }
                                                                }
                                                                stbl::Child::SampleToChunk(stc) => {
                                                                    match &stc.version {
                                                                        stsc::SampleToChunkVersions::V0(stc) => {
                                                                            let sample_to_chunk = stc.sample_to_chunk_table_async(r, &opts).collect::<Result<Vec<_>, _>>().await.unwrap();
                                                                        }
                                                                        stsc::SampleToChunkVersions::Unknown(v) => (),
                                                                    }
                                                                    sample_to_chunk = Some(stc);
                                                                }
                                                                stbl::Child::SampleSize(ss) => {
                                                                    match &ss.version {
                                                                        stsz::SampleSizeVersions::V0(ss) => {
                                                                            match &ss.sample_size {
                                                                                stsz::v0::SampleSize::Uniform(u) => (),
                                                                                stsz::v0::SampleSize::Table(st) => {
                                                                                    let sample_sizes = st.sample_size_table_async(r, &opts).collect::<Result<Vec<_>, _>>().await.unwrap();
                                                                                }
                                                                            }
                                                                        }
                                                                        stsz::SampleSizeVersions::Unknown(v) => (),
                                                                    }
                                                                    sample_sizes = Some(ss);
                                                                }
                                                                stbl::Child::ChunkOffset(co) => {
                                                                    match &co.version {
                                                                        stco::ChunkOffsetVersions::V0(co) => {
                                                                            let offsets = co.chunk_offset_table_async(r, &opts).collect::<Result<Vec<_>, _>>().await.unwrap();
                                                                        }
                                                                        stco::ChunkOffsetVersions::Unknown(v) => (),
                                                                    }
                                                                    chunk_offset = Some(co);
                                                                }
                                                                stbl::Child::Unsupported(u) => (),
                                                            }
                                                        }
                                                    }
                                                }
                                                mdia::Child::Userdata(u) => {
                                                    let mut child_iter = u.children_async(r, &opts);
                                                    while let Some(child) = child_iter.next().await
                                                    {
                                                    }
                                                }
                                                mdia::Child::Unsupported(_) => (),
                                            }
                                        }
                                        drop(child_iter);

                                        if let (Some(media_info), Some(sample_desc_table)) =
                                            (media_info, sample_desc_table)
                                        {
                                            let stsd = match sample_desc_table.version {
                                                stsd::SampleDescriptionVersions::V0(stsd) => stsd,
                                                v => panic!("unknown stsd version: {v:?}"),
                                            };
                                            let mut desc_iter = stsd.descriptions_async(r, &opts);
                                            match (media_info, handler_ref) {
                                                (
                                                    MediaKind::Video {
                                                        header,
                                                        handler_ref,
                                                    },
                                                    Some(MediaComponentSubtype::Video) | None,
                                                ) => {
                                                    let mut entries = vec![];
                                                    while let Some(desc) = desc_iter.next().await {
                                                        let desc = desc.unwrap();
                                                        let entry = desc
                                                            .content
                                                            .as_video_entry_async(
                                                                desc.fourcc,
                                                                desc_iter.reader(),
                                                                &opts,
                                                            )
                                                            .await
                                                            .unwrap();
                                                        let mut extension_iter =
                                                            entry.entry.description.children_async(
                                                                desc_iter.reader(),
                                                                &opts,
                                                            );

                                                        while let Some(extension) =
                                                            extension_iter.next().await
                                                        {
                                                            let extension = extension.unwrap();
                                                        }
                                                        entries.push(entry);
                                                    }
                                                    sample_description =
                                                        Some(TrackKind::Video(VideoTrack {
                                                            entries,
                                                        }));
                                                }
                                                (
                                                    MediaKind::Audio {
                                                        header,
                                                        handler_ref,
                                                    },
                                                    Some(MediaComponentSubtype::Audio) | None,
                                                ) => {
                                                    let mut entries = vec![];
                                                    while let Some(desc) = desc_iter.next().await {
                                                        let desc = desc.unwrap();
                                                        let entry = desc
                                                            .content
                                                            .as_audio_entry_async(
                                                                desc.fourcc,
                                                                desc_iter.reader(),
                                                                &opts,
                                                            )
                                                            .await
                                                            .unwrap();
                                                        entries.push(entry);
                                                    }
                                                    sample_description =
                                                        Some(TrackKind::Audio(AudioTrack {
                                                            entries,
                                                        }));
                                                }
                                                (
                                                    MediaKind::Base {
                                                        header,
                                                        info,
                                                        handler_ref,
                                                        timecode: Some(timecode),
                                                    },
                                                    Some(MediaComponentSubtype::Timecode),
                                                ) => {
                                                    while let Some(desc) = desc_iter.next().await {
                                                        let entry = desc
                                                            .unwrap()
                                                            .content
                                                            .as_timecode_description_async(
                                                                desc_iter.reader(),
                                                                &opts,
                                                            )
                                                            .await
                                                            .unwrap();
                                                    }
                                                }
                                                (
                                                    MediaKind::Base {
                                                        header,
                                                        info,
                                                        handler_ref,
                                                        timecode: None,
                                                    },
                                                    Some(subtype),
                                                ) if subtype != MediaComponentSubtype::Timecode => {
                                                    match subtype {
                                                        MediaComponentSubtype::Text => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_text_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Music => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_music_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Mpeg => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_mpeg_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Sprite => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_sprite_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Tween => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_tween_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Q3d => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_3d_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Streaming => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_streaming_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Hint => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_hint_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Qtvr => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_vr_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Panoramic => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_panoramic_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        MediaComponentSubtype::Object => {
                                                            while let Some(desc) =
                                                                desc_iter.next().await
                                                            {
                                                                let entry = desc
                                                                    .unwrap()
                                                                    .content
                                                                    .as_object_description_async(
                                                                        desc_iter.reader(),
                                                                        &opts,
                                                                    )
                                                                    .await
                                                                    .unwrap();
                                                            }
                                                        }
                                                        o => (),
                                                    }
                                                }
                                                o => panic!("bad media format: {o:?}"),
                                            }
                                        }
                                    }
                                    trak::Child::Userdata(udata) => {
                                        let mut child_iter = udata.children_async(r, &opts);
                                        while let Some(child) = child_iter.next().await {
                                        }
                                    }
                                    trak::Child::Unsupported(fcc) => {
                                    }
                                }
                            }

                            if let (
                                Some(kind),
                                Some(time_to_sample),
                                Some(sample_size),
                                Some(sample_to_chunk),
                                Some(chunk_offset),
                                Some(header),
                            ) = (
                                sample_description,
                                time_to_sample,
                                sample_sizes,
                                sample_to_chunk,
                                chunk_offset,
                                track_header,
                            ) {
                                let header = match header.version {
                                    quicktime_2001::tkhd::TrackHeaderVersions::V0(v0) => v0,
                                    _ => panic!(),
                                };


                                let time_to_sample = match time_to_sample.version {
                                    stts::TimeToSampleVersions::V0(stts) => stts,
                                    _ => panic!(),
                                };
                                let sample_size = match sample_size.version {
                                    stsz::SampleSizeVersions::V0(stsz) => stsz,
                                    _ => panic!(),
                                };
                                let sample_to_chunk = match sample_to_chunk.version {
                                    stsc::SampleToChunkVersions::V0(stsc) => stsc,
                                    _ => panic!(),
                                };
                                let chunk_offset = match chunk_offset.version {
                                    stco::ChunkOffsetVersions::V0(stco) => stco,
                                    _ => panic!(),
                                };

                                tracks.push(self::Track {
                                    kind,
                                    time_to_sample,
                                    sample_size,
                                    sample_to_chunk,
                                    chunk_offset,
                                    header,
                                })
                            }
                        }
                        moov::Child::Userdata(udta) => {
                            let mut child_iter = udta.children_async(r, &opts);
                            while let Some(child) = child_iter.next().await {
                            }
                        }
                        moov::Child::ColorTable(ctb) => {
                        }
                        moov::Child::Unsupported(missed) => {
                        }
                        moov::Child::CompressedMovie(..) | moov::Child::ReferenceMovie(..) => (),
                    }
                }
            }
            root::Child::MovieData(mdat) => {
                movie_data = Some(mdat);
            }
            root::Child::Unsupported(a) => (),
        }
    }
    drop(root_iter);

    let header = match movie_header.unwrap().version {
        quicktime_2001::mvhd::MovieHeaderVersions::V0(v0) => v0,
        _ => panic!(),
    };

    let video = MovVideo {
        header,
        data: movie_data.unwrap(),
        tracks,
    };
    let time_granularity = core::time::Duration::from_nanos(1000000000 / (video.header.time_scale as u64));

    // TODO: 
    // each video track has its own width + height ?
    // - like a channel is setup with a VideoTrackInfo { width: u32, height: u32, frames: tokio::mpsc::Receiver<DisplayBuffer<A>> }
    //   - need a way to configure which channels are selected without (preferrably) needing to have
    //   a channel open
    //      - this can be done by sending the enumerated video/audio tracks, then creating a channel if its opened
    //          - like sending a OpenTrack(tokio::mpsc::Sender<DisplayBuffer>) and keeping the
    //          Receiver on the thread that is configuring the tracks
    //              - then the thing that displays the a/v can decide what to do
    // - the container also has to manage the current display time of the video
    //      - can set a AtomicU64 for the current ns of the time
    //          - this can then be truncated by the display if desired
    //      - the duration also needs to be updated based on when pause is pressed
    //          - e.g if it pauses in the middle of a duration of a frame then it only has half of
    //          the duration left
    //          - also the player needs to act differently when played vs paused
    //              - like when paused it wont request any frames unless jogged but otherwise waits
    //              until play
    //              - when in play it will constantly request frames
    //              - or at least pause isnt as aggressive as play in terms of requesting frames
    //
    // - add a duration to the metadata

    let duration = video.header.duration();

    let tracks = video.tracks.iter().filter_map(|track| {
        use quicktime_2001::{AudioTrack, VideoTrack};
        Some(match &track.kind {
            quicktime_2001::TrackKind::Audio(_) => {
                crate::player::container::TrackKind::Audio(crate::player::container::Track {
                    id: track.header.track_id,
                    selected: false,
                    kind: crate::player::container::track::Audio {
                    }
                })
            }
            quicktime_2001::TrackKind::Video(VideoTrack { entries }) => {
                let entry = entries.first().unwrap();
                crate::player::container::TrackKind::Video(crate::player::container::Track {
                    id: track.header.track_id,
                    selected: false,
                    kind: crate::player::container::track::Video {
                        width: entry.entry.description.width as _,
                        height: entry.entry.description.height as _,
                    }
                })
            }
        })
    }).collect::<Vec<_>>();

    let (track_cmd_tx, track_cmd_rx) = tokio::sync::mpsc::channel(32);
    let (track_recv_tx, track_recv_rx) = tokio::sync::mpsc::channel(32);

    (r, video, Metadata {
        time_granularity,
        duration,
        tracks,
        channels: Some(TrackChannels {
            tx: track_cmd_tx,
            rx: track_recv_rx,
        })
    }, (track_cmd_rx, track_recv_tx))
}

struct OpenTracks<'a> {
    tracks: &'a mut std::collections::BTreeMap<u32, CodecTaskHandle>,
}

impl <'a> OpenTracks<'a> {
    fn new(tracks: &'a mut std::collections::BTreeMap<u32, CodecTaskHandle>) -> Self {
        Self {
            tracks,
        }
    }
}

impl <'a> Future for OpenTracks<'a> {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut any_ready = false;
        for (_, track) in self.tracks.iter_mut() {
            if let core::task::Poll::Ready(Some((buffer, id))) = std::pin::Pin::new(&mut track.decoded_frame_out).poll_recv(cx) {
                track.frame_buffer.insert(id, buffer);
                any_ready = true;
            }
        }

        if any_ready {
            core::task::Poll::Ready(())
        } else {
            core::task::Poll::Pending
        }
    }
}

