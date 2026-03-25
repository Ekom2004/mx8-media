use std::env;
use std::process::{Command, Stdio};
use std::time::Instant;

use mx8_find_media::{
    extract_sampled_rgb_frames_filter_graph_profiled,
    extract_sampled_rgb_frames_filter_graph_profiled_with_discard,
    extract_sampled_rgb_frames_manual_profiled, extract_windowed_rgb_frames_profiled,
    DecodeDiscardMode, ExtractFramesRequest, ExtractFramesStats, ExtractWindow,
    ExtractWindowedFramesRequest,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse()?;
    let request = ExtractFramesRequest {
        source_uri: args.source,
        scan_start_ms: args.start_ms,
        scan_end_ms: args.end_ms,
        sample_fps: args.sample_fps,
        frame_width: args.frame_width,
        frame_height: args.frame_height,
    };

    print_bench("libav", args.runs, || bench_libav(&request))?;
    print_bench("libav-filtergraph", args.runs, || {
        bench_libav_filter_graph(&request)
    })?;
    print_bench("libav-filtergraph-nonref", args.runs, || {
        bench_libav_filter_graph_nonref(&request)
    })?;
    print_bench("libav-filtergraph-nonkey", args.runs, || {
        bench_libav_filter_graph_nonkey(&request)
    })?;
    print_bench("ffmpeg-cli", args.runs, || bench_ffmpeg_cli(&request))?;
    if args.session_chunks > 1 {
        print_bench("libav-filtergraph-nonref-reopen-session", args.runs, || {
            bench_nonref_reopen_session(&request, args.session_chunks)
        })?;
        print_bench(
            "libav-filtergraph-nonref-persistent-session",
            args.runs,
            || bench_nonref_persistent_session(&request, args.session_chunks),
        )?;
    }
    Ok(())
}

fn print_bench<F>(label: &str, runs: u32, mut bench: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Result<BenchResult, Box<dyn std::error::Error>>,
{
    let mut total_ms = 0.0_f64;
    let mut frame_count = 0_usize;
    let mut run_ms = Vec::with_capacity(runs as usize);
    let mut last_stats = None;
    for _ in 0..runs {
        let result = bench()?;
        frame_count = result.frames;
        total_ms += result.elapsed_ms;
        run_ms.push(result.elapsed_ms);
        last_stats = result.stats;
    }
    let avg_ms = total_ms / runs as f64;
    println!(
        "{label}: avg_elapsed_ms={avg_ms:.2} frames={frame_count} runs_ms={:?}",
        format_runs(&run_ms)
    );
    if let Some(stats) = last_stats {
        println!("{label}-stats: {}", format_stats(&stats));
    }
    Ok(())
}

fn bench_libav(request: &ExtractFramesRequest) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let (response, stats) = extract_sampled_rgb_frames_manual_profiled(request)?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: response.frames.len(),
        elapsed_ms,
        stats: Some(stats),
    })
}

fn bench_ffmpeg_cli(
    request: &ExtractFramesRequest,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let scan_duration_secs =
        ((request.scan_end_ms - request.scan_start_ms) as f64 / 1000.0).max(0.001);
    let filter = format!(
        "fps={},scale={}:{}:force_original_aspect_ratio=decrease,pad={}:{}:(ow-iw)/2:(oh-ih)/2:black",
        request.sample_fps,
        request.frame_width,
        request.frame_height,
        request.frame_width,
        request.frame_height
    );
    let output = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin")
        .arg("-ss")
        .arg(format!("{:.3}", request.scan_start_ms as f64 / 1000.0))
        .arg("-t")
        .arg(format!("{scan_duration_secs:.3}"))
        .arg("-i")
        .arg(request.source_uri.as_str())
        .arg("-vf")
        .arg(filter)
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg("-vsync")
        .arg("vfr")
        .arg("-f")
        .arg("rawvideo")
        .arg("pipe:1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("ffmpeg benchmark failed: {}", stderr.trim()).into());
    }
    let frame_bytes = (request.frame_width * request.frame_height * 3) as usize;
    let frame_count = output.stdout.len() / frame_bytes;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: frame_count,
        elapsed_ms,
        stats: None,
    })
}

fn bench_libav_filter_graph(
    request: &ExtractFramesRequest,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let (response, stats) = extract_sampled_rgb_frames_filter_graph_profiled(request)?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: response.frames.len(),
        elapsed_ms,
        stats: Some(stats),
    })
}

fn bench_libav_filter_graph_nonref(
    request: &ExtractFramesRequest,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let (response, stats) = extract_sampled_rgb_frames_filter_graph_profiled_with_discard(
        request,
        DecodeDiscardMode::NonRef,
    )?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: response.frames.len(),
        elapsed_ms,
        stats: Some(stats),
    })
}

fn bench_libav_filter_graph_nonkey(
    request: &ExtractFramesRequest,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let (response, stats) = extract_sampled_rgb_frames_filter_graph_profiled_with_discard(
        request,
        DecodeDiscardMode::NonKey,
    )?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: response.frames.len(),
        elapsed_ms,
        stats: Some(stats),
    })
}

fn bench_nonref_reopen_session(
    request: &ExtractFramesRequest,
    session_chunks: u32,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut total_frames = 0_usize;
    let chunk_ms = request.scan_end_ms - request.scan_start_ms;
    let mut total_stats = ExtractFramesStats::default();
    for window in build_windows(request, session_chunks) {
        let chunk_request = ExtractFramesRequest {
            source_uri: request.source_uri.clone(),
            scan_start_ms: window.scan_start_ms,
            scan_end_ms: window.scan_end_ms,
            sample_fps: request.sample_fps,
            frame_width: request.frame_width,
            frame_height: request.frame_height,
        };
        let (response, stats) = extract_sampled_rgb_frames_filter_graph_profiled_with_discard(
            &chunk_request,
            DecodeDiscardMode::NonRef,
        )?;
        total_frames += response.frames.len();
        accumulate_stats(&mut total_stats, &stats);
        if chunk_request.scan_end_ms - chunk_request.scan_start_ms != chunk_ms {
            return Err("chunk duration changed unexpectedly".into());
        }
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchResult {
        frames: total_frames,
        elapsed_ms,
        stats: Some(total_stats),
    })
}

fn bench_nonref_persistent_session(
    request: &ExtractFramesRequest,
    session_chunks: u32,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let windowed_request = ExtractWindowedFramesRequest {
        source_uri: request.source_uri.clone(),
        windows: build_windows(request, session_chunks),
        sample_fps: request.sample_fps,
        frame_width: request.frame_width,
        frame_height: request.frame_height,
    };
    let (response, stats) = extract_windowed_rgb_frames_profiled(&windowed_request)?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let total_frames = response
        .windows
        .iter()
        .map(|window| window.frames.len())
        .sum();
    Ok(BenchResult {
        frames: total_frames,
        elapsed_ms,
        stats: Some(stats),
    })
}

fn format_runs(values: &[f64]) -> Vec<String> {
    values.iter().map(|value| format!("{value:.2}")).collect()
}

fn build_windows(request: &ExtractFramesRequest, session_chunks: u32) -> Vec<ExtractWindow> {
    let chunk_ms = request.scan_end_ms - request.scan_start_ms;
    (0..session_chunks)
        .map(|index| {
            let start_ms = request.scan_start_ms + (chunk_ms * index as u64);
            ExtractWindow {
                scan_start_ms: start_ms,
                scan_end_ms: start_ms + chunk_ms,
            }
        })
        .collect()
}

fn format_stats(stats: &ExtractFramesStats) -> String {
    format!(
        "packets_read={} packets_sent={} decoded_frames={} filtered_frames={} kept_frames={} skipped_frames={} open_input_ms={:.2} stream_info_ms={:.2} codec_setup_ms={:.2} seek_ms={:.2} read_packets_ms={:.2} send_packets_ms={:.2} receive_frames_ms={:.2} filter_graph_ms={:.2} scale_and_pad_ms={:.2}",
        stats.packets_read,
        stats.packets_sent,
        stats.decoded_frames,
        stats.filtered_frames,
        stats.kept_frames,
        stats.skipped_frames,
        duration_ms(stats.open_input),
        duration_ms(stats.stream_info),
        duration_ms(stats.codec_setup),
        duration_ms(stats.seek),
        duration_ms(stats.read_packets),
        duration_ms(stats.send_packets),
        duration_ms(stats.receive_frames),
        duration_ms(stats.filter_graph),
        duration_ms(stats.scale_and_pad),
    )
}

fn duration_ms(value: std::time::Duration) -> f64 {
    value.as_secs_f64() * 1000.0
}

fn accumulate_stats(total: &mut ExtractFramesStats, add: &ExtractFramesStats) {
    total.open_input += add.open_input;
    total.stream_info += add.stream_info;
    total.codec_setup += add.codec_setup;
    total.seek += add.seek;
    total.read_packets += add.read_packets;
    total.send_packets += add.send_packets;
    total.receive_frames += add.receive_frames;
    total.filter_graph += add.filter_graph;
    total.scale_and_pad += add.scale_and_pad;
    total.packets_read += add.packets_read;
    total.packets_sent += add.packets_sent;
    total.decoded_frames += add.decoded_frames;
    total.filtered_frames += add.filtered_frames;
    total.kept_frames += add.kept_frames;
    total.skipped_frames += add.skipped_frames;
}

struct BenchResult {
    frames: usize,
    elapsed_ms: f64,
    stats: Option<ExtractFramesStats>,
}

struct Args {
    source: String,
    start_ms: u64,
    end_ms: u64,
    sample_fps: f32,
    frame_width: u32,
    frame_height: u32,
    runs: u32,
    session_chunks: u32,
}

impl Args {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut source = None;
        let mut start_ms = None;
        let mut end_ms = None;
        let mut sample_fps = None;
        let mut frame_width = None;
        let mut frame_height = None;
        let mut runs = 1_u32;
        let mut session_chunks = 1_u32;

        let mut args = env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--source" => source = Some(args.next().ok_or("missing value for --source")?),
                "--start-ms" => {
                    start_ms = Some(args.next().ok_or("missing value for --start-ms")?.parse()?)
                }
                "--end-ms" => {
                    end_ms = Some(args.next().ok_or("missing value for --end-ms")?.parse()?)
                }
                "--sample-fps" => {
                    sample_fps = Some(
                        args.next()
                            .ok_or("missing value for --sample-fps")?
                            .parse()?,
                    )
                }
                "--frame-width" => {
                    frame_width = Some(
                        args.next()
                            .ok_or("missing value for --frame-width")?
                            .parse()?,
                    )
                }
                "--frame-height" => {
                    frame_height = Some(
                        args.next()
                            .ok_or("missing value for --frame-height")?
                            .parse()?,
                    )
                }
                "--runs" => runs = args.next().ok_or("missing value for --runs")?.parse()?,
                "--session-chunks" => {
                    session_chunks = args
                        .next()
                        .ok_or("missing value for --session-chunks")?
                        .parse()?
                }
                other => return Err(format!("unknown flag: {other}").into()),
            }
        }

        Ok(Self {
            source: source.ok_or("missing --source")?,
            start_ms: start_ms.ok_or("missing --start-ms")?,
            end_ms: end_ms.ok_or("missing --end-ms")?,
            sample_fps: sample_fps.ok_or("missing --sample-fps")?,
            frame_width: frame_width.ok_or("missing --frame-width")?,
            frame_height: frame_height.ok_or("missing --frame-height")?,
            runs,
            session_chunks,
        })
    }
}
