use crate::app::GlobalState;
use egui_wgpu::wgpu;
use egui_winit::winit;
use std::collections::HashMap;
use std::sync::Arc;

pub struct WgpuState {
    pub adapter: wgpu::Adapter,
    pub device: Arc<wgpu::Device>,
    pub queue: Arc<wgpu::Queue>,
    pub instance: wgpu::Instance,
}

impl WgpuState {
    fn create_window(
        &mut self,
        event_loop: &winit::event_loop::ActiveEventLoop,
        kind: crate::app::WindowKind,
        fullscreen: Option<winit::window::Fullscreen>,
        proxy: &winit::event_loop::EventLoopProxy<UserEvent>,
    ) -> UiWindow {
        use crate::app::WindowKind;
        let (inner_size, min_inner_size) = match kind {
            WindowKind::Main => (
                winit::dpi::LogicalSize::new(640f64, 400f64),
                winit::dpi::LogicalSize::new(100f64, 200f64),
            ),
            _ => (
                winit::dpi::LogicalSize::new(320f64, 200f64),
                winit::dpi::LogicalSize::new(320f64, 200f64),
            ),
        };

        let builder = winit::window::WindowAttributes::default()
            .with_title(String::from(""))
            .with_visible(false)
            .with_min_inner_size(min_inner_size)
            .with_inner_size(inner_size)
            .with_fullscreen(fullscreen);

        #[cfg(all(unix, not(target_os = "macos")))]
        let builder = {
            use winit::platform::{wayland, x11};
            let builder = wayland::WindowAttributesExtWayland::with_name(builder, class, class);
            x11::WindowAttributesExtX11::with_name(builder, class, class)
        };
        let window = Arc::new(event_loop.create_window(builder).unwrap());
        let size = window.inner_size();

        let surface = self.instance.create_surface(window.clone()).unwrap();

        let surface_caps = surface.get_capabilities(&self.adapter);
        let surface_format = surface_caps
            .formats
            .iter()
            .copied()
            .find(|f| f.is_srgb())
            .unwrap_or(surface_caps.formats[0]);

        let config = wgpu::SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format: surface_format,
            width: size.width,
            height: size.height,
            present_mode: wgpu::PresentMode::AutoVsync,
            alpha_mode: wgpu::CompositeAlphaMode::Auto,
            view_formats: Vec::new(),
            desired_maximum_frame_latency: 2,
        };
        surface.configure(&self.device, &config);

        let viewport_id = match kind {
            WindowKind::Main => egui::ViewportId::ROOT,
            _ => egui::ViewportId::from_hash_of(window.id()),
        };

        let mut egui_winit = egui_winit::State::new(
            egui::Context::default(),
            viewport_id,
            &window,
            None,
            None,
            None,
        );
        let limits = wgpu::Limits::default();
        egui_winit.set_max_texture_side(limits.max_texture_dimension_2d as usize);

        let egui_renderer = egui_wgpu::Renderer::new(
            &self.device,
            surface_format,
            egui_wgpu::RendererOptions::default(),
        );

        {
            let repaint_proxy = proxy.clone();
            let window_proxy = window.clone();
            egui_winit
                .egui_ctx()
                .set_request_repaint_callback(move |info| {
                    let _ = repaint_proxy
                        .send_event(UserEvent::RepaintRequest(info, Some(window_proxy.id())));
                });
        }

        let window_state = crate::app::WindowState::from_window_kind(kind, egui_winit.egui_ctx());

        UiWindow {
            egui_state: egui_winit,
            renderer: egui_renderer,
            surface,
            window,
            surface_config: config,
            scale_factor: 1.,
            window_state,
            shapes: Vec::new(),
            repaint_delay: std::time::Duration::MAX,
        }
    }
}

pub struct UiWindow {
    egui_state: egui_winit::State,
    renderer: egui_wgpu::Renderer,
    surface: wgpu::Surface<'static>,
    surface_config: wgpu::SurfaceConfiguration,
    window: Arc<winit::window::Window>,
    scale_factor: f64,
    window_state: crate::app::WindowState,
    shapes: Vec<egui::ClippedPrimitive>,
    repaint_delay: std::time::Duration,
}

impl UiWindow {
    fn id(&self) -> winit::window::WindowId {
        self.window.id()
    }

    fn is_root(&self) -> bool {
        self.window_state.is_root()
    }
}

pub struct WindowHandler<'a> {
    pub wgpu: WgpuState,
    pub proxy: winit::event_loop::EventLoopProxy<UserEvent>,
    pub windows: HashMap<winit::window::WindowId, UiWindow>,
    pub global_state: GlobalState<'a>,
}

impl<'a> WindowHandler<'a> {
    pub async fn new(
        global_state: GlobalState<'a>,
    ) -> (winit::event_loop::EventLoop<UserEvent>, Self) {
        let event_loop: winit::event_loop::EventLoop<UserEvent> =
            winit::event_loop::EventLoop::with_user_event()
                .build()
                .unwrap();
        let proxy = event_loop.create_proxy();

        let backends = if cfg!(windows) {
            wgpu::Backends::DX12
        } else if cfg!(target_os = "macos") {
            wgpu::Backends::PRIMARY
        } else {
            wgpu::Backends::all()
        };

        let instance_descriptor = wgpu::InstanceDescriptor {
            backends,
            ..Default::default()
        };

        let instance = wgpu::Instance::new(&instance_descriptor);
        //let surface = instance.create_surface(window.clone()).unwrap();
        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
            })
            .await
            .expect("Unable to create adapter");

        let limits = wgpu::Limits::default();
        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: None,
                required_features: wgpu::Features::default(),
                required_limits: limits.clone(),
                memory_hints: wgpu::MemoryHints::default(),
                trace: wgpu::Trace::Off,
                experimental_features: wgpu::ExperimentalFeatures::disabled(),
            })
            .await
            .unwrap();

        let wgpu = WgpuState {
            adapter,
            device: device.into(),
            queue: queue.into(),
            instance,
        };

        let ctrl_proxy = proxy.clone();
        ctrlc::set_handler(move || {
            let _ = ctrl_proxy.send_event(UserEvent::Exit);
        })
        .unwrap();

        (
            event_loop,
            Self {
                proxy,
                wgpu,
                windows: HashMap::new(),
                global_state,
            },
        )
    }

    fn ui_ctrl_flow(
        &mut self,
        ctrl: crate::app::UiRequest,
        event_loop: &winit::event_loop::ActiveEventLoop,
    ) {
    }
}

impl<'a> winit::application::ApplicationHandler<UserEvent> for WindowHandler<'a> {
    fn new_events(
        &mut self,
        event_loop: &winit::event_loop::ActiveEventLoop,
        cause: winit::event::StartCause,
    ) {
        use winit::event::StartCause;

        match cause {
            StartCause::Init => {
                let window = self.wgpu.create_window(
                    event_loop,
                    crate::app::WindowKind::Main,
                    None,
                    &self.proxy,
                );
                let id = window.id();
                self.windows.insert(id, window);
            }
            _ => (),
        }
    }

    fn resumed(&mut self, _: &winit::event_loop::ActiveEventLoop) {
        for window in self.windows.values_mut() {
            window.window.set_visible(true)
        }
    }

    fn window_event(
        &mut self,
        event_loop: &winit::event_loop::ActiveEventLoop,
        id: winit::window::WindowId,
        event: winit::event::WindowEvent,
    ) {
        use winit::event::WindowEvent;

        let Some(window) = self.windows.get_mut(&id) else {
            return;
        };

        match event {
            WindowEvent::CloseRequested => {
                if window.is_root() {
                    let _ = self.proxy.send_event(UserEvent::Exit);
                } else {
                    let _ = self.windows.remove(&id);
                }
            }

            WindowEvent::ScaleFactorChanged { scale_factor, .. } => {
                window.scale_factor = scale_factor;
            }
            WindowEvent::Resized(size) => {
                if size.width > 0 && size.height > 0 {
                    window.surface_config.width = size.width;
                    window.surface_config.height = size.height;
                    window
                        .surface
                        .configure(&self.wgpu.device, &window.surface_config);
                    //app.resize(size);
                }
            }
            WindowEvent::RedrawRequested => {
                //let mut ctrl_flow = None;
                {
                    let raw_input = window.egui_state.take_egui_input(&window.window);
                    let egui_output = window.egui_state.egui_ctx().run(raw_input, |ctx| {
                        if let Some(ctrl_flow) = window.window_state.update(
                            ctx,
                            &mut self.global_state,
                            &self.wgpu,
                            &mut window.renderer,
                            &self.proxy,
                        ) {
                            let _ = self.proxy.send_event(UserEvent::Ui(ctrl_flow));
                        }
                    });

                    window
                        .egui_state
                        .handle_platform_output(&window.window, egui_output.platform_output);
                    for (id, image_delta) in egui_output.textures_delta.set {
                        window.renderer.update_texture(
                            &self.wgpu.device,
                            &self.wgpu.queue,
                            id,
                            &image_delta,
                        );
                    }

                    for id in egui_output.textures_delta.free {
                        window.renderer.free_texture(&id);
                    }

                    let pixels_per_point = window.egui_state.egui_ctx().pixels_per_point();
                    window.shapes = window
                        .egui_state
                        .egui_ctx()
                        .tessellate(egui_output.shapes, pixels_per_point);
                }

                let output = window.surface.get_current_texture().unwrap();
                /*
                use wgpu::CurrentSurfaceTexture;
                let output = match window.surface.get_current_texture() {
                    CurrentSurfaceTexture::Success(texture) => texture,
                    CurrentSurfaceTexture::Suboptimal(texture) => texture,
                    _ => {
                        let control_flow =
                            winit::event_loop::ControlFlow::wait_duration(window.repaint_delay);
                        event_loop.set_control_flow(control_flow);
                        return;
                    }
                };
                */

                let view = output
                    .texture
                    .create_view(&wgpu::TextureViewDescriptor::default());
                let mut encoder =
                    self.wgpu
                        .device
                        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                            label: Some("Render Encoder"),
                        });

                {
                    {
                        let screen_descriptor = egui_wgpu::ScreenDescriptor {
                            pixels_per_point: window.window.scale_factor() as f32,
                            size_in_pixels: [
                                window.surface_config.width,
                                window.surface_config.height,
                            ],
                        };

                        let cmd_buffers = window.renderer.update_buffers(
                            &self.wgpu.device,
                            &self.wgpu.queue,
                            &mut encoder,
                            &window.shapes,
                            &screen_descriptor,
                        );
                        self.wgpu.queue.submit(cmd_buffers);

                        let mut pass = encoder
                            .begin_render_pass(&wgpu::RenderPassDescriptor {
                                label: Some("Gui Render Pass"),
                                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                                    depth_slice: None,
                                    view: &view,
                                    resolve_target: None,
                                    ops: wgpu::Operations {
                                        load: wgpu::LoadOp::Load,
                                        store: wgpu::StoreOp::Store,
                                    },
                                })],
                                depth_stencil_attachment: None,
                                occlusion_query_set: None,
                                timestamp_writes: None,
                            })
                            .forget_lifetime();

                        window
                            .renderer
                            .render(&mut pass, &window.shapes, &screen_descriptor);
                    }

                    self.wgpu.queue.submit(std::iter::once(encoder.finish()));
                    window.window.pre_present_notify();
                    output.present();
                }

                let control_flow =
                    winit::event_loop::ControlFlow::wait_duration(window.repaint_delay);
                event_loop.set_control_flow(control_flow);
            }
            ev => {
                /*
                match &ev {
                        WindowEvent::ModifiersChanged(modifiers) => {
                            //app.modifiers = modifiers.state()
                        }
                        WindowEvent::KeyboardInput { event, .. } => {
                        }
                }
                */

                let res = window.egui_state.on_window_event(&window.window, &ev);

                if !res.consumed || matches!(ev, WindowEvent::ModifiersChanged(_)) {
                    //app.handle_window_event(&wgpu, &event);
                }

                if res.repaint {
                    window.window.request_redraw();
                }
            }
        }
    }

    fn exiting(&mut self, _event_loop: &winit::event_loop::ActiveEventLoop) {
        for window in self.windows.values_mut() {
            window.window.set_visible(false);
        }
        std::process::exit(0);
    }

    fn user_event(&mut self, event_loop: &winit::event_loop::ActiveEventLoop, event: UserEvent) {
        match event {
            UserEvent::Exit => event_loop.exit(),
            UserEvent::RepaintRequest(_info, id) => {
                if let Some(id) = id {
                    if let Some(window) = self.windows.get_mut(&id) {
                        window.window.request_redraw();
                    }
                } else {
                    for (_, window) in &mut self.windows {
                        window.window.request_redraw();
                    }
                }
            }
            UserEvent::Ui(ctrl) => self.ui_ctrl_flow(ctrl, event_loop),
        }
    }
}

pub enum UserEvent {
    RepaintRequest(egui::RequestRepaintInfo, Option<winit::window::WindowId>),
    Exit,
    Ui(crate::app::UiRequest),
}
