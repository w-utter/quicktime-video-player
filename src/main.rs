pub mod window;
pub mod app;
pub mod player;

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (ev_loop, mut window_manger) = rt.block_on(window::WindowHandler::new(app::GlobalState::new(&rt)));
    ev_loop.run_app(&mut window_manger).unwrap()
}
