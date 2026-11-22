//! Measure actual unpolled future storage; none of these constructors authenticate or contact a native database.
use super::super::retained_activity::Original;
use super::*;
use std::future::Future;
fn sizes<A, F: Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
    (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
}
fn measure<F: Future>(future: &F) -> (usize, usize) {
    (std::mem::size_of_val(future), std::mem::size_of::<F::Output>())
}
#[test]
fn actual_queue_body_setup_retirement_and_boxed_capture_boundaries_have_finite_storage_without_native_work() {
    let mut fixture = MaintenanceFixture::unconnected_relation_future_fixture();
    let future = prove(&mut fixture);
    let body = measure(&future);
    drop(future);
    let future = fixture.prepare_relation_queue();
    let setup = measure(&future);
    drop(future);
    let future = fixture.retire_relation_queue();
    let retirement = measure(&future);
    drop(future);
    type Borrow = &'static mut MaintenanceFixture;
    type Focus = &'static mut Original;
    let original = sizes(|fixture: Borrow| captures::original(fixture));
    let focused = sizes(|(fixture, original): (Borrow, Focus)| captures::blocking(fixture, original, false));
    let partial = sizes(|(fixture, original): (Borrow, Focus)| captures::blocking(fixture, original, true));
    for (name, storage, output) in [
        ("actual body", body.0, body.1),
        ("actual setup", setup.0, setup.1),
        ("actual retirement", retirement.0, retirement.1),
        ("boxed original", original.0, original.1),
        ("boxed focused", focused.0, focused.1),
        ("boxed partial", partial.0, partial.1),
    ] {
        eprintln!("owned relation fixture {name}: future={storage}, output={output}");
        assert!(storage <= 4096, "{name} future stores {storage} bytes");
        assert!(output <= 128, "{name} output stores {output} bytes");
    }
    assert_eq!(fixture.relation_unpolled_work(), (0, 0, 0, 0));
}
