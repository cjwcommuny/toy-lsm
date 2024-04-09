use criterion::{black_box, criterion_group, criterion_main, Criterion};
use better_mini_lsm::fibonacci;

pub fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("ycsb", |b| {
        b.to_async(&runtime)
            .iter(|| async { fibonacci(black_box(20)).await })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);