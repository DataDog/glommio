// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020
// Datadog, Inc.
use glommio::channels::channel_mesh::{Full, MeshBuilder};

#[test]
fn racy_test() {
    let a: MeshBuilder<bool, Full> = MeshBuilder::full(41, 15);
    let b = a.clone();
    std::thread::scope(|s| {
        s.spawn(move || {
            let _ = b.clone();
        });
        let _ = a.clone();
    });
}
