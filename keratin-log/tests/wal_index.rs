use keratin_log::*;

#[tokio::test]
async fn wal_index_fetch_correctness() {
    let dir = util::test_dir("wal_index");
    let cfg = KeratinConfig::test_default();
    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    let mut to_send = vec![];
    for i in 0..100_000 {
        to_send.push(Message {
            payload: (i.to_string().into_bytes()),
            flags: 0,
            headers: vec![],
        });
    }
    k.append_batch(to_send, None).await.unwrap();

    let reader = k.reader();

    for _ in 0..500 {
        let off = fastrand::u64(0..100_000);
        let m = reader.fetch(off).unwrap().unwrap();
        assert_eq!(m.offset, off as u64);
        assert_eq!(m.payload, off.to_string().into_bytes());
    }
}
