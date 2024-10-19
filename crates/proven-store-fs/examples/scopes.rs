use proven_store::{Store, Store1, Store2};
use proven_store_fs::FsStore;

fn main() {
    let global_store = FsStore::new("/tmp/store/global".into());
    let application_store = FsStore::new("/tmp/store/applications".into());
    let nft_store = FsStore::new("/tmp/store/nfts".into());

    let app = Application::new(global_store, application_store, nft_store);

    app.put_global("test".to_string(), vec![1, 2, 3]);
    app.put_application("app1".to_string(), "test".to_string(), vec![4, 5, 6]);
    app.put_nft(
        "app1".to_string(),
        "nft1".to_string(),
        "test".to_string(),
        vec![7, 8, 9],
    );
}

struct Application<GS: Store, AS: Store1, NS: Store2> {
    global_store: GS,
    application_store: AS,
    nft_store: NS,
}

impl<GS: Store, AS: Store1, NS: Store2> Application<GS, AS, NS> {
    fn new(global_store: GS, application_store: AS, nft_store: NS) -> Self {
        Application {
            global_store,
            application_store,
            nft_store,
        }
    }

    fn put_global(&self, key: String, value: Vec<u8>) {
        self.global_store.put_blocking(key, value).unwrap();
    }

    fn put_application(&self, app_id: String, key: String, value: Vec<u8>) {
        self.application_store
            .scope(app_id)
            .put_blocking(key, value)
            .unwrap();
    }

    fn put_nft(&self, app_id: String, nft_id: String, key: String, value: Vec<u8>) {
        self.nft_store
            .scope(app_id)
            .scope(nft_id)
            .put_blocking(key, value)
            .unwrap();
    }
}
