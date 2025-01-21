#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::future_not_send)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use deno_core::{op2, OpState};
use futures::{Stream, StreamExt};
use proven_sql::SqlParam;

static BATCH_SIZE: usize = 100;

pub struct SqlQueryResultsManager {
    next_row_stream_id: AtomicU32,
    row_streams: HashMap<u32, Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>>,
}

impl SqlQueryResultsManager {
    pub fn new() -> Self {
        Self {
            next_row_stream_id: AtomicU32::new(0),
            row_streams: HashMap::new(),
        }
    }

    pub fn save_stream(
        &mut self,
        row_stream: Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>,
    ) -> u32 {
        let row_stream_id = self.next_row_stream_id.fetch_add(1, Ordering::Relaxed);

        self.row_streams.insert(row_stream_id, row_stream);

        row_stream_id
    }

    pub async fn get_batch_from_stream(&mut self, row_stream_id: u32) -> Vec<Vec<SqlParam>> {
        match self.row_streams.get_mut(&row_stream_id) {
            Some(stream) => stream.take(BATCH_SIZE).collect().await,
            None => Vec::new(),
        }
    }
}

#[op2(async)]
#[serde]
pub async fn op_get_row_batch(
    state: Rc<RefCell<OpState>>,
    row_stream_id: u32,
) -> Vec<Vec<SqlParam>> {
    let mut query_results_manager = {
        loop {
            let query_results_manager = {
                let mut borrowed_state = state.borrow_mut();

                borrowed_state.try_take::<SqlQueryResultsManager>()
            };

            match query_results_manager {
                Some(store) => break store,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    let batch = query_results_manager
        .get_batch_from_stream(row_stream_id)
        .await;

    state.borrow_mut().put(query_results_manager);

    batch
}
