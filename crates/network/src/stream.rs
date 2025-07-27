//! Stream abstractions for the network layer

use crate::error::{NetworkResult, StreamError};
use bytes::Bytes;
use flume::{Receiver, Sender};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

/// Configuration for a stream
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Buffer size for the stream channels
    pub buffer_size: usize,
    /// Initial flow control window size
    pub initial_window: u32,
    /// Maximum window size
    pub max_window: u32,
    /// Timeout for stream operations
    pub timeout: Duration,
    /// Whether to enable compression
    pub compression: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            initial_window: 65536, // 64KB
            max_window: 1048576,   // 1MB
            timeout: Duration::from_secs(30),
            compression: false,
        }
    }
}

/// Frame types for stream multiplexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamFrame {
    /// Open a new stream
    Open {
        stream_id: Uuid,
        stream_type: String,
        metadata: std::collections::HashMap<String, String>,
    },
    /// Data frame
    Data {
        stream_id: Uuid,
        sequence: u64,
        payload: Bytes,
        /// End of stream marker
        fin: bool,
    },
    /// Flow control window update
    WindowUpdate { stream_id: Uuid, increment: u32 },
    /// Reset/close stream
    Reset {
        stream_id: Uuid,
        error_code: u32,
        reason: String,
    },
}

/// Flow control for a stream
#[derive(Debug)]
pub struct FlowController {
    /// Send window (how much we can send)
    send_window: AtomicU32,
    /// Receive window (how much we can receive)
    recv_window: AtomicU32,
    /// Notify when send window increases
    send_notify: Notify,
    /// Bytes in flight (unacknowledged)
    in_flight: AtomicU64,
}

impl FlowController {
    pub fn new(initial_window: u32) -> Self {
        Self {
            send_window: AtomicU32::new(initial_window),
            recv_window: AtomicU32::new(initial_window),
            send_notify: Notify::new(),
            in_flight: AtomicU64::new(0),
        }
    }

    /// Wait for send window capacity
    pub async fn wait_for_capacity(&self, size: usize) -> Result<(), StreamError> {
        loop {
            let window = self.send_window.load(Ordering::Acquire);
            if window >= size as u32 {
                // Try to reserve the space
                match self.send_window.compare_exchange(
                    window,
                    window - size as u32,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.in_flight.fetch_add(size as u64, Ordering::AcqRel);
                        return Ok(());
                    }
                    Err(_) => continue, // Retry
                }
            }

            // Wait for window update
            self.send_notify.notified().await;
        }
    }

    /// Try to acquire send capacity without waiting
    pub fn try_acquire_capacity(&self, size: usize) -> Result<(), StreamError> {
        let window = self.send_window.load(Ordering::Acquire);
        if window < size as u32 {
            return Err(StreamError::WouldBlock);
        }

        match self.send_window.compare_exchange(
            window,
            window - size as u32,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.in_flight.fetch_add(size as u64, Ordering::AcqRel);
                Ok(())
            }
            Err(_) => Err(StreamError::WouldBlock),
        }
    }

    /// Update send window (from peer's window update)
    pub fn update_send_window(&self, increment: u32) {
        self.send_window.fetch_add(increment, Ordering::AcqRel);
        self.send_notify.notify_waiters();
    }

    /// Consume receive window
    pub fn consume_recv_window(&self, size: u32) -> Result<(), StreamError> {
        let window = self.recv_window.load(Ordering::Acquire);
        if window < size {
            return Err(StreamError::FlowControl("Receive window exceeded".into()));
        }
        self.recv_window.fetch_sub(size, Ordering::AcqRel);
        Ok(())
    }

    /// Get available receive window for sending updates
    pub fn available_recv_window(&self) -> u32 {
        self.recv_window.load(Ordering::Acquire)
    }
}

/// A handle to an active stream
pub struct StreamHandle {
    /// Stream ID
    pub id: Uuid,
    /// Peer node
    pub peer: NodeId,
    /// Stream type/service
    pub stream_type: String,
    /// Send side of the stream
    pub sender: StreamSender,
    /// Receive side of the stream
    pub receiver: StreamReceiver,
    /// Flow controller
    pub flow_control: Arc<FlowController>,
}

/// Send side of a stream
pub struct StreamSender {
    pub(crate) inner: Sender<Bytes>,
    flow_control: Arc<FlowController>,
    sequence: Arc<AtomicU64>,
    // Optional frame sender for sending data as stream frames
    frame_sender: Option<(Uuid, flume::Sender<crate::message::MultiplexedFrame>)>,
}

impl StreamSender {
    pub fn new(sender: Sender<Bytes>, flow_control: Arc<FlowController>) -> Self {
        Self {
            inner: sender,
            flow_control,
            sequence: Arc::new(AtomicU64::new(0)),
            frame_sender: None,
        }
    }

    pub fn new_with_frame_sender(
        sender: Sender<Bytes>,
        flow_control: Arc<FlowController>,
        stream_id: Uuid,
        frame_sender: flume::Sender<crate::message::MultiplexedFrame>,
    ) -> Self {
        Self {
            inner: sender,
            flow_control,
            sequence: Arc::new(AtomicU64::new(0)),
            frame_sender: Some((stream_id, frame_sender)),
        }
    }

    /// Send data with flow control
    pub async fn send(&self, data: Bytes) -> Result<(), StreamError> {
        // Wait for flow control window
        self.flow_control.wait_for_capacity(data.len()).await?;

        // If we have a frame sender, send via frames
        if let Some((stream_id, ref frame_sender)) = self.frame_sender {
            let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);
            let frame = crate::message::MultiplexedFrame {
                stream_id: Some(stream_id),
                data: crate::message::FrameData::Stream(StreamFrame::Data {
                    stream_id,
                    sequence,
                    payload: data,
                    fin: false,
                }),
            };

            frame_sender
                .send_async(frame)
                .await
                .map_err(|_| StreamError::Closed { id: stream_id })?;
        } else {
            // Only send to local receiver if we don't have a frame sender
            self.inner
                .send_async(data)
                .await
                .map_err(|_| StreamError::Closed { id: Uuid::nil() })?;
        }

        Ok(())
    }

    /// Send data with timeout
    pub async fn send_timeout(&self, data: Bytes, timeout: Duration) -> Result<(), StreamError> {
        tokio::time::timeout(timeout, self.send(data))
            .await
            .map_err(|_| StreamError::Timeout)?
    }

    /// Try to send without blocking
    pub fn try_send(&self, data: Bytes) -> Result<(), StreamError> {
        // Check flow control
        self.flow_control.try_acquire_capacity(data.len())?;

        // Try to send
        self.inner
            .try_send(data)
            .map_err(|_| StreamError::WouldBlock)?;

        Ok(())
    }

    /// Close the stream
    pub fn close(self) {
        // Dropping the sender closes the stream
        drop(self);
    }
}

/// Receive side of a stream
pub struct StreamReceiver {
    inner: Receiver<Bytes>,
    flow_control: Arc<FlowController>,
}

impl StreamReceiver {
    pub fn new(receiver: Receiver<Bytes>, flow_control: Arc<FlowController>) -> Self {
        Self {
            inner: receiver,
            flow_control,
        }
    }

    /// Receive data from the stream
    pub async fn recv(&self) -> Option<Bytes> {
        match self.inner.recv_async().await {
            Ok(data) => {
                // Update flow control
                let _ = self.flow_control.consume_recv_window(data.len() as u32);
                Some(data)
            }
            Err(_) => None,
        }
    }

    /// Receive data with timeout
    pub async fn recv_timeout(&self, timeout: Duration) -> Option<Bytes> {
        tokio::time::timeout(timeout, self.recv())
            .await
            .unwrap_or_default()
    }

    /// Try to receive without blocking
    pub fn try_recv(&self) -> Option<Bytes> {
        match self.inner.try_recv() {
            Ok(data) => {
                // Update flow control
                let _ = self.flow_control.consume_recv_window(data.len() as u32);
                Some(data)
            }
            Err(_) => None,
        }
    }
}

/// Bidirectional stream wrapper
pub struct Stream {
    pub handle: StreamHandle,
}

impl Stream {
    /// Send data on the stream
    pub async fn send(&self, data: impl Into<Bytes>) -> NetworkResult<()> {
        self.handle.sender.send(data.into()).await?;
        Ok(())
    }

    /// Send data with timeout
    pub async fn send_timeout(
        &self,
        data: impl Into<Bytes>,
        timeout: Duration,
    ) -> NetworkResult<()> {
        self.handle
            .sender
            .send_timeout(data.into(), timeout)
            .await?;
        Ok(())
    }

    /// Receive data from the stream
    pub async fn recv(&self) -> Option<Bytes> {
        self.handle.receiver.recv().await
    }

    /// Receive data with timeout
    pub async fn recv_timeout(&self, timeout: Duration) -> Option<Bytes> {
        self.handle.receiver.recv_timeout(timeout).await
    }

    /// Get the stream ID
    pub fn id(&self) -> Uuid {
        self.handle.id
    }

    /// Get the peer node ID
    pub fn peer(&self) -> &NodeId {
        &self.handle.peer
    }
}
