// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Chained reader for concatenating multiple async readers.
//!
//! Used by FilePhysicalSeries to concatenate all versions in oldest-to-newest order.
//! This is a general-purpose utility that chains multiple AsyncRead sources together.

use std::io::{self, Cursor, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use crate::file::AsyncReadSeek;

enum SeekState {
    Idle,
    Pending { position: u64, reader_index: usize },
    Failed(String),
}

/// A reader that chains multiple AsyncRead sources together.
///
/// Reads from each source in order until EOF, then moves to the next source.
/// Used to concatenate all versions of a FilePhysicalSeries entry.
pub struct ChainedReader {
    /// The readers to chain together (in order)
    readers: Vec<Pin<Box<dyn AsyncReadSeek>>>,
    /// Logical size of each reader
    sizes: Vec<u64>,
    /// Current reader index
    current_index: usize,
    /// Total bytes read so far (for seek support)
    position: u64,
    /// Total size of all readers combined
    total_size: u64,
    /// State of the current or most recent seek
    seek_state: SeekState,
}

impl ChainedReader {
    /// Create a new chained reader from a list of readers and their sizes.
    ///
    /// The readers should be in the order they should be read (e.g., oldest first).
    /// The sizes are needed for seek support.
    #[must_use]
    pub fn new(readers: Vec<Pin<Box<dyn AsyncReadSeek>>>, sizes: Vec<u64>) -> Self {
        assert_eq!(
            readers.len(),
            sizes.len(),
            "readers and sizes must have same length"
        );

        let total_size: u64 = sizes.iter().sum();

        Self {
            readers,
            sizes,
            current_index: 0,
            position: 0,
            total_size,
            seek_state: SeekState::Idle,
        }
    }

    /// Create a chained reader from in-memory byte vectors.
    ///
    /// This is useful for tests and for small file versions stored inline.
    #[must_use]
    pub fn from_bytes(chunks: Vec<Vec<u8>>) -> Self {
        let sizes: Vec<u64> = chunks.iter().map(|c| c.len() as u64).collect();
        let readers: Vec<Pin<Box<dyn AsyncReadSeek>>> = chunks
            .into_iter()
            .map(|c| Box::pin(Cursor::new(c)) as Pin<Box<dyn AsyncReadSeek>>)
            .collect();
        Self::new(readers, sizes)
    }

    fn checked_seek_position(&self, position: SeekFrom) -> io::Result<u64> {
        let new_position = match position {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::End(offset) => self.total_size.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek before start"))?;

        if new_position > self.total_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot seek past end",
            ));
        }
        Ok(new_position)
    }

    fn reader_position(&self, position: u64) -> (usize, u64) {
        let mut start = 0;
        for (index, size) in self.sizes.iter().copied().enumerate() {
            let end = start + size;
            if position < end {
                return (index, position - start);
            }
            start = end;
        }
        (self.readers.len(), 0)
    }

    fn failed_seek_error(message: &str) -> io::Error {
        io::Error::other(format!(
            "Chained reader is unusable after a child seek failed: {message}"
        ))
    }
}

impl AsyncRead for ChainedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &self.seek_state {
            SeekState::Idle => {}
            SeekState::Pending { .. } => {
                return Poll::Ready(Err(io::Error::other(
                    "Read attempted before chained seek completed",
                )));
            }
            SeekState::Failed(message) => {
                return Poll::Ready(Err(Self::failed_seek_error(message)));
            }
        }

        loop {
            // Check if we've exhausted all readers
            if self.current_index >= self.readers.len() {
                return Poll::Ready(Ok(())); // EOF
            }

            // Get the current reader
            let current_idx = self.current_index;
            let reader = &mut self.readers[current_idx];
            let before_len = buf.filled().len();

            match Pin::new(reader).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let bytes_read = buf.filled().len() - before_len;
                    if bytes_read > 0 {
                        self.position += bytes_read as u64;
                        return Poll::Ready(Ok(()));
                    } else {
                        // EOF on current reader, move to next
                        self.current_index += 1;
                        // Continue loop to try next reader
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncSeek for ChainedReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match &self.seek_state {
            SeekState::Idle => {}
            SeekState::Pending { .. } => {
                return Err(io::Error::other("Chained seek already in progress"));
            }
            SeekState::Failed(message) => return Err(Self::failed_seek_error(message)),
        }

        let new_position = self.checked_seek_position(position)?;
        let (target_index, target_offset) = self.reader_position(new_position);

        for index in 0..self.readers.len() {
            let child_position = if index < target_index {
                self.sizes[index]
            } else if index == target_index {
                target_offset
            } else {
                0
            };
            if let Err(error) = self.readers[index]
                .as_mut()
                .start_seek(SeekFrom::Start(child_position))
            {
                self.seek_state = SeekState::Failed(error.to_string());
                return Err(error);
            }
        }

        self.seek_state = SeekState::Pending {
            position: new_position,
            reader_index: target_index,
        };
        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let (new_position, target_index) = match &self.seek_state {
            SeekState::Idle => return Poll::Ready(Ok(self.position)),
            SeekState::Pending {
                position,
                reader_index,
            } => (*position, *reader_index),
            SeekState::Failed(message) => {
                return Poll::Ready(Err(Self::failed_seek_error(message)));
            }
        };

        for reader in &mut self.readers {
            match reader.as_mut().poll_complete(cx) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => {
                    self.seek_state = SeekState::Failed(error.to_string());
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        self.position = new_position;
        self.current_index = target_index;
        self.seek_state = SeekState::Idle;
        Poll::Ready(Ok(new_position))
    }
}

// Implement Unpin to satisfy the blanket impl for AsyncReadSeek
impl Unpin for ChainedReader {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    struct PendingSeekReader {
        cursor: Cursor<Vec<u8>>,
        target: Option<u64>,
        returned_pending: bool,
    }

    struct FailingSeekReader {
        fail_on_start: bool,
    }

    impl AsyncRead for FailingSeekReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncSeek for FailingSeekReader {
        fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> io::Result<()> {
            if self.fail_on_start {
                Err(io::Error::other("start seek failure"))
            } else {
                Ok(())
            }
        }

        fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            Poll::Ready(Err(io::Error::other("complete seek failure")))
        }
    }

    impl PendingSeekReader {
        fn new(content: &[u8]) -> Self {
            Self {
                cursor: Cursor::new(content.to_vec()),
                target: None,
                returned_pending: false,
            }
        }
    }

    impl AsyncRead for PendingSeekReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.cursor).poll_read(cx, buf)
        }
    }

    impl AsyncSeek for PendingSeekReader {
        fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
            let SeekFrom::Start(target) = position else {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "test reader accepts absolute seeks only",
                ));
            };
            self.target = Some(target);
            self.returned_pending = false;
            Ok(())
        }

        fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            let Some(target) = self.target else {
                return Poll::Ready(Ok(self.cursor.position()));
            };
            if !self.returned_pending {
                self.returned_pending = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.cursor.set_position(target);
            self.target = None;
            Poll::Ready(Ok(target))
        }
    }

    #[tokio::test]
    async fn test_chained_reader_basic() {
        let chunks = vec![b"Hello, ".to_vec(), b"World".to_vec(), b"!".to_vec()];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "Hello, World!");
    }

    #[tokio::test]
    async fn test_chained_reader_empty_chunks() {
        let chunks = vec![
            b"A".to_vec(),
            b"".to_vec(), // Empty chunk
            b"B".to_vec(),
            b"".to_vec(), // Another empty chunk
            b"C".to_vec(),
        ];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "ABC");
    }

    #[tokio::test]
    async fn test_chained_reader_single_chunk() {
        let chunks = vec![b"Single chunk content".to_vec()];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "Single chunk content");
    }

    #[tokio::test]
    async fn test_chained_reader_no_chunks() {
        let chunks: Vec<Vec<u8>> = vec![];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = Vec::new();
        let _ = reader.read_to_end(&mut result).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_chained_reader_csv_lines() {
        // Simulate CSV data split across multiple versions
        let chunks = vec![
            b"name,value\n".to_vec(),
            b"alice,100\n".to_vec(),
            b"bob,200\n".to_vec(),
            b"carol,300\n".to_vec(),
        ];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "name,value\nalice,100\nbob,200\ncarol,300\n");
    }

    #[tokio::test]
    async fn test_rewind_after_eof_reads_complete_stream_again() {
        let mut reader =
            ChainedReader::from_bytes(vec![b"abc".to_vec(), b"def".to_vec(), b"ghi".to_vec()]);

        let mut first = String::new();
        _ = reader.read_to_string(&mut first).await.unwrap();
        assert_eq!(first, "abcdefghi");

        assert_eq!(reader.seek(SeekFrom::Start(0)).await.unwrap(), 0);
        let mut second = String::new();
        _ = reader.read_to_string(&mut second).await.unwrap();
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn test_partial_read_then_rewind() {
        let mut reader =
            ChainedReader::from_bytes(vec![b"abc".to_vec(), b"def".to_vec(), b"ghi".to_vec()]);
        let mut prefix = [0; 5];
        _ = reader.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"abcde");

        assert_eq!(reader.seek(SeekFrom::Start(0)).await.unwrap(), 0);
        let mut all = String::new();
        _ = reader.read_to_string(&mut all).await.unwrap();
        assert_eq!(all, "abcdefghi");
    }

    #[tokio::test]
    async fn test_seek_within_and_across_chunk_boundaries() {
        let mut reader = ChainedReader::from_bytes(vec![
            b"abc".to_vec(),
            Vec::new(),
            b"defg".to_vec(),
            b"hi".to_vec(),
        ]);

        assert_eq!(reader.seek(SeekFrom::Start(2)).await.unwrap(), 2);
        let mut across = [0; 4];
        _ = reader.read_exact(&mut across).await.unwrap();
        assert_eq!(&across, b"cdef");

        assert_eq!(reader.seek(SeekFrom::Start(3)).await.unwrap(), 3);
        let mut boundary = [0; 4];
        _ = reader.read_exact(&mut boundary).await.unwrap();
        assert_eq!(&boundary, b"defg");

        assert_eq!(reader.seek(SeekFrom::Start(7)).await.unwrap(), 7);
        let mut final_chunk = String::new();
        _ = reader.read_to_string(&mut final_chunk).await.unwrap();
        assert_eq!(final_chunk, "hi");
    }

    #[tokio::test]
    async fn test_current_end_and_eof_seeks() {
        let mut reader =
            ChainedReader::from_bytes(vec![b"abc".to_vec(), b"defg".to_vec(), b"hi".to_vec()]);
        let mut prefix = [0; 5];
        _ = reader.read_exact(&mut prefix).await.unwrap();

        assert_eq!(reader.seek(SeekFrom::Current(-3)).await.unwrap(), 2);
        let mut middle = [0; 4];
        _ = reader.read_exact(&mut middle).await.unwrap();
        assert_eq!(&middle, b"cdef");

        assert_eq!(reader.seek(SeekFrom::End(-2)).await.unwrap(), 7);
        let mut tail = String::new();
        _ = reader.read_to_string(&mut tail).await.unwrap();
        assert_eq!(tail, "hi");

        assert_eq!(reader.seek(SeekFrom::End(0)).await.unwrap(), 9);
        let mut eof = [0; 1];
        assert_eq!(reader.read(&mut eof).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_repeated_backward_and_forward_seeks() {
        let mut reader =
            ChainedReader::from_bytes(vec![b"abc".to_vec(), b"def".to_vec(), b"ghi".to_vec()]);

        for (position, expected) in [(6, b"ghi".as_slice()), (1, b"bcdefghi"), (4, b"efghi")] {
            assert_eq!(
                reader.seek(SeekFrom::Start(position)).await.unwrap(),
                position
            );
            let mut result = Vec::new();
            _ = reader.read_to_end(&mut result).await.unwrap();
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_invalid_seeks_do_not_change_position() {
        let mut reader = ChainedReader::from_bytes(vec![b"abc".to_vec(), b"def".to_vec()]);
        let mut prefix = [0; 2];
        _ = reader.read_exact(&mut prefix).await.unwrap();

        assert!(reader.seek(SeekFrom::Start(7)).await.is_err());
        assert!(reader.seek(SeekFrom::Current(-3)).await.is_err());
        assert!(reader.seek(SeekFrom::End(1)).await.is_err());
        assert_eq!(reader.stream_position().await.unwrap(), 2);

        let mut remainder = String::new();
        _ = reader.read_to_string(&mut remainder).await.unwrap();
        assert_eq!(remainder, "cdef");
    }

    #[tokio::test]
    async fn test_seek_waits_for_pending_child_seeks() {
        let readers: Vec<Pin<Box<dyn AsyncReadSeek>>> = vec![
            Box::pin(PendingSeekReader::new(b"abc")),
            Box::pin(PendingSeekReader::new(b"def")),
        ];
        let mut reader = ChainedReader::new(readers, vec![3, 3]);

        assert_eq!(reader.seek(SeekFrom::Start(4)).await.unwrap(), 4);
        let mut remainder = String::new();
        _ = reader.read_to_string(&mut remainder).await.unwrap();
        assert_eq!(remainder, "ef");
    }

    #[tokio::test]
    async fn test_start_seek_failure_makes_reader_unusable() {
        let readers: Vec<Pin<Box<dyn AsyncReadSeek>>> = vec![
            Box::pin(PendingSeekReader::new(b"abc")),
            Box::pin(FailingSeekReader {
                fail_on_start: true,
            }),
        ];
        let mut reader = ChainedReader::new(readers, vec![3, 0]);

        let error = reader.seek(SeekFrom::Start(1)).await.unwrap_err();
        assert_eq!(error.to_string(), "start seek failure");

        let mut byte = [0];
        let error = reader.read(&mut byte).await.unwrap_err();
        assert!(error.to_string().contains("unusable"));
        let error = reader.seek(SeekFrom::Start(0)).await.unwrap_err();
        assert!(error.to_string().contains("unusable"));
    }

    #[tokio::test]
    async fn test_poll_complete_failure_makes_reader_unusable() {
        let readers: Vec<Pin<Box<dyn AsyncReadSeek>>> = vec![Box::pin(FailingSeekReader {
            fail_on_start: false,
        })];
        let mut reader = ChainedReader::new(readers, vec![1]);

        let error = reader.seek(SeekFrom::Start(0)).await.unwrap_err();
        assert_eq!(error.to_string(), "complete seek failure");

        let mut byte = [0];
        let error = reader.read(&mut byte).await.unwrap_err();
        assert!(error.to_string().contains("unusable"));
        let error = reader.seek(SeekFrom::Start(0)).await.unwrap_err();
        assert!(error.to_string().contains("unusable"));
    }
}
